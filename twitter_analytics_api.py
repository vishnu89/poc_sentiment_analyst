from keys import oauth_keys
import twitter

def convert_file(orgi_fn):
    def new_fn(*args, **kwargs):
        orgi_result = orgi_fn(*args, **kwargs)
        new_result = args[0].convert_fn(orgi_result)  # args[0] -> self
        args[0].result = new_result
        return new_result
    return new_fn


class TwitterAnalytics(object):
    def __init__(self, oauth_keys, output_format):
        self.result, self.filename = None, None
        self.convert_fn = getattr(self, output_format)
        try:
            consumer_key, consumer_secret, access_token, access_token_secret = oauth_keys
            auth = twitter.oauth.OAuth(access_token, access_token_secret, consumer_key, consumer_secret)
            self.twitter_api = twitter.Twitter(auth=auth)
        except Exception as exp:
            raise Exception('Failed to authenticate please check your keys: %s' % exp)
        
    def to_data_frame(self, result):
        return pd.read_json(self.to_json(result))

    def to_json(self, result):
        return unicode(json.dumps(result, ensure_ascii=False, indent=1))

    def save(self,  result=None, filename=None, filetype='to_json'):
        result = result if result else self.result
        filename = filename if filename else os.path.join('default', self.filename)
        filename = '%s%s' % (filename,file_ext(filetype))
        if isinstance(result, (pd.DataFrame, pd.Series)):
            book = getattr(result, filetype)
            book(filename)
        elif isinstance(result, unicode) and filetype == 'to_json': # if result is in json format write it directly
            with io.open(filename, 'a+', encoding='utf-8') as book:
                book.write(result)
        elif isinstance(result, unicode) and filetype != 'to_json': # convert result to dataFrame and write
            book = getattr(pd.read_json(result), filetype)
            book(filename)
        else:
            raise KeyError('%s is a unknown file system' % type(result))

    @convert_file
    def trends(self, woe_id, output_format = None):
        self.convert_fn = getattr(self, output_format) if output_format else self.convert_fn
        self.filename = '%s_%s_%s' % (timer(), 'trends', woe_id)
        return self.twitter_api.trends.place(_id=woe_id)

    @convert_file
    def search(self, q, max_results=200, output_format = None, **kw):
        # Max queries = 180 queries/15min
        self.convert_fn = getattr(self, output_format) if output_format else self.convert_fn
        self.filename = '%s_%s_%s' % (timer(), 'search', q)
        max_results = min(1000, max_results)
        search_results = self.twitter_api.search.tweets(q=q, count=min(100,max_results), **kw)
        statuses = search_results['statuses']
        for _ in range(10):  # 10*100 = 1000
            next_results = search_results['search_metadata'].get('next_results')
            if not next_results or len(statuses) >= max_results:
                break  # No more results when next_results doesn't exist
            # Create a dictionary from next_results, which has the following form:
            # ?max_id=313519052523986943&q=NCAA&include_entities=1
            kwargs = dict([kv.split('=') for kv in unquote(next_results[1:]).split("&")])
            search_results = self.twitter_api.search.tweets(**kwargs)  # recursive calling the search function 
            statuses += search_results['statuses']
        return statuses
tweet = TwitterAnalytics(oauth_keys(), 'to_json')