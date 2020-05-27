DROP TABLE IF EXISTS test_uatraits;
CREATE TABLE test_uatraits ( d Date, ua string) engine=MergeTree(d, (ua), 8192);
INSERT INTO test_uatraits format Values (now(), 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:76.0) Gecko/20100101 Firefox/76.0');
INSERT INTO test_uatraits format Values (now(), 'Mozilla/5.0 (Linux; Android 9; Pixel 2 XL Build/PPP3.180510.008) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Mobile Safari/537.3');
select extractOSFromUserAgent(ua), extractBrowserFromUserAgent(ua) from test_uatraits;
select extractOSFromUserAgent(ua) from test_uatraits; 
select extractBrowserFromUserAgent(ua)from test_uatraits;
DROP TABLE test_uatraits;
