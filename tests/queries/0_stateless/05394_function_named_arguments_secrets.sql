-- Tags: no-fasttest
-- Tag no-fasttest: `encrypt` needs OpenSSL, `s3` needs AWS

-- Secrets are masked when arguments are named, also in the exception message.
SELECT encrypt('aes-128-ecb', plaintext => 'text', key => 'named_secret_key_1'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3('http://localhost:11111/test/a.csv', access_key_id => 'x', secret_access_key => 'named_secret_key_2'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3('http://localhost:11111/test/a.csv', secret_access_key => 'named_secret_key_3', access_key_id => 'x'); -- { serverError BAD_ARGUMENTS }
SYSTEM FLUSH LOGS query_log;
SELECT count() FROM system.query_log WHERE current_database = currentDatabase() AND event_date >= yesterday() AND (query LIKE '%named\_secret\_key\_%' OR exception LIKE '%named\_secret\_key\_%') AND query NOT LIKE '%query_log%';
