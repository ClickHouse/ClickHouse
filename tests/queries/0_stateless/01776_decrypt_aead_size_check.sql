-- Tags: no-fasttest
-- Tag no-fasttest: Depends on OpenSSL

SELECT decrypt('aes-128-gcm', 'text', 'key', 'IV'); -- { serverError BAD_ARGUMENTS }
