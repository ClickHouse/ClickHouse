-- Tags: no-fasttest
-- Tag no-fasttest: Depends on OpenSSL

SELECT encrypt('aes-128-ecb', [1, -1, 0, NULL], 'text'); -- { serverError 43 }
