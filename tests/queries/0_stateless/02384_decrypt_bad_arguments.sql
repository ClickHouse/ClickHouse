-- Tags: no-fasttest
SELECT decrypt('aes-128-gcm', [1024, 65535, NULL, NULL, 9223372036854775807, 1048576, NULL], 'text', 'key', 'IV'); -- { serverError 43 }
