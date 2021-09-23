SELECT * FROM remote('127..2', 'a.'); -- { serverError 62 }

-- Clear cache to avoid future errors in the logs
SYSTEM DROP DNS CACHE
