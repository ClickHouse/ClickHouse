SELECT * FROM remote('127..2', 'a.'); -- { serverError 36 }

-- Clear cache to avoid future errors in the logs
SYSTEM DROP DNS CACHE
