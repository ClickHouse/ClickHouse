SELECT * FROM remote('127..2', 'a.'); -- { serverError SYNTAX_ERROR }

-- Clear cache to avoid future errors in the logs
SYSTEM CLEAR DNS CACHE
