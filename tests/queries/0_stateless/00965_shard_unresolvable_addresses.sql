SELECT count() FROM remote('127.0.0.1,localhos', system.one); -- { serverError 279 }
SELECT count() FROM remote('127.0.0.1|localhos', system.one);
