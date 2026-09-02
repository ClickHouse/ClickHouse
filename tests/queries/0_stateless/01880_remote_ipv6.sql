SET connections_with_failover_max_tries=0;

SELECT * FROM remote('[::1]', system.one) FORMAT Null;
SELECT * FROM remote('[::1]:9000', system.one) FORMAT Null;

SELECT * FROM remote('[::1', system.one) FORMAT Null; -- { serverError BAD_ARGUMENTS }
SELECT * FROM remote('::1]', system.one) FORMAT Null; -- { serverError BAD_ARGUMENTS }
SELECT * FROM remote('::1', system.one) FORMAT Null; -- { serverError BAD_ARGUMENTS }

SELECT * FROM remote('[::1][::1]', system.one) FORMAT Null; -- { serverError BAD_ARGUMENTS }
SELECT * FROM remote('[::1][::1', system.one) FORMAT Null; -- { serverError BAD_ARGUMENTS }
SELECT * FROM remote('[::1]::1]', system.one) FORMAT Null; -- { serverError BAD_ARGUMENTS }

SELECT * FROM remote('[::1]') FORMAT Null;
SELECT * FROM remote('[::1]:9000') FORMAT Null;

SELECT * FROM remote('[::1') FORMAT Null; -- { serverError BAD_ARGUMENTS }
SELECT * FROM remote('::1]') FORMAT Null; -- { serverError BAD_ARGUMENTS }
SELECT * FROM remote('::1') FORMAT Null; -- { serverError BAD_ARGUMENTS }

SELECT * FROM remote('[::1][::1]') FORMAT Null; -- { serverError BAD_ARGUMENTS }
SELECT * FROM remote('[::1][::1') FORMAT Null; -- { serverError BAD_ARGUMENTS }
SELECT * FROM remote('[::1]::1]') FORMAT Null; -- { serverError BAD_ARGUMENTS }
