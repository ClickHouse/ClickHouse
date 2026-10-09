-- The OAuth token is sent in the Authorization header, so it must not contain CR or LF.
-- The proxy URL points to a closed port, so an unchecked token fails with a network error instead.

SET allow_experimental_ytsaurus_table_function = 1, allow_experimental_ytsaurus_table_engine = 1;

SELECT * FROM ytsaurus('http://127.0.0.1:1', '//tmp/t', 'token\r\nX-Injected: 1', 'x Int64'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM ytsaurus('http://127.0.0.1:1', '//tmp/t', 'token\nX-Injected: 1', 'x Int64'); -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS t_05346;
CREATE TABLE t_05346 (x Int64) ENGINE = YTsaurus('http://127.0.0.1:1', '//tmp/t', 'token\r\nX-Injected: 1'); -- { serverError BAD_ARGUMENTS }
DROP TABLE IF EXISTS t_05346;

SELECT 'OK';
