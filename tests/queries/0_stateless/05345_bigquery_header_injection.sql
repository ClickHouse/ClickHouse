-- Values that become HTTP header values must not contain CR or LF.
-- The base_url points to a closed port, so an unchecked value fails with a network error instead.

SELECT * FROM bigquery('project', 'dataset', 'table', 'token', billing_project = 'p\r\nX-Injected: 1', base_url = 'http://127.0.0.1:1'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM bigquery('project', 'dataset', 'table', 'token\nX-Injected: 1', base_url = 'http://127.0.0.1:1'); -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS t_05345;
CREATE TABLE t_05345 (x Int64) ENGINE = BigQuery('project', 'dataset', 'table', 'token', billing_project = 'p\r\nX-Injected: 1', base_url = 'http://127.0.0.1:1'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_05345 (x Int64) ENGINE = BigQuery('project', 'dataset', 'table', 'token\rX-Injected: 1', base_url = 'http://127.0.0.1:1'); -- { serverError BAD_ARGUMENTS }
DROP TABLE IF EXISTS t_05345;

SELECT 'OK';
