-- Argument validation of the bigquery table function. All errors are thrown while
-- parsing arguments, so no network access happens.

SELECT * FROM bigquery(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM bigquery('project'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM bigquery('project', 'dataset'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM bigquery('project', 'dataset', 'table', 'token', 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- No credentials.
SELECT * FROM bigquery('project', 'dataset', 'table'); -- { serverError BAD_ARGUMENTS }
-- Multiple credential methods at once.
SELECT * FROM bigquery('project', 'dataset', 'table', 'token', service_account_key = '{}'); -- { serverError BAD_ARGUMENTS }
-- An incomplete refresh token triple.
SELECT * FROM bigquery('project', 'dataset', 'table', client_id = 'id'); -- { serverError BAD_ARGUMENTS }
-- Unknown argument.
SELECT * FROM bigquery('project', 'dataset', 'table', 'token', unknown_argument = 'x'); -- { serverError BAD_ARGUMENTS }
-- Arguments must be strings.
SELECT * FROM bigquery('project', 'dataset', 'table', 'token', base_url = 42); -- { serverError BAD_ARGUMENTS }
-- Invalid characters in identifiers.
SELECT * FROM bigquery('project', 'dataset', 'bad/table', 'token'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM bigquery('', 'dataset', 'table', 'token'); -- { serverError BAD_ARGUMENTS }
-- base_url must be an http(s) URL without a path.
SELECT * FROM bigquery('project', 'dataset', 'table', 'token', base_url = 'ftp://example.com'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM bigquery('project', 'dataset', 'table', 'token', base_url = 'https://example.com/path'); -- { serverError BAD_ARGUMENTS }

-- The same validation applies to the table engine.
CREATE TABLE t_04512 (x Int64) ENGINE = BigQuery('project', 'dataset', 'table'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_04512 (x Int64) ENGINE = BigQuery('project', 'dataset'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT 'OK';
