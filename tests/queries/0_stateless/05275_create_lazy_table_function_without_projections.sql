-- A CREATE with no projections must not resolve a lazy table function just to inspect its metadata.
SET http_max_tries = 1, connect_timeout = 1, http_receive_timeout = 1;
DROP TABLE IF EXISTS lazy_table_function_without_projections;
CREATE TABLE lazy_table_function_without_projections (x String) AS url('http://127.0.0.1:1/nonexistent');
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'lazy_table_function_without_projections';
DROP TABLE lazy_table_function_without_projections;
