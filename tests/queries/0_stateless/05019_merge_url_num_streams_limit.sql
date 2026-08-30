-- `URL` reads one URI but may resize its output after reading. A `Merge` read
-- must account for the URI bound and disable that resize for an excessive request.
DROP TABLE IF EXISTS t_merge_url_num_streams_limit;
DROP TABLE IF EXISTS t_url_num_streams_limit;

CREATE TABLE t_url_num_streams_limit (n UInt64)
ENGINE = URL('http://127.0.0.1:8123/?query=SELECT%201%20FORMAT%20TabSeparated', TabSeparated);

CREATE TABLE t_merge_url_num_streams_limit (n UInt64)
ENGINE = Merge(currentDatabase(), '^t_url_num_streams_limit$');

SELECT count() FROM t_merge_url_num_streams_limit
SETTINGS max_threads = 4, max_streams_to_max_threads_ratio = 1073741824;

DROP TABLE t_merge_url_num_streams_limit;
DROP TABLE t_url_num_streams_limit;
