-- The trivial INSERT SELECT optimization rewrites `max_threads` of the inner SELECT
-- from `max_insert_threads`. The value must be capped by `max_threads`: with the default
-- `max_insert_threads = 'auto'` the raw setting resolves to the number of CPU cores,
-- but an explicit `max_threads = 1` must still limit the inner SELECT to a single thread.

DROP TABLE IF EXISTS t_trivial_insert_select_cap;
CREATE TABLE t_trivial_insert_select_cap (x UInt64) ENGINE = MergeTree ORDER BY ();

INSERT INTO t_trivial_insert_select_cap
SELECT getSetting('max_threads') FROM numbers_mt(1)
SETTINGS optimize_trivial_insert_select = 1, max_threads = 1, max_insert_threads = 'auto';

SELECT x FROM t_trivial_insert_select_cap;

DROP TABLE t_trivial_insert_select_cap;
