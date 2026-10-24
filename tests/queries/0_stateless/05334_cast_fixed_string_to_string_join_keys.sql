-- The comparison of `FixedString` with `String` is zero-padded, and the join keys follow it,
-- whatever `cast_fixed_string_to_string_strip_trailing_zeros` is.

DROP TABLE IF EXISTS t_fs;
DROP TABLE IF EXISTS t_s;

CREATE TABLE t_fs (fs FixedString(3)) ENGINE = MergeTree ORDER BY fs;
CREATE TABLE t_s (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_fs VALUES ('a'), ('ab'), ('abc');
INSERT INTO t_s VALUES ('a'), ('ab'), ('x');

SELECT 'where', count() FROM t_fs, t_s WHERE fs = s;
SELECT 'on', count() FROM t_fs JOIN t_s ON fs = s;
SELECT 'on, swapped', count() FROM t_s JOIN t_fs ON s = fs;
SELECT 'on, no runtime filters', count() FROM t_fs JOIN t_s ON fs = s SETTINGS enable_join_runtime_filters = 0;
SELECT 'on, runtime filters', count() FROM t_fs JOIN t_s ON fs = s SETTINGS enable_join_runtime_filters = 1;
SELECT 'on, parallel_hash', count() FROM t_fs JOIN t_s ON fs = s SETTINGS join_algorithm = 'parallel_hash';
SELECT 'on, full_sorting_merge', count() FROM t_fs JOIN t_s ON fs = s SETTINGS join_algorithm = 'full_sorting_merge';
SELECT 'on, old analyzer', count() FROM t_fs JOIN t_s ON fs = s SETTINGS enable_analyzer = 0;
SELECT 'on, strip', count() FROM t_fs JOIN t_s ON fs = s SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 1;
SELECT 'on, filter', count() FROM t_fs JOIN t_s ON fs = s WHERE s = 'ab';
SELECT 'using', fs, hex(fs) FROM t_fs JOIN (SELECT s AS fs FROM t_s) AS x USING fs ORDER BY fs;
SELECT 'using, runtime filters', count() FROM t_fs JOIN (SELECT s AS fs FROM t_s) AS x USING fs SETTINGS enable_join_runtime_filters = 1;
SELECT 'using, old analyzer', count() FROM t_fs JOIN (SELECT s AS fs FROM t_s) AS x USING fs SETTINGS enable_analyzer = 0;

DROP TABLE t_fs;
DROP TABLE t_s;
