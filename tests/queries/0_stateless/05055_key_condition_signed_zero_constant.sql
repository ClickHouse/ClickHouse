-- `-0.` and `+0.` compare equal but are distinct values, so a key transform that reads the bit pattern
-- maps them to two key values while the comparison feeding it treats them as one. A point range built
-- from one spelling must not prune the mark holding the other.

DROP TABLE IF EXISTS t_signed_zero;
CREATE TABLE t_signed_zero (f Float64) ENGINE = MergeTree ORDER BY toString(f)
SETTINGS index_granularity = 1, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_signed_zero VALUES (-0);

SELECT 'equals', count() FROM t_signed_zero WHERE f = 0;
SELECT 'equals, ground truth', countIf(f = 0) FROM t_signed_zero;
SELECT 'notEquals', count() FROM t_signed_zero WHERE f != 0;
SELECT 'notEquals, ground truth', countIf(f != 0) FROM t_signed_zero;

DROP TABLE IF EXISTS t_signed_zero_tuple;
CREATE TABLE t_signed_zero_tuple (k Tuple(Float64, Float64)) ENGINE = MergeTree ORDER BY toString(k)
SETTINGS index_granularity = 1, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_signed_zero_tuple VALUES ((-0, 1));

SELECT 'zero inside a tuple constant', count() FROM t_signed_zero_tuple WHERE k = (0., 1.);
SELECT 'zero inside a tuple constant, ground truth', countIf(k = (0., 1.)) FROM t_signed_zero_tuple;

-- A non-zero constant names a single value, so its range still prunes.
DROP TABLE IF EXISTS t_signed_zero_nonzero;
CREATE TABLE t_signed_zero_nonzero (f Float64) ENGINE = MergeTree ORDER BY toString(f)
SETTINGS index_granularity = 1, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_signed_zero_nonzero VALUES (1), (2), (3);

SELECT 'non-zero constant', count() FROM t_signed_zero_nonzero WHERE f = 2;
SELECT 'non-zero constant', extract(explain, 'Granules: \\d+/\\d+')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_signed_zero_nonzero WHERE f = 2)
WHERE match(explain, 'Granules: \\d+/\\d+');

-- `negate` sends both spellings to key values the index compares as equal, so a zero constant keeps
-- pruning there, and it still finds the row holding the other spelling.
DROP TABLE IF EXISTS t_signed_zero_negate;
CREATE TABLE t_signed_zero_negate (f Float64) ENGINE = MergeTree ORDER BY (-f)
SETTINGS index_granularity = 1, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_signed_zero_negate VALUES (-1), (-2), (-0);

SELECT 'zero constant, transform keeps the values equal', count() FROM t_signed_zero_negate WHERE f = 0;
SELECT 'zero constant, transform keeps the values equal, ground truth', countIf(f = 0) FROM t_signed_zero_negate;
SELECT 'zero constant, transform keeps the values equal', extract(explain, 'Granules: \\d+/\\d+')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_signed_zero_negate WHERE f = 0)
WHERE match(explain, 'Granules: \\d+/\\d+');

DROP TABLE t_signed_zero;
DROP TABLE t_signed_zero_tuple;
DROP TABLE t_signed_zero_nonzero;
DROP TABLE t_signed_zero_negate;
