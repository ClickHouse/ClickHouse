-- `-0.` and `+0.` compare equal but are distinct values, so a key transform that reads the bit pattern
-- maps them to two key values while the comparison feeding it treats them as one. A point range built
-- from one spelling must not prune the mark holding the other.

DROP TABLE IF EXISTS t_signed_zero;
CREATE TABLE t_signed_zero (f Float64) ENGINE = MergeTree ORDER BY toString(f)
SETTINGS index_granularity = 1, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_signed_zero VALUES (-0);

SELECT 'equals', count() FROM t_signed_zero WHERE f = 0;
SELECT 'equals, ground truth', countIf(f = 0) FROM t_signed_zero;
-- An implicit projection can answer this count from part metadata, and the metadata is what the
-- wrong range describes, so both projection settings stay at their defaults for this row.
SELECT 'notEquals', count() FROM t_signed_zero WHERE f != 0
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;
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

-- `IS NOT DISTINCT FROM` builds the same atom, and the atom also feeds the partition pruner and a
-- `minmax` skip index.
DROP TABLE IF EXISTS t_signed_zero_not_distinct;
CREATE TABLE t_signed_zero_not_distinct (f Float64) ENGINE = MergeTree ORDER BY toString(f)
SETTINGS index_granularity = 1, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_signed_zero_not_distinct VALUES (-0);

SELECT 'isNotDistinctFrom', count() FROM t_signed_zero_not_distinct WHERE f IS NOT DISTINCT FROM 0;
SELECT 'isNotDistinctFrom, ground truth', countIf(f IS NOT DISTINCT FROM 0) FROM t_signed_zero_not_distinct;

DROP TABLE IF EXISTS t_signed_zero_partition;
CREATE TABLE t_signed_zero_partition (f Float64) ENGINE = MergeTree PARTITION BY toString(f) ORDER BY tuple()
SETTINGS index_granularity = 1, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_signed_zero_partition VALUES (-0);

SELECT 'partition key', count() FROM t_signed_zero_partition WHERE f = 0;
SELECT 'partition key, ground truth', countIf(f = 0) FROM t_signed_zero_partition;

DROP TABLE IF EXISTS t_signed_zero_minmax;
CREATE TABLE t_signed_zero_minmax (f Float64, INDEX idx toString(f) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_signed_zero_minmax VALUES (-0);

SELECT 'minmax index', count() FROM t_signed_zero_minmax WHERE f = 0;
SELECT 'minmax index, ground truth', countIf(f = 0) FROM t_signed_zero_minmax;

-- A non-zero constant leaves the skip index a range to compare, so it still drops granules there.
DROP TABLE IF EXISTS t_signed_zero_minmax_control;
CREATE TABLE t_signed_zero_minmax_control (f Float64, INDEX idx toString(f) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_signed_zero_minmax_control VALUES (1), (2), (3);

SELECT 'minmax index, non-zero constant', count() FROM t_signed_zero_minmax_control WHERE f = 2;
SELECT 'minmax index, non-zero constant, ground truth', countIf(f = 2) FROM t_signed_zero_minmax_control;
SELECT 'minmax index, non-zero constant', extract(explain, 'Granules: \\d+/\\d+')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_signed_zero_minmax_control WHERE f = 2)
WHERE match(explain, 'Granules: \\d+/\\d+');

-- A dynamically typed domain compares values of different types as equal, so a constant there does not
-- name a single stored value and no range can be built from it, whatever the constant's own type is.
DROP TABLE IF EXISTS t_signed_zero_dynamic;
CREATE TABLE t_signed_zero_dynamic (d Dynamic) ENGINE = MergeTree ORDER BY toString(d)
SETTINGS index_granularity = 1, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_signed_zero_dynamic VALUES (toFloat64('-0')::Dynamic);

SELECT 'dynamically typed domain', count() FROM t_signed_zero_dynamic WHERE d = toUInt64(0)::Dynamic;
SELECT 'dynamically typed domain, ground truth', countIf(d = toUInt64(0)::Dynamic) FROM t_signed_zero_dynamic;

DROP TABLE IF EXISTS t_signed_zero_dynamic_nested;
CREATE TABLE t_signed_zero_dynamic_nested (d Tuple(Dynamic)) ENGINE = MergeTree ORDER BY toString(d)
SETTINGS index_granularity = 1, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_signed_zero_dynamic_nested VALUES (tuple(-0.0::Float64));

SELECT 'dynamically typed domain nested in a container', count() FROM t_signed_zero_dynamic_nested WHERE d = tuple(0);
SELECT 'dynamically typed domain nested in a container, ground truth', countIf(d = tuple(0)) FROM t_signed_zero_dynamic_nested;

DROP TABLE t_signed_zero;
DROP TABLE t_signed_zero_tuple;
DROP TABLE t_signed_zero_nonzero;
DROP TABLE t_signed_zero_negate;
DROP TABLE t_signed_zero_not_distinct;
DROP TABLE t_signed_zero_partition;
DROP TABLE t_signed_zero_minmax;
DROP TABLE t_signed_zero_minmax_control;
DROP TABLE t_signed_zero_dynamic;
DROP TABLE t_signed_zero_dynamic_nested;
