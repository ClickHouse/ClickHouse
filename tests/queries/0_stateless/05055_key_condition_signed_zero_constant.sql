-- `-0.` and `+0.` compare equal but are distinct values, so a key transform that reads the bit pattern
-- maps them to two key values while the comparison feeding it treats them as one. A point range built
-- from one spelling must not prune the mark holding the other.

DROP TABLE IF EXISTS t_signed_zero;
CREATE TABLE t_signed_zero (f Float64) ENGINE = MergeTree ORDER BY toString(f)
SETTINGS index_granularity = 1, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_signed_zero VALUES (-0);

SELECT 'equals', count() FROM t_signed_zero WHERE f = 0;
SELECT 'equals, ground truth', countIf(f = 0) FROM t_signed_zero;
-- An implicit projection may answer this count from part metadata when key analysis reports the
-- filter always-true for a part, which is what the wrong range does here, so both projection
-- settings stay at their defaults for this row.
SELECT 'notEquals', count() FROM t_signed_zero WHERE f != 0
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SELECT 'notEquals, ground truth', countIf(f != 0) FROM t_signed_zero;

DROP TABLE IF EXISTS t_signed_zero_tuple;
CREATE TABLE t_signed_zero_tuple (k Tuple(Float64, Float64)) ENGINE = MergeTree ORDER BY toString(k)
SETTINGS index_granularity = 1, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_signed_zero_tuple VALUES ((-0, 1));

SELECT 'zero inside a tuple constant', count() FROM t_signed_zero_tuple WHERE k = (0., 1.);
SELECT 'zero inside a tuple constant, ground truth', countIf(k = (0., 1.)) FROM t_signed_zero_tuple;

-- A container constant with no zero inside it names a single key value, so its range still prunes.
DROP TABLE IF EXISTS t_signed_zero_tuple_control;
CREATE TABLE t_signed_zero_tuple_control (k Tuple(Float64, Float64)) ENGINE = MergeTree ORDER BY toString(k)
SETTINGS index_granularity = 1, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_signed_zero_tuple_control VALUES ((1, 1)), ((2, 1)), ((3, 1));

SELECT 'non-zero constant inside a tuple', count() FROM t_signed_zero_tuple_control WHERE k = (2., 1.);
SELECT 'non-zero constant inside a tuple', extract(explain, 'Granules: \\d+/\\d+')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_signed_zero_tuple_control WHERE k = (2., 1.))
WHERE match(explain, 'Granules: \\d+/\\d+');

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

-- A non-zero constant names one partition value, so the atom still reaches the partition pruner.
DROP TABLE IF EXISTS t_signed_zero_partition_control;
CREATE TABLE t_signed_zero_partition_control (f Float64) ENGINE = MergeTree PARTITION BY toString(f) ORDER BY tuple()
SETTINGS index_granularity = 1, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_signed_zero_partition_control VALUES (1), (2);

SELECT 'partition key, non-zero constant', count() FROM t_signed_zero_partition_control WHERE f = 2;
SELECT 'partition key, non-zero constant, the pruner still gets the atom', count()
FROM (EXPLAIN indexes = 1 SELECT f FROM t_signed_zero_partition_control WHERE f = 2)
WHERE explain ILIKE '%Condition:%toString(f)%';

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

-- A dynamically typed domain compares values of different types as equal, so a zero there stands for a
-- stored `-0.` however it is written, including on an alternative holding no floating-point value.
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

-- A container can hold a dynamically typed member beside a fixed-typed one, and each member reads a zero
-- inside it in its own domain.
DROP TABLE IF EXISTS t_signed_zero_dynamic_sibling;
CREATE TABLE t_signed_zero_dynamic_sibling (k Tuple(UInt64, Dynamic)) ENGINE = MergeTree ORDER BY toString(k)
SETTINGS index_granularity = 1, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_signed_zero_dynamic_sibling VALUES ((1, toFloat64('-0')::Dynamic));

SELECT 'zero on the dynamically typed member', count() FROM t_signed_zero_dynamic_sibling
WHERE k = (1, toUInt64(0))::Tuple(UInt64, Dynamic);
SELECT 'zero on the dynamically typed member, ground truth',
    countIf(k = (1, toUInt64(0))::Tuple(UInt64, Dynamic)) FROM t_signed_zero_dynamic_sibling;

-- A fixed-typed member has one spelling of a zero, so a zero there still names a single key value.
DROP TABLE IF EXISTS t_signed_zero_dynamic_sibling_control;
CREATE TABLE t_signed_zero_dynamic_sibling_control (k Tuple(UInt64, Dynamic)) ENGINE = MergeTree ORDER BY toString(k)
SETTINGS index_granularity = 1, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_signed_zero_dynamic_sibling_control VALUES ((0, toUInt64(1))), ((0, toUInt64(2))), ((1, toUInt64(1)));

SELECT 'zero on a fixed-typed member beside a dynamically typed one', count()
FROM t_signed_zero_dynamic_sibling_control WHERE k = (0, toUInt64(1))::Tuple(UInt64, Dynamic);
SELECT 'zero on a fixed-typed member beside a dynamically typed one, ground truth',
    countIf(k = (0, toUInt64(1))::Tuple(UInt64, Dynamic)) FROM t_signed_zero_dynamic_sibling_control;
SELECT 'zero on a fixed-typed member beside a dynamically typed one', extract(explain, 'Granules: \\d+/\\d+')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_signed_zero_dynamic_sibling_control
    WHERE k = (0, toUInt64(1))::Tuple(UInt64, Dynamic))
WHERE match(explain, 'Granules: \\d+/\\d+');

-- `Variant` is a second such domain, and a constant there can sit on the integer alternative, so it
-- carries no floating-point value at all while still standing for a stored `-0.`.
DROP TABLE IF EXISTS t_signed_zero_variant;
CREATE TABLE t_signed_zero_variant (v Variant(Float64, UInt64)) ENGINE = MergeTree ORDER BY toString(v)
SETTINGS index_granularity = 1, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_signed_zero_variant VALUES (toFloat64('-0')::Variant(Float64, UInt64));

SELECT 'variant domain', count() FROM t_signed_zero_variant WHERE v = toUInt64(0)::Variant(Float64, UInt64);
SELECT 'variant domain, ground truth', countIf(v = toUInt64(0)::Variant(Float64, UInt64)) FROM t_signed_zero_variant;

-- A string that parses as a zero is one of those spellings, and a comparison with it reads a stored
-- `-0.` as a number.
DROP TABLE IF EXISTS t_signed_zero_dynamic_string;
CREATE TABLE t_signed_zero_dynamic_string (j JSON) ENGINE = MergeTree ORDER BY j.b::String
SETTINGS index_granularity = 1, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_signed_zero_dynamic_string VALUES ('{"b":-0.0}');

SELECT 'zero written as a string', count() FROM t_signed_zero_dynamic_string WHERE j.b = '0';
SELECT 'zero written as a string, ground truth', countIf(j.b = '0') FROM t_signed_zero_dynamic_string;

-- A constant that denotes no number stands for itself alone there, so its range still prunes.
DROP TABLE IF EXISTS t_signed_zero_dynamic_control;
CREATE TABLE t_signed_zero_dynamic_control (j JSON) ENGINE = MergeTree ORDER BY j.b::String
SETTINGS index_granularity = 1, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_signed_zero_dynamic_control VALUES ('{"b":"str_0"}'), ('{"b":"str_1"}'), ('{"b":"str_2"}');

SELECT 'constant denoting no number', count() FROM t_signed_zero_dynamic_control WHERE j.b = 'str_0';
SELECT 'constant denoting no number, ground truth', countIf(j.b = 'str_0') FROM t_signed_zero_dynamic_control;
SELECT 'constant denoting no number', extract(explain, 'Granules: \\d+/\\d+')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_signed_zero_dynamic_control WHERE j.b = 'str_0')
WHERE match(explain, 'Granules: \\d+/\\d+');

DROP TABLE t_signed_zero;
DROP TABLE t_signed_zero_tuple;
DROP TABLE t_signed_zero_tuple_control;
DROP TABLE t_signed_zero_nonzero;
DROP TABLE t_signed_zero_negate;
DROP TABLE t_signed_zero_not_distinct;
DROP TABLE t_signed_zero_partition;
DROP TABLE t_signed_zero_partition_control;
DROP TABLE t_signed_zero_minmax;
DROP TABLE t_signed_zero_minmax_control;
DROP TABLE t_signed_zero_dynamic;
DROP TABLE t_signed_zero_dynamic_nested;
DROP TABLE t_signed_zero_dynamic_sibling;
DROP TABLE t_signed_zero_dynamic_sibling_control;
DROP TABLE t_signed_zero_variant;
DROP TABLE t_signed_zero_dynamic_string;
DROP TABLE t_signed_zero_dynamic_control;
