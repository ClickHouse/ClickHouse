-- An Enum constant compared against a String/FixedString column used to be converted to the enum's
-- underlying number instead of its name, so key analysis, skip indexes and IN sets all used the wrong
-- bytes. Every assertion below prints 1.
--
-- The enum is Enum8('7' = 3): the name '7' and the number 3 are both valid strings and both are stored,
-- so a cell distinguishes "returned nothing" from "returned the WRONG row".
--
-- Reference values: for = and range predicates the reference is an unindexed table, which is correct
-- even before the fix. For the IN family the reference is the equivalent String literal, because IN set
-- construction goes through convertFieldToType and is storage independent, so an unindexed table returns
-- the same wrong answer and could not detect the bug.

DROP TABLE IF EXISTS ref_str;
DROP TABLE IF EXISTS pk_str;
DROP TABLE IF EXISTS pk_lc;
DROP TABLE IF EXISTS pk_nullable;
DROP TABLE IF EXISTS pk_fixed1;
DROP TABLE IF EXISTS pk_fixed4;
DROP TABLE IF EXISTS pk_partition;
DROP TABLE IF EXISTS bf_str;
DROP TABLE IF EXISTS bf_fixed4;
DROP TABLE IF EXISTS bf_array;
DROP TABLE IF EXISTS pk_pair;
DROP TABLE IF EXISTS pk_fixed10;
DROP TABLE IF EXISTS cast_ref;

CREATE TABLE ref_str (v String) ENGINE = Log;
INSERT INTO ref_str VALUES ('7'), ('3'), ('V0'), ('zz');

CREATE TABLE pk_str (v String) ENGINE = MergeTree ORDER BY v SETTINGS index_granularity = 1;
INSERT INTO pk_str VALUES ('7'), ('3'), ('V0'), ('zz');

CREATE TABLE pk_lc (v LowCardinality(String)) ENGINE = MergeTree ORDER BY v SETTINGS index_granularity = 1;
INSERT INTO pk_lc VALUES ('7'), ('3'), ('V0'), ('zz');

CREATE TABLE pk_nullable (v Nullable(String)) ENGINE = MergeTree ORDER BY v
SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO pk_nullable VALUES ('7'), ('3'), ('V0'), ('zz');

-- FixedString(1) is narrower than the name, FixedString(4) is wider: the wider one only matches if the
-- converted name is zero padded to the column width.
CREATE TABLE pk_fixed1 (v FixedString(1)) ENGINE = MergeTree ORDER BY v SETTINGS index_granularity = 1;
INSERT INTO pk_fixed1 VALUES ('7'), ('3');

CREATE TABLE pk_fixed4 (v FixedString(4)) ENGINE = MergeTree ORDER BY v SETTINGS index_granularity = 1;
INSERT INTO pk_fixed4 VALUES ('7'), ('3');

CREATE TABLE pk_partition (v String) ENGINE = MergeTree PARTITION BY v ORDER BY tuple();
INSERT INTO pk_partition VALUES ('7'), ('3'), ('V0');

-- The bloom filter tables deliberately do NOT store the string '3', and they use a low false positive
-- rate. With the default rate this fixture is small enough that '3' collides with an existing granule,
-- so the wrongly converted constant would keep the right granule by accident and the cell would pass
-- before the fix. Measured on the default rate: v = '3' yields Granules: 1/4 although no row matches.
CREATE TABLE bf_str (id UInt64, v String, INDEX idx v TYPE bloom_filter(0.001) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO bf_str VALUES (0, '7'), (1, 'V0'), (2, 'zz'), (3, 'qq');

CREATE TABLE bf_fixed4 (id UInt64, v FixedString(4), INDEX idx v TYPE bloom_filter(0.001) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO bf_fixed4 VALUES (0, '7'), (1, 'V0'), (2, 'zz'), (3, 'qq');

CREATE TABLE bf_array (id UInt64, v Array(String), INDEX idx v TYPE bloom_filter(0.001) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO bf_array VALUES (0, ['7']), (1, ['V0']);

CREATE TABLE pk_pair (a String, b String) ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 1;
INSERT INTO pk_pair VALUES ('7', 'x'), ('3', 'x'), ('V0', 'x');

-- Pins that the FixedString padding path itself is unchanged (the 01503_fixed_string_primary_key shape).
CREATE TABLE pk_fixed10 (key FixedString(10), i Int) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1;
INSERT INTO pk_fixed10 SELECT toFixedString(toString(number % 10), 10), number FROM numbers(80);

-- INSERT ... SELECT converts through castColumn, which was already correct: the reference for values().
CREATE TABLE cast_ref (x String) ENGINE = Log;
INSERT INTO cast_ref SELECT CAST('7', 'Enum8(\'7\' = 3)');

SELECT 'pk_equals', (SELECT groupArray(v) FROM pk_str WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)
    = (SELECT groupArray(v) FROM ref_str WHERE v = CAST('7', 'Enum8(\'7\' = 3)'));

SELECT 'pk_less', (SELECT groupArray(v) FROM pk_str WHERE v < CAST('7', 'Enum8(\'7\' = 3)')
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)
    = (SELECT groupArray(v) FROM ref_str WHERE v < CAST('7', 'Enum8(\'7\' = 3)'));

SELECT 'pk_not_equals_control', (SELECT arraySort(groupArray(v)) FROM pk_str WHERE v != CAST('7', 'Enum8(\'7\' = 3)')
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)
    = (SELECT arraySort(groupArray(v)) FROM ref_str WHERE v != CAST('7', 'Enum8(\'7\' = 3)'));

SELECT 'pk_low_cardinality', (SELECT groupArray(toString(v)) FROM pk_lc WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)
    = (SELECT groupArray(v) FROM ref_str WHERE v = CAST('7', 'Enum8(\'7\' = 3)'));

SELECT 'pk_nullable', (SELECT groupArray(v) FROM pk_nullable WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)
    = (SELECT groupArray(v) FROM ref_str WHERE v = CAST('7', 'Enum8(\'7\' = 3)'));

SELECT 'pk_fixed_string_narrow', (SELECT groupArray(toString(v)) FROM pk_fixed1 WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0) = ['7'];

SELECT 'pk_fixed_string_wide', (SELECT groupArray(trim(toString(v))) FROM pk_fixed4 WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0) = ['7'];

SELECT 'partition_key', (SELECT groupArray(v) FROM pk_partition WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
    SETTINGS optimize_use_implicit_projections = 0)
    = (SELECT groupArray(v) FROM ref_str WHERE v = CAST('7', 'Enum8(\'7\' = 3)'));

SELECT 'bloom_filter_equals', (SELECT groupArray(v) FROM bf_str WHERE v = CAST('7', 'Enum8(\'7\' = 3)'))
    = (SELECT groupArray(v) FROM bf_str WHERE v = CAST('7', 'Enum8(\'7\' = 3)') SETTINGS use_skip_indexes = 0);

-- Only matches if the name is padded to the column width before it is hashed.
SELECT 'bloom_filter_fixed_string', (SELECT groupArray(trim(toString(v))) FROM bf_fixed4 WHERE v = CAST('7', 'Enum8(\'7\' = 3)')) = ['7'];

SELECT 'bloom_filter_in', (SELECT groupArray(v) FROM bf_str WHERE v IN (CAST('7', 'Enum8(\'7\' = 3)')))
    = (SELECT groupArray(v) FROM bf_str WHERE v IN ('7'));

SELECT 'bloom_filter_has', (SELECT groupArray(v) FROM bf_array WHERE has(v, CAST('7', 'Enum8(\'7\' = 3)')))
    = (SELECT groupArray(v) FROM bf_array WHERE has(v, CAST('7', 'Enum8(\'7\' = 3)')) SETTINGS use_skip_indexes = 0);

SELECT 'in_set', (SELECT groupArray(v) FROM ref_str WHERE v IN (CAST('7', 'Enum8(\'7\' = 3)')))
    = (SELECT groupArray(v) FROM ref_str WHERE v IN ('7'));

-- NOT IN was inverted, not merely over pruning: it excluded the number and returned the name.
SELECT 'not_in', (SELECT arraySort(groupArray(v)) FROM pk_str WHERE v NOT IN (CAST('7', 'Enum8(\'7\' = 3)'))
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)
    = (SELECT arraySort(groupArray(v)) FROM pk_str WHERE v NOT IN ('7')
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0);

-- An OR chain of enum constants is rewritten to IN, which used to drop a disjunct.
SELECT 'or_chain_rewritten_to_in', (SELECT arraySort(groupArray(v)) FROM pk_str
    WHERE v = CAST('7', 'Enum8(\'7\' = 3, \'zz\' = 9)') OR v = CAST('zz', 'Enum8(\'7\' = 3, \'zz\' = 9)')
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)
    = (SELECT arraySort(groupArray(v)) FROM pk_str WHERE v IN ('7', 'zz')
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0);

SELECT 'tuple_in', (SELECT groupArray(a) FROM pk_pair WHERE (a, b) IN ((CAST('7', 'Enum8(\'7\' = 3)'), 'x'))
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)
    = (SELECT groupArray(a) FROM pk_pair WHERE (a, b) IN (('7', 'x'))
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0);

-- Nullable(Enum) reaches the conversion with the wrapper still on the source type hint.
SELECT 'nullable_enum_equals', (SELECT groupArray(v) FROM pk_str
    WHERE v = CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Nullable(Enum8('7' = 3)))
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)
    = (SELECT groupArray(v) FROM ref_str WHERE v = '7');

SELECT 'nullable_enum_in', (SELECT groupArray(v) FROM ref_str
    WHERE v IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Nullable(Enum8('7' = 3)))))
    = (SELECT groupArray(v) FROM ref_str WHERE v IN ('7'));

SELECT 'nullable_enum_values', (SELECT hex(x) FROM values('x String', CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Nullable(Enum8('7' = 3)))))
    = (SELECT hex(x) FROM cast_ref);

-- values() used to write the number where INSERT ... SELECT writes the name.
SELECT 'values_table_function', (SELECT hex(x) FROM values('x String', CAST('7', 'Enum8(\'7\' = 3)')))
    = (SELECT hex(x) FROM cast_ref);

SELECT 'enum_with_extra_label', (SELECT groupArray(v) FROM pk_str WHERE v = CAST('7', 'Enum8(\'7\' = 3, \'nope\' = 9)')
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)
    = (SELECT groupArray(v) FROM ref_str WHERE v = '7');

SELECT 'enum16', (SELECT groupArray(v) FROM pk_str WHERE v = CAST('7', 'Enum16(\'7\' = 3)')
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)
    = (SELECT groupArray(v) FROM ref_str WHERE v = '7');

-- Correctness could also be restored by declining the index, which would silently cost pruning, so the
-- enum constant must prune exactly as much as the equivalent String literal.
SELECT 'pk_still_prunes', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_str WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
          SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0))
    = (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_str WHERE v = '7'
          SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0));

-- The reference above would also be satisfied if the enum constant made key analysis give up, because a
-- declined index reads every granule. This pins that some granules really are skipped, so the constant is
-- still turned into a usable range rather than being dropped.
SELECT 'pk_prunes_something', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    < sum(toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_str WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
          SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0));

SELECT 'bloom_filter_still_prunes', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM bf_str WHERE v = CAST('7', 'Enum8(\'7\' = 3)')))
    = (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM bf_str WHERE v = '7'));

-- Conversions that must not change.
SELECT 'control_string_to_enum', toInt8(CAST('7', 'Enum8(\'7\' = 3)')) = 3;
SELECT 'control_enum_to_string_cast', CAST(CAST('7', 'Enum8(\'7\' = 3)') AS String) = '7';
SELECT 'control_bool_to_string', CAST(true, 'String') = 'true';
SELECT 'control_plain_string_constant', (SELECT groupArray(v) FROM pk_str WHERE v = '7'
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0) = ['7'];
SELECT 'control_fixed_string_padding', (SELECT count() FROM pk_fixed10 WHERE key = '1'
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0) = 8;

-- An Enum element inside a container is still converted to the number, because the Array/Tuple/Map
-- recursion of convertFieldToType passes no element type hint down, and so does
-- createColumnFromConstantArray in the bloom filter condition. The cells below assert the current
-- (wrong) values rather than the desired ones, so the limitation is documented instead of hidden.
-- The hint propagation is added by https://github.com/ClickHouse/ClickHouse/pull/110084.
SELECT 'known_limitation_array_element', (SELECT hex(x[1]) FROM values('x Array(String)', [CAST('7', 'Enum8(\'7\' = 3)')])) = '33';

-- hasAny and hasAll go through createColumnFromConstantArray, so their bloom filter lookup still uses
-- the number and over prunes. Unlike has, which converts the element with the hint and is fixed above.
SELECT 'known_limitation_has_any', (SELECT groupArray(v) FROM bf_array WHERE hasAny(v, [CAST('7', 'Enum8(\'7\' = 3)')])) = [];
SELECT 'known_limitation_has_all', (SELECT groupArray(v) FROM bf_array WHERE hasAll(v, [CAST('7', 'Enum8(\'7\' = 3)')])) = [];

DROP TABLE ref_str;
DROP TABLE pk_str;
DROP TABLE pk_lc;
DROP TABLE pk_nullable;
DROP TABLE pk_fixed1;
DROP TABLE pk_fixed4;
DROP TABLE pk_partition;
DROP TABLE bf_str;
DROP TABLE bf_fixed4;
DROP TABLE bf_array;
DROP TABLE pk_pair;
DROP TABLE pk_fixed10;
DROP TABLE cast_ref;
