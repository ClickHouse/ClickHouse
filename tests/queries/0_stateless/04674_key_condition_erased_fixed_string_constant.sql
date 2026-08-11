-- A `FixedString(N)` constant hidden inside a `Variant`/`Dynamic` wrapper is compared zero-padded,
-- so a point key range built from its padded bytes prunes matching granules. Each `equals`/`notEquals`
-- arm below must agree with the unindexed `ENGINE = Log` oracle; each pruning arm must keep pruning.

DROP TABLE IF EXISTS oracle;
DROP TABLE IF EXISTS pk_string;
DROP TABLE IF EXISTS pk_low_cardinality;
DROP TABLE IF EXISTS pk_nullable;
DROP TABLE IF EXISTS pk_fixed_string_5;
DROP TABLE IF EXISTS oracle_fixed_string_5;
DROP TABLE IF EXISTS pk_fixed_string_3;
DROP TABLE IF EXISTS oracle_fixed_string_3;

CREATE TABLE oracle (s String) ENGINE = Log;
INSERT INTO oracle VALUES ('V0'), ('V0\0'), ('V0\0\0'), ('V0X'), ('X');

CREATE TABLE pk_string (s String) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1;
INSERT INTO pk_string SELECT s FROM oracle;

CREATE TABLE pk_low_cardinality (s LowCardinality(String)) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1;
INSERT INTO pk_low_cardinality SELECT s FROM oracle;

CREATE TABLE pk_nullable (s Nullable(String)) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO pk_nullable SELECT s FROM oracle;

CREATE TABLE oracle_fixed_string_5 (s FixedString(5)) ENGINE = Log;
INSERT INTO oracle_fixed_string_5 SELECT s FROM oracle;
CREATE TABLE pk_fixed_string_5 (s FixedString(5)) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1;
INSERT INTO pk_fixed_string_5 SELECT s FROM oracle;

CREATE TABLE oracle_fixed_string_3 (s FixedString(3)) ENGINE = Log;
INSERT INTO oracle_fixed_string_3 VALUES ('V0'), ('V0X'), ('X');
CREATE TABLE pk_fixed_string_3 (s FixedString(3)) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1;
INSERT INTO pk_fixed_string_3 SELECT s FROM oracle_fixed_string_3;

SELECT '-- A: equals agrees with the oracle (count and row set)';

SELECT 'String key, Variant(FixedString(3))',
    (SELECT count() FROM oracle WHERE s = CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64)))
        = (SELECT count() FROM pk_string WHERE s = CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64))),
    (SELECT groupArray(hex(s)) FROM (SELECT s FROM oracle WHERE s = CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64)) ORDER BY s))
        = (SELECT groupArray(hex(s)) FROM (SELECT s FROM pk_string WHERE s = CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64)) ORDER BY s));

SELECT 'String key, Dynamic holding FixedString(3)',
    (SELECT count() FROM oracle WHERE s = CAST(toFixedString('V0', 3) AS Dynamic))
        = (SELECT count() FROM pk_string WHERE s = CAST(toFixedString('V0', 3) AS Dynamic));

SELECT 'String key, Variant(LowCardinality(FixedString(3)))',
    (SELECT count() FROM oracle WHERE s = CAST(CAST(toFixedString('V0', 3) AS LowCardinality(FixedString(3))) AS Variant(LowCardinality(FixedString(3)), UInt64)))
        = (SELECT count() FROM pk_string WHERE s = CAST(CAST(toFixedString('V0', 3) AS LowCardinality(FixedString(3))) AS Variant(LowCardinality(FixedString(3)), UInt64)));

SELECT 'String key, Dynamic holding LowCardinality(FixedString(3))',
    (SELECT count() FROM oracle WHERE s = CAST(CAST(toFixedString('V0', 3) AS LowCardinality(FixedString(3))) AS Dynamic))
        = (SELECT count() FROM pk_string WHERE s = CAST(CAST(toFixedString('V0', 3) AS LowCardinality(FixedString(3))) AS Dynamic));

-- max_types = 0 stores the value in the shared variant, which encodes its type separately.
SELECT 'String key, Dynamic(max_types = 0)',
    (SELECT count() FROM oracle WHERE s = CAST(toFixedString('V0', 3) AS Dynamic(max_types = 0)))
        = (SELECT count() FROM pk_string WHERE s = CAST(toFixedString('V0', 3) AS Dynamic(max_types = 0)));

-- A constant that exactly fills its declared width is padded no less: comparison pads the key side too.
SELECT 'String key, Variant(FixedString(2)) filled exactly',
    (SELECT count() FROM oracle WHERE s = CAST(toFixedString('V0', 2) AS Variant(FixedString(2), UInt64)))
        = (SELECT count() FROM pk_string WHERE s = CAST(toFixedString('V0', 2) AS Variant(FixedString(2), UInt64)));

SELECT 'LowCardinality(String) key, Variant(FixedString(3))',
    (SELECT count() FROM oracle WHERE s = CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64)))
        = (SELECT count() FROM pk_low_cardinality WHERE s = CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64)));

SELECT 'Nullable(String) key, Variant(FixedString(3))',
    (SELECT count() FROM oracle WHERE s = CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64)))
        = (SELECT count() FROM pk_nullable WHERE s = CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64)));

SELECT '-- A2: notEquals agrees with the oracle (exact-count projection pinned on)';

-- Both settings are pinned because only `_exact_count_projection` derives the count from the key
-- ranges; with either one off the plan filters rows and the comparison holds even for a wrong range.
SELECT 'String key, Variant(FixedString(3))',
    (SELECT count() FROM oracle WHERE s != CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64)))
        = (SELECT count() FROM pk_string WHERE s != CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64)))
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;

SELECT 'String key, Dynamic holding FixedString(3)',
    (SELECT count() FROM oracle WHERE s != CAST(toFixedString('V0', 3) AS Dynamic))
        = (SELECT count() FROM pk_string WHERE s != CAST(toFixedString('V0', 3) AS Dynamic))
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;

SELECT '-- Keys that already pruned correctly are untouched';

SELECT 'FixedString(5) key, Variant(FixedString(3))',
    (SELECT count() FROM oracle_fixed_string_5 WHERE s = CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64)))
        = (SELECT count() FROM pk_fixed_string_5 WHERE s = CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64)));

SELECT 'FixedString(3) key, Variant(FixedString(4))',
    (SELECT count() FROM oracle_fixed_string_3 WHERE s = CAST(toFixedString('V0XY', 4) AS Variant(FixedString(4), UInt64)))
        = (SELECT count() FROM pk_fixed_string_3 WHERE s = CAST(toFixedString('V0XY', 4) AS Variant(FixedString(4), UInt64)));

SELECT 'String key, plain FixedString(3)',
    (SELECT count() FROM oracle WHERE s = toFixedString('V0', 3))
        = (SELECT count() FROM pk_string WHERE s = toFixedString('V0', 3));

-- like/startsWith/match take the constant as a pattern and never reach the range builder.
SELECT 'String key, LIKE pattern',
    (SELECT count() FROM oracle WHERE s LIKE 'V0%') = (SELECT count() FROM pk_string WHERE s LIKE 'V0%');

SELECT '-- B: an erased constant whose active type is String keeps pruning';

SELECT 'plain String constant', count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_string WHERE s = 'V0') WHERE explain ILIKE '%Granules: 1/5%';

SELECT 'Variant(String) constant', count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_string WHERE s = CAST('V0' AS Variant(String, UInt64))) WHERE explain ILIKE '%Granules: 1/5%';

SELECT 'Dynamic holding String', count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_string WHERE s = CAST('V0' AS Dynamic)) WHERE explain ILIKE '%Granules: 1/5%';

-- A JSON subcolumn is Dynamic, so this reaches the same site with no CAST written.
SELECT 'JSON path constant', count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_string WHERE s = ('{"a":"V0"}'::JSON).a) WHERE explain ILIKE '%Granules: 1/5%';

SELECT 'FixedString(5) key, Variant(FixedString(3))', count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_fixed_string_5 WHERE s = CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64)))
WHERE explain ILIKE '%Granules: 3/5%';

SELECT '-- C: the declining arms report the index as unused';

SELECT count() FROM pk_string WHERE s = CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64)) SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT count() FROM pk_string WHERE s = CAST(toFixedString('V0', 3) AS Dynamic) SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT count() FROM pk_string WHERE s = CAST('V0' AS Variant(String, UInt64)) SETTINGS force_primary_key = 1;

DROP TABLE oracle;
DROP TABLE pk_string;
DROP TABLE pk_low_cardinality;
DROP TABLE pk_nullable;
DROP TABLE pk_fixed_string_5;
DROP TABLE oracle_fixed_string_5;
DROP TABLE pk_fixed_string_3;
DROP TABLE oracle_fixed_string_3;
