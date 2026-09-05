-- Tags: no-fasttest
-- no-fasttest: 'countmin' sketches need a 3rd party library

SET explain_query_plan_default = 'legacy';
SET allow_statistics = 1;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET allow_reorder_prewhere_conditions = 1; -- CI may inject False, preventing statistics-based prewhere ordering
SET move_all_conditions_to_prewhere = 1;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_countmin_lc_numeric_fixture;

CREATE TABLE t_countmin_lc_numeric_fixture
(
    id UInt64,
    lc_value LowCardinality(UInt32) STATISTICS(countmin), -- LowCardinality column under test
    baseline_value UInt32 STATISTICS(countmin), -- same distribution without LowCardinality wrapper
    selective_probe UInt8 STATISTICS(countmin) -- known comparator used to prove ordering
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS auto_statistics_types = '', refresh_statistics_interval = 0;

INSERT INTO t_countmin_lc_numeric_fixture
SELECT
    number,
    toUInt32(if(number < 400, 42, number)),
    toUInt32(if(number < 400, 42, number)),
    toUInt8(number < 20)
FROM numbers(1000);

-- Sanity-check the fixture distribution used by the PREWHERE ordering assertions below.
SELECT 'numeric fixture distribution';
SELECT countIf(lc_value = 42), countIf(lc_value = 777), countIf(selective_probe = 1) FROM t_countmin_lc_numeric_fixture;

SELECT 'lc_value common value ordered after selective_probe';
SELECT countIf(
    position(prewhere_line, '__table1.selective_probe') > 0
    AND position(prewhere_line, '__table1.lc_value') > 0
    AND position(prewhere_line, '__table1.selective_probe') < position(prewhere_line, '__table1.lc_value')) = 1 FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM
    (
        EXPLAIN actions = 1
        SELECT count() FROM t_countmin_lc_numeric_fixture
        WHERE lc_value = 42 AND selective_probe = 1
    )
    WHERE explain LIKE '%Prewhere filter column%'
);

SELECT 'baseline_value common value ordered after selective_probe';
SELECT countIf(
    position(prewhere_line, '__table1.selective_probe') > 0
    AND position(prewhere_line, '__table1.baseline_value') > 0
    AND position(prewhere_line, '__table1.selective_probe') < position(prewhere_line, '__table1.baseline_value')) = 1 FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM
    (
        EXPLAIN actions = 1
        SELECT count() FROM t_countmin_lc_numeric_fixture
        WHERE baseline_value = 42 AND selective_probe = 1
    )
    WHERE explain LIKE '%Prewhere filter column%'
);

SELECT 'lc_value rare value ordered before selective_probe';
SELECT countIf(
    position(prewhere_line, '__table1.lc_value') > 0
    AND position(prewhere_line, '__table1.selective_probe') > 0
    AND position(prewhere_line, '__table1.lc_value') < position(prewhere_line, '__table1.selective_probe')) = 1 FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM
    (
        EXPLAIN actions = 1
        SELECT count() FROM t_countmin_lc_numeric_fixture
        WHERE lc_value = 777 AND selective_probe = 1
    )
    WHERE explain LIKE '%Prewhere filter column%'
);

DROP TABLE t_countmin_lc_numeric_fixture;

DROP TABLE IF EXISTS t_countmin_lc_nullable_string_fixture;

CREATE TABLE t_countmin_lc_nullable_string_fixture
(
    id UInt64,
    lc_value LowCardinality(Nullable(String)) STATISTICS(countmin),
    selective_probe UInt8 STATISTICS(countmin)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS auto_statistics_types = '', refresh_statistics_interval = 0;

INSERT INTO t_countmin_lc_nullable_string_fixture
SELECT
    number,
    multiIf(number < 600, CAST(NULL, 'Nullable(String)'), number < 610, '', 'apple'),
    toUInt8(number < 30)
FROM numbers(1000);

-- Sanity-check NULL, empty, common, and probe counts before testing CountMin estimates.
SELECT 'nullable string fixture distribution';
SELECT countIf(isNull(lc_value)), countIf(lc_value = ''), countIf(lc_value = 'apple'), countIf(selective_probe = 1) FROM t_countmin_lc_nullable_string_fixture;

SELECT 'lc_value empty string is not inflated by NULLs';
SELECT countIf(
    position(prewhere_line, '__table1.lc_value') > 0
    AND position(prewhere_line, '__table1.selective_probe') > 0
    AND position(prewhere_line, '__table1.lc_value') < position(prewhere_line, '__table1.selective_probe')) = 1 FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM
    (
        EXPLAIN actions = 1
        SELECT count() FROM t_countmin_lc_nullable_string_fixture
        WHERE lc_value = '' AND selective_probe = 1
    )
    WHERE explain LIKE '%Prewhere filter column%'
);

SELECT 'lc_value common string ordered after selective_probe';
SELECT countIf(
    position(prewhere_line, '__table1.selective_probe') > 0
    AND position(prewhere_line, '__table1.lc_value') > 0
    AND position(prewhere_line, '__table1.selective_probe') < position(prewhere_line, '__table1.lc_value')) = 1 FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM
    (
        EXPLAIN actions = 1
        SELECT count() FROM t_countmin_lc_nullable_string_fixture
        WHERE lc_value = 'apple' AND selective_probe = 1
    )
    WHERE explain LIKE '%Prewhere filter column%'
);

SELECT 'lc_value missing string ordered before selective_probe';
SELECT countIf(
    position(prewhere_line, '__table1.lc_value') > 0
    AND position(prewhere_line, '__table1.selective_probe') > 0
    AND position(prewhere_line, '__table1.lc_value') < position(prewhere_line, '__table1.selective_probe')) = 1 FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM
    (
        EXPLAIN actions = 1
        SELECT count() FROM t_countmin_lc_nullable_string_fixture
        WHERE lc_value = 'missing' AND selective_probe = 1
    )
    WHERE explain LIKE '%Prewhere filter column%'
);

DROP TABLE t_countmin_lc_nullable_string_fixture;

DROP TABLE IF EXISTS t_countmin_lc_large_dictionary;
DROP TABLE IF EXISTS t_countmin_lc_large_dictionary_subset;

-- `t_countmin_lc_large_dictionary_subset` is a filtered subset of `t_countmin_lc_large_dictionary`.
-- It exercises the CountMin LowCardinality path where dictionary size is larger
-- than the number of rows, so only touched dictionary indexes must be counted.
CREATE TABLE t_countmin_lc_large_dictionary
(
    id UInt64,
    lc_value LowCardinality(UInt32),
    selective_probe UInt8,
    payload String
)
ENGINE = Memory;

CREATE TABLE t_countmin_lc_large_dictionary_subset
(
    id UInt64,
    lc_value LowCardinality(UInt32) STATISTICS(countmin),
    selective_probe UInt8 STATISTICS(countmin),
    payload String STATISTICS(countmin)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS auto_statistics_types = '', refresh_statistics_interval = 0;

INSERT INTO t_countmin_lc_large_dictionary
SELECT
    number,
    CAST(multiIf(number < 200, number, number IN (200, 201, 203), 42, number IN (202, 204), 7, 199), 'LowCardinality(UInt32)'),
    toUInt8(number IN (200, 201)),
    repeat('x', 10000)
FROM numbers(206);

INSERT INTO t_countmin_lc_large_dictionary_subset
SELECT id - 200, lc_value, selective_probe, payload
FROM t_countmin_lc_large_dictionary
WHERE id >= 200;

-- Sanity-check the filtered block used to exercise touched LowCardinality dictionary indexes.
SELECT 'sliced dictionary fixture distribution';
SELECT count(), countIf(lc_value = 42), countIf(lc_value = 7), countIf(lc_value = 199), countIf(selective_probe = 1) FROM t_countmin_lc_large_dictionary_subset;

SELECT 'sliced lc_value common touched value uses row frequency';
SELECT countIf(
    position(prewhere_line, '__table1.selective_probe') > 0
    AND position(prewhere_line, '__table1.lc_value') > 0
    AND position(prewhere_line, '__table1.selective_probe') < position(prewhere_line, '__table1.lc_value')) = 1 FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM
    (
        EXPLAIN actions = 1
        SELECT count() FROM t_countmin_lc_large_dictionary_subset
        WHERE lc_value = 42 AND selective_probe = 1
    )
    WHERE explain LIKE '%Prewhere filter column%'
);

SELECT 'sliced lc_value unused dictionary value is not counted';
SELECT countIf(
    position(prewhere_line, '__table1.lc_value') > 0
    AND position(prewhere_line, '__table1.payload') > 0
    AND position(prewhere_line, '__table1.lc_value') < position(prewhere_line, '__table1.payload')) = 1 FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM
    (
        EXPLAIN actions = 1
        SELECT count() FROM t_countmin_lc_large_dictionary_subset
        WHERE lc_value = 5 AND payload = 'absent'
    )
    WHERE explain LIKE '%Prewhere filter column%'
);

DROP TABLE t_countmin_lc_large_dictionary_subset;
DROP TABLE t_countmin_lc_large_dictionary;
