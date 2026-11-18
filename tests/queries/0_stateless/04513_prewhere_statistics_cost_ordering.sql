-- Regression test: when statistics are enabled, the PREWHERE optimizer must
-- combine selectivity (estimated_row_count) with I/O cost (columns_size)
-- using the classic cost / (1 - selectivity) rule, not sort by selectivity alone.
--
-- Without this fix, the auto-collected Uniq statistic on `modality` (2 distinct
-- values → 50% selectivity) makes the optimizer place the expensive Map-key
-- predicate first because its default 1% selectivity looks "more selective",
-- ignoring that the Map column is ~500x more expensive to read.

SET optimize_functions_to_subcolumns = 0;
SET optimize_move_to_prewhere = 1;
SET allow_reorder_prewhere_conditions = 1;
SET use_statistics = 1;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_prewhere_stats_cost;
CREATE TABLE t_prewhere_stats_cost (id UInt64, modality LowCardinality(String), h Map(String, String))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, default_compression_codec = 'LZ4';

-- Split `modality` 50/50: the reject-count ratio is then ~2x while the Map column dwarfs the
-- scalar, so the cost gap dominates and cheap-filter-first stays stable under randomized serialization.
INSERT INTO t_prewhere_stats_cost
SELECT number, if(number % 2 = 0, 'active', ''), map('k', repeat('v', 300), 'k2', repeat('w', 300))
FROM numbers(200000);
OPTIMIZE TABLE t_prewhere_stats_cost FINAL;

SELECT '-- with statistics: cheap filter is placed before the expensive Map predicate';
SELECT position(explain, 'modality') > 0 AND position(explain, 'modality') < position(explain, 'arrayElement') AS cheap_first
FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_prewhere_stats_cost WHERE modality = '' AND h['k'] = 'nope'
) WHERE explain LIKE '%Prewhere filter column%';

SELECT '-- correctness: result is the same regardless of ordering';
SELECT count() FROM t_prewhere_stats_cost WHERE modality = '' AND h['k'] = 'nope';
SELECT count() FROM t_prewhere_stats_cost WHERE modality = '' AND h['k'] = 'nope'
SETTINGS allow_reorder_prewhere_conditions = 0;

DROP TABLE t_prewhere_stats_cost;

-- The cost of a condition is the decoded size of its columns plus the bytes its functions produce.
SET move_all_conditions_to_prewhere = 1;

DROP TABLE IF EXISTS t_prewhere_cost_decoded;
CREATE TABLE t_prewhere_cost_decoded (a UInt32 CODEC(NONE), s String CODEC(ZSTD(1)))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1, auto_statistics_types = 'basic, uniq_v2';
INSERT INTO t_prewhere_cost_decoded
SELECT toUInt32(cityHash64(number)), concat('k', toString(intDiv(number, 50000)), '-', repeat('ab-cd-', 5)) FROM numbers(200000);
OPTIMIZE TABLE t_prewhere_cost_decoded FINAL;

SELECT '-- a well-compressed wide string is not cheaper than a selective fixed-size column';
SELECT position(explain, 'less(') > 0 AND position(explain, 'less(') < position(explain, 'endsWith(')
FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_prewhere_cost_decoded WHERE a < 4294967 AND endsWith(s, 'zz')
    SETTINGS min_marks_per_part_to_load_statistics = 0
) WHERE explain LIKE '%Prewhere filter column%';

SELECT '-- the same without statistics';
SELECT position(explain, 'less(') > 0 AND position(explain, 'less(') < position(explain, 'endsWith(')
FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_prewhere_cost_decoded WHERE a < 4294967 AND endsWith(s, 'zz')
    SETTINGS min_marks_per_part_to_load_statistics = 0, use_statistics = 0
) WHERE explain LIKE '%Prewhere filter column%';

SELECT count() FROM t_prewhere_cost_decoded WHERE a < 4294967 AND endsWith(s, 'zz');
SELECT count() FROM t_prewhere_cost_decoded WHERE a < 4294967 AND endsWith(s, 'zz') SETTINGS allow_reorder_prewhere_conditions = 0;

DROP TABLE t_prewhere_cost_decoded;

DROP TABLE IF EXISTS t_prewhere_cost_functions;
CREATE TABLE t_prewhere_cost_functions (n UInt8 CODEC(NONE), w UInt32 CODEC(NONE))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1, auto_statistics_types = 'basic, uniq_v2';
INSERT INTO t_prewhere_cost_functions SELECT toUInt8(cityHash64(number, 1)), toUInt32(cityHash64(number, 2)) FROM numbers(200000);
OPTIMIZE TABLE t_prewhere_cost_functions FINAL;

SELECT '-- a conversion chain over a narrow column is charged for the strings it produces';
SELECT position(explain, 'less(') > 0 AND position(explain, 'less(') < position(explain, 'toUInt64OrZero(')
FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_prewhere_cost_functions WHERE toUInt64OrZero(toString(n)) = 1000 AND w < 50000000
    SETTINGS min_marks_per_part_to_load_statistics = 0
) WHERE explain LIKE '%Prewhere filter column%';

SELECT count() FROM t_prewhere_cost_functions WHERE toUInt64OrZero(toString(n)) = 1000 AND w < 50000000;
SELECT count() FROM t_prewhere_cost_functions WHERE toUInt64OrZero(toString(n)) = 1000 AND w < 50000000
SETTINGS allow_reorder_prewhere_conditions = 0;

DROP TABLE t_prewhere_cost_functions;

DROP TABLE IF EXISTS t_prewhere_cost_json;
CREATE TABLE t_prewhere_cost_json (lc LowCardinality(String) CODEC(NONE), j JSON CODEC(ZSTD(1)))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1, auto_statistics_types = 'basic, uniq_v2';
INSERT INTO t_prewhere_cost_json
SELECT
    concat('v', toString(cityHash64(number, 3) % 30)),
    if(intDiv(number, 2000) % 100 = 0,
        format('{{"k0":{},"k1":{}}}', toString(cityHash64(number, 4) % 5000000), toString(number % 1000)),
        format('{{"k1":{}}}', toString(number % 1000)))::JSON
FROM numbers(200000);
OPTIMIZE TABLE t_prewhere_cost_json FINAL;

SELECT '-- a sparse JSON path behind a cast runs after a selective LowCardinality filter';
SELECT position(explain, 'in(') > 0 AND position(explain, 'in(') < position(explain, 'toUInt64OrZero(')
FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_prewhere_cost_json WHERE toUInt64OrZero(toString(j.k0)) = 1234567 AND lc IN ('v0', 'v1')
    SETTINGS min_marks_per_part_to_load_statistics = 0, allow_calculating_subcolumns_sizes_for_merge_tree_reading = 1
) WHERE explain LIKE '%Prewhere filter column%';

SELECT count() FROM t_prewhere_cost_json WHERE toUInt64OrZero(toString(j.k0)) = 1234567 AND lc IN ('v0', 'v1');
SELECT count() FROM t_prewhere_cost_json WHERE toUInt64OrZero(toString(j.k0)) = 1234567 AND lc IN ('v0', 'v1')
SETTINGS allow_reorder_prewhere_conditions = 0;

DROP TABLE t_prewhere_cost_json;

DROP TABLE IF EXISTS t_prewhere_cost_lc;
CREATE TABLE t_prewhere_cost_lc (lc LowCardinality(String) CODEC(NONE), u UInt64 CODEC(NONE))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1, auto_statistics_types = 'basic, uniq_v2';
INSERT INTO t_prewhere_cost_lc SELECT concat('v', toString(cityHash64(number, 5) % 10)), cityHash64(number, 6) % 1000000 FROM numbers(200000);
OPTIMIZE TABLE t_prewhere_cost_lc FINAL;

SELECT '-- a function over LowCardinality runs on the dictionary, so it stays cheap';
SELECT position(explain, 'lower(') > 0 AND position(explain, 'lower(') < position(explain, 'u, 5')
FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_prewhere_cost_lc WHERE lower(lc) = 'v1' AND u = 5
    SETTINGS min_marks_per_part_to_load_statistics = 0
) WHERE explain LIKE '%Prewhere filter column%';

SELECT count() FROM t_prewhere_cost_lc WHERE lower(lc) = 'v1' AND u = 5;
SELECT count() FROM t_prewhere_cost_lc WHERE lower(lc) = 'v1' AND u = 5 SETTINGS allow_reorder_prewhere_conditions = 0;

DROP TABLE t_prewhere_cost_lc;

DROP TABLE IF EXISTS t_prewhere_cost_lc_cast;
CREATE TABLE t_prewhere_cost_lc_cast (s String CODEC(NONE), w UInt32 CODEC(NONE))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1, auto_statistics_types = 'basic, uniq_v2', string_serialization_version = 'single_stream';
INSERT INTO t_prewhere_cost_lc_cast SELECT concat('k', toString(cityHash64(number, 7) % 10)), toUInt32(cityHash64(number, 8)) FROM numbers(200000);
OPTIMIZE TABLE t_prewhere_cost_lc_cast FINAL;

SELECT '-- a conversion of a full column to LowCardinality is charged for the values it hashes';
SELECT position(explain, 'less(') > 0 AND position(explain, 'less(') < position(explain, 'toLowCardinality(')
FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_prewhere_cost_lc_cast WHERE toLowCardinality(s) = 'k1' AND w < 3000000000
    SETTINGS min_marks_per_part_to_load_statistics = 0
) WHERE explain LIKE '%Prewhere filter column%';

SELECT count() FROM t_prewhere_cost_lc_cast WHERE toLowCardinality(s) = 'k1' AND w < 3000000000;
SELECT count() FROM t_prewhere_cost_lc_cast WHERE toLowCardinality(s) = 'k1' AND w < 3000000000 SETTINGS allow_reorder_prewhere_conditions = 0;

DROP TABLE t_prewhere_cost_lc_cast;

DROP TABLE IF EXISTS t_prewhere_cost_join;
DROP TABLE IF EXISTS t_prewhere_cost_join_attr;
CREATE TABLE t_prewhere_cost_join_attr (k LowCardinality(String), v LowCardinality(String)) ENGINE = Join(ANY, LEFT, k) SETTINGS join_use_nulls = 0;
INSERT INTO t_prewhere_cost_join_attr SELECT concat('v', toString(number)), concat('w', toString(number)) FROM numbers(30);
CREATE TABLE t_prewhere_cost_join (lc LowCardinality(String) CODEC(NONE), w UInt32 CODEC(NONE))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1, auto_statistics_types = 'basic, uniq_v2';
INSERT INTO t_prewhere_cost_join SELECT concat('v', toString(cityHash64(number, 9) % 30)), toUInt32(cityHash64(number, 10)) FROM numbers(200000);
OPTIMIZE TABLE t_prewhere_cost_join FINAL;

SELECT '-- a function that does not run on the dictionary is charged for its values';
SELECT position(explain, 'less(') > 0 AND position(explain, 'less(') < position(explain, 'joinGet(')
FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_prewhere_cost_join WHERE joinGet('t_prewhere_cost_join_attr', 'v', lc) = 'w1' AND w < 3000000000
    SETTINGS min_marks_per_part_to_load_statistics = 0
) WHERE explain LIKE '%Prewhere filter column%';

SELECT count() FROM t_prewhere_cost_join WHERE joinGet('t_prewhere_cost_join_attr', 'v', lc) = 'w1' AND w < 3000000000;
SELECT count() FROM t_prewhere_cost_join WHERE joinGet('t_prewhere_cost_join_attr', 'v', lc) = 'w1' AND w < 3000000000 SETTINGS allow_reorder_prewhere_conditions = 0;

DROP TABLE t_prewhere_cost_join;
DROP TABLE t_prewhere_cost_join_attr;
