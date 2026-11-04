-- Tags: no-fasttest, no-parallel-replicas, no-replicated-database
-- no-fasttest: the JSON cases need the JSON type.
-- no-parallel-replicas: EXPLAIN indexes = 1 gains a per-node Granules block, and
-- use_skip_indexes_on_data_read is not supported with parallel replicas.
-- no-replicated-database: hypothetical indexes are session-scoped and not replicated

-- Auto statistics can drop a whole part before any skip index is read, which would make the
-- assertions below measure something other than the skip index.
SET use_statistics_for_part_pruning = 0;

-- Every table pins index_granularity, index_granularity_bytes and min_bytes_for_wide_part so the
-- granule counts asserted through EXPLAIN are stable under merge-tree settings randomization, and
-- orders by tuple() so the primary key never prunes and the only Granules: 1/16 line is the index's.

SELECT '-- 1. Dynamic column, negated atom on the granule path';
DROP TABLE IF EXISTS t_dyn;
CREATE TABLE t_dyn (k UInt64, d Dynamic, INDEX idx d TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_dyn SELECT number, number FROM numbers(64);

SELECT 'no index      ', count() FROM t_dyn WHERE d != 1048577 SETTINGS use_skip_indexes = 0;
SELECT 'bulk          ', count() FROM t_dyn WHERE d != 1048577 SETTINGS secondary_indices_enable_bulk_filtering = 1;
SELECT 'granule       ', count() FROM t_dyn WHERE d != 1048577 SETTINGS secondary_indices_enable_bulk_filtering = 0;
SELECT 'granules 16/16', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dyn WHERE d != 1048577
    SETTINGS secondary_indices_enable_bulk_filtering = 0) WHERE explain LIKE '%Granules: 16/16%';

SELECT '-- 2. Dynamic column, IS NULL on the granule path';
DROP TABLE IF EXISTS t_dyn_null;
CREATE TABLE t_dyn_null (k UInt64, d Dynamic, INDEX idx d TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_dyn_null SELECT number, if(number % 2 = 0, NULL, number) FROM numbers(64);

SELECT 'no index      ', count() FROM t_dyn_null WHERE d IS NULL SETTINGS use_skip_indexes = 0;
SELECT 'granule       ', count() FROM t_dyn_null WHERE d IS NULL SETTINGS secondary_indices_enable_bulk_filtering = 0;

SELECT '-- 3. Dynamic column still prunes for a positive atom';
SELECT 'no index      ', count() FROM t_dyn WHERE d = 3 SETTINGS use_skip_indexes = 0;
SELECT 'granule       ', count() FROM t_dyn WHERE d = 3 SETTINGS secondary_indices_enable_bulk_filtering = 0;
SELECT 'granules 1/16 ', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dyn WHERE d = 3
    SETTINGS secondary_indices_enable_bulk_filtering = 0) WHERE explain LIKE '%Granules: 1/16%';

SELECT '-- 4. JSON subcolumn, the shape the AST fuzzer found';
DROP TABLE IF EXISTS t_json;
CREATE TABLE t_json (k UInt64, j JSON, INDEX idx j.a TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_json SELECT number, concat('{"a":', toString(number), '}') FROM numbers(64);

SELECT 'no index      ', count() FROM t_json WHERE j.a != 1048577 SETTINGS use_skip_indexes = 0;
SELECT 'granule       ', count() FROM t_json WHERE j.a != 1048577 SETTINGS secondary_indices_enable_bulk_filtering = 0;
SELECT 'granules 16/16', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_json WHERE j.a != 1048577
    SETTINGS secondary_indices_enable_bulk_filtering = 0) WHERE explain LIKE '%Granules: 16/16%';

SELECT '-- 5. JSON subcolumn absent from every row, IS NULL';
DROP TABLE IF EXISTS t_json_absent;
CREATE TABLE t_json_absent (k UInt64, j JSON, INDEX idx j.a TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_json_absent SELECT number, '{"b":1}' FROM numbers(64);

SELECT 'no index      ', count() FROM t_json_absent WHERE j.a IS NULL SETTINGS use_skip_indexes = 0;
SELECT 'granule       ', count() FROM t_json_absent WHERE j.a IS NULL SETTINGS secondary_indices_enable_bulk_filtering = 0;

SELECT '-- 6. Variant column, IS NULL on the granule path';
DROP TABLE IF EXISTS t_var;
CREATE TABLE t_var (k UInt64, v Variant(UInt64, String), INDEX idx v TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_var SELECT number, if(number % 2 = 0, NULL, number) FROM numbers(64);

SELECT 'no index      ', count() FROM t_var WHERE v IS NULL SETTINGS use_skip_indexes = 0;
SELECT 'granule       ', count() FROM t_var WHERE v IS NULL SETTINGS secondary_indices_enable_bulk_filtering = 0;
SELECT 'granules 16/16', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_var WHERE v IS NULL
    SETTINGS secondary_indices_enable_bulk_filtering = 0) WHERE explain LIKE '%Granules: 16/16%';

SELECT '-- 7. Two useful indexes and a disjunction, which reaches the granule path at defaults';
DROP TABLE IF EXISTS t_or;
CREATE TABLE t_or (k UInt64, d Dynamic, s String,
                   INDEX i1 d TYPE set(100) GRANULARITY 1, INDEX i2 s TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_or SELECT number, number, toString(number) FROM numbers(64);

SELECT 'no index      ', count() FROM t_or WHERE d != 1048577 OR s = 'nope' SETTINGS use_skip_indexes = 0;
SELECT 'disjunctions  ', count() FROM t_or WHERE d != 1048577 OR s = 'nope'
    SETTINGS use_skip_indexes_for_disjunctions = 1, use_skip_indexes_on_data_read = 1,
             secondary_indices_enable_bulk_filtering = 1;

SELECT '-- 8. Composite index (d, k): the answer is right and k still prunes';
DROP TABLE IF EXISTS t_comp;
CREATE TABLE t_comp (k UInt64, d Dynamic, INDEX idx (d, k) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_comp SELECT number, number FROM numbers(64);

SELECT 'no index      ', count() FROM t_comp WHERE d != 1048577 SETTINGS use_skip_indexes = 0;
SELECT 'granule       ', count() FROM t_comp WHERE d != 1048577 SETTINGS secondary_indices_enable_bulk_filtering = 0;
SELECT 'granules 1/16 ', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_comp WHERE k = 3
    SETTINGS secondary_indices_enable_bulk_filtering = 0) WHERE explain LIKE '%Granules: 1/16%';

SELECT '-- 9. A measurable type keeps its granule range';
DROP TABLE IF EXISTS t_str;
CREATE TABLE t_str (k UInt64, s String, INDEX idx s TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_str SELECT number, toString(number) FROM numbers(64);

SELECT 'no index      ', count() FROM t_str WHERE s = '3' SETTINGS use_skip_indexes = 0;
SELECT 'granule       ', count() FROM t_str WHERE s = '3' SETTINGS secondary_indices_enable_bulk_filtering = 0;
SELECT 'granules 1/16 ', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_str WHERE s = '3'
    SETTINGS secondary_indices_enable_bulk_filtering = 0) WHERE explain LIKE '%Granules: 1/16%';

SELECT '-- 10. The build-path range: an empirical EXPLAIN WHATIF estimate';
DROP TABLE IF EXISTS t_dyn_hypo;
-- No materialized index, so the baseline is a full scan and the estimate is attributable to the
-- hypothetical index alone. The empirical estimator is the only consumer of an aggregator-built
-- granule, so this is the one place the build-path range is observable.
CREATE TABLE t_dyn_hypo (k UInt64, d Dynamic)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_dyn_hypo SELECT number, number FROM numbers(64);

CREATE HYPOTHETICAL INDEX idx_h ON t_dyn_hypo (d) TYPE set(100) GRANULARITY 1;

-- source: empirical is asserted because evaluateIndex falls back to statistical and then to
-- applicability_only, either of which would report a skip ratio this arm cannot interpret.
SELECT replaceRegexpAll(trim(explain), ' +', ' ') AS line
FROM (EXPLAIN WHATIF SELECT count() FROM t_dyn_hypo WHERE d != 1048577)
WHERE explain LIKE '%skip_ratio:%' OR explain LIKE '%source:%'
ORDER BY line;

DROP TABLE t_dyn;
DROP TABLE t_dyn_null;
DROP TABLE t_json;
DROP TABLE t_json_absent;
DROP TABLE t_var;
DROP TABLE t_or;
DROP TABLE t_comp;
DROP TABLE t_str;
DROP TABLE t_dyn_hypo;
