-- Plan-level integration of `join_algorithm = 'partitioned_hash'` with the optimizations that
-- previously enumerated only `hash` / `parallel_hash`. JOIN runtime filters are built for it
-- (`supportsRuntimeFilter`). RIGHT/FULL non-joined rows are emitted by the parallel
-- `NonJoinedBlocksTransform` sources instead of by the one `JoiningTransform` that finishes last.
-- `query_plan_convert_join_to_in` rewrites its qualifying joins. Each case also checks result
-- parity against `hash` and `parallel_hash`.

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
-- The runner randomizes `max_bytes_before_external_join`; any non-zero spill budget would wrap the join
-- in `SpillingHashJoin`.
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET max_bytes_in_join = 0;

DROP TABLE IF EXISTS t_phj_plan_build;
DROP TABLE IF EXISTS t_phj_plan_probe;

CREATE TABLE t_phj_plan_build (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_phj_plan_probe (k UInt64, p UInt64) ENGINE = MergeTree ORDER BY k;

-- The key ranges overlap on a quarter of each side. A runtime filter then has most probe rows to
-- prune. The RIGHT/FULL output has plenty of unmatched build rows to emit.
INSERT INTO t_phj_plan_build SELECT number, number * 2 FROM numbers(200000);
INSERT INTO t_phj_plan_probe SELECT number + 150000, number FROM numbers(200000);

SELECT '-- a runtime filter is planted for partitioned_hash';
SELECT count() > 0 FROM (
    EXPLAIN SELECT count() FROM t_phj_plan_probe AS p INNER JOIN t_phj_plan_build AS b ON p.k = b.k
    SETTINGS join_algorithm = 'partitioned_hash', enable_join_runtime_filters = 1
) WHERE explain LIKE '%BuildRuntimeFilter%';

SELECT '-- the partitioned algorithm survives the runtime-filter algorithm pruning';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_phj_plan_probe AS p INNER JOIN t_phj_plan_build AS b ON p.k = b.k
    SETTINGS join_algorithm = 'partitioned_hash', enable_join_runtime_filters = 1
) WHERE explain LIKE '%Algorithm: PartitionedHashJoin%';

SELECT '-- listing both algorithms keeps partitioned_hash when a filter is planted';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_phj_plan_probe AS p INNER JOIN t_phj_plan_build AS b ON p.k = b.k
    SETTINGS join_algorithm = 'partitioned_hash,parallel_hash', enable_join_runtime_filters = 1
) WHERE explain LIKE '%Algorithm: PartitionedHashJoin%';

SELECT 'inner results are unchanged by the runtime filter', h.1, h = pa, h = pf FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_plan_probe AS p INNER JOIN t_phj_plan_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', enable_join_runtime_filters = 0) AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_plan_probe AS p INNER JOIN t_phj_plan_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', enable_join_runtime_filters = 0) AS pa,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_plan_probe AS p INNER JOIN t_phj_plan_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', enable_join_runtime_filters = 1) AS pf);

SELECT '-- non-joined rows come from parallel sources under partitioned_hash';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT count() FROM t_phj_plan_probe AS p RIGHT JOIN t_phj_plan_build AS b ON p.k = b.k
    SETTINGS join_algorithm = 'partitioned_hash', max_threads = 4
) WHERE explain LIKE '%NonJoinedBlocks%';

SELECT 'right parity across algorithms and stream counts', h.1, h = ph, h = pa1, h = pa4, h = pa16 FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_plan_probe AS p RIGHT JOIN t_phj_plan_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_plan_probe AS p RIGHT JOIN t_phj_plan_build AS b ON p.k = b.k SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_plan_probe AS p RIGHT JOIN t_phj_plan_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_threads = 1) AS pa1,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_plan_probe AS p RIGHT JOIN t_phj_plan_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_threads = 4) AS pa4,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_plan_probe AS p RIGHT JOIN t_phj_plan_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_threads = 16) AS pa16);

SELECT 'full parity across algorithms and stream counts', h.1, h = ph, h = pa1, h = pa16 FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_plan_probe AS p FULL JOIN t_phj_plan_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_plan_probe AS p FULL JOIN t_phj_plan_build AS b ON p.k = b.k SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_plan_probe AS p FULL JOIN t_phj_plan_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_threads = 1) AS pa1,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_plan_probe AS p FULL JOIN t_phj_plan_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_threads = 16) AS pa16);

SELECT 'right parity with join_use_nulls', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_plan_probe AS p RIGHT JOIN t_phj_plan_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', join_use_nulls = 1) AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_plan_probe AS p RIGHT JOIN t_phj_plan_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', join_use_nulls = 1, max_threads = 16) AS pa);

SELECT 'a one-partition build still emits every non-joined row', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_plan_probe AS p RIGHT JOIN (SELECT * FROM t_phj_plan_build ORDER BY k LIMIT 8) AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_plan_probe AS p RIGHT JOIN (SELECT * FROM t_phj_plan_build ORDER BY k LIMIT 8) AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_threads = 16) AS pa);

SELECT '-- convert_join_to_in rewrites a qualifying partitioned_hash join';
SELECT count() FROM (
    EXPLAIN SELECT p.k FROM t_phj_plan_probe AS p INNER JOIN (SELECT k FROM t_phj_plan_build) AS b ON p.k = b.k
    SETTINGS join_algorithm = 'partitioned_hash', query_plan_convert_join_to_in = 1
) WHERE explain LIKE '%Join%';

SELECT 'the rewritten query gives the same rows as the join', j.1, j = i FROM (SELECT
    (SELECT (count(), sum(p.k)) FROM t_phj_plan_probe AS p INNER JOIN (SELECT k FROM t_phj_plan_build) AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', query_plan_convert_join_to_in = 0) AS j,
    (SELECT (count(), sum(p.k)) FROM t_phj_plan_probe AS p INNER JOIN (SELECT k FROM t_phj_plan_build) AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', query_plan_convert_join_to_in = 1) AS i);

SELECT '-- with automatic external join the partitioned algorithm stays selected';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_phj_plan_probe AS p INNER JOIN t_phj_plan_build AS b ON p.k = b.k
    SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 1000000, max_bytes_ratio_before_external_join = 0
) WHERE explain LIKE '%Algorithm: SpillingHashJoin(PartitionedHashJoin)%';

SELECT '-- listing both algorithms keeps partitioned_hash when spilling is configured';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_phj_plan_probe AS p INNER JOIN t_phj_plan_build AS b ON p.k = b.k
    SETTINGS join_algorithm = 'partitioned_hash,parallel_hash', max_bytes_before_external_join = 1000000, max_bytes_ratio_before_external_join = 0
) WHERE explain LIKE '%SpillingHashJoin(PartitionedHashJoin)%';

DROP TABLE t_phj_plan_probe;
DROP TABLE t_phj_plan_build;
