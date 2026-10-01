-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: the EXPLAIN checks assert on the local read step.

-- With `use_top_k_dynamic_filtering`, an `ORDER BY ... LIMIT` read adds its threshold filter to the PREWHERE as the
-- first condition. The filter is not added when a condition depends on the block it runs on, when a stored column
-- has the filter's name, or when the read is in the order of the sort column. The rows are the same either way.

SET explain_query_plan_default = 'legacy'; -- prints the PREWHERE conditions in the order they run
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET use_top_k_dynamic_filtering = 1;
SET use_skip_indexes_for_top_k = 0;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET enable_multiple_prewhere_read_steps = 1;
SET optimize_use_projections = 1;
SET optimize_read_in_order = 1;

DROP TABLE IF EXISTS t_topk_prewhere;
CREATE TABLE t_topk_prewhere (k UInt32, pred UInt32, tag String)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
INSERT INTO t_topk_prewhere SELECT number, number % 10, concat('t', toString(number % 7)) FROM numbers(50000);

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT k FROM t_topk_prewhere PREWHERE pred = 3 AND tag = 't2' ORDER BY k LIMIT 10)
WHERE explain ILIKE '%Prewhere filter column: and(\_\_topKFilter(k), equals(pred, 3%tag%';
SELECT groupArray(k) FROM (SELECT k FROM t_topk_prewhere WHERE pred = 3 AND tag = 't2' ORDER BY k LIMIT 20)
SETTINGS use_top_k_dynamic_filtering = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_topk_prewhere WHERE pred = 3 AND tag = 't2' ORDER BY k LIMIT 20)
SETTINGS use_top_k_dynamic_filtering = 1;

-- A stateful or per-block condition keeps the filter out; a per-query one does not.
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT k FROM t_topk_prewhere PREWHERE rowNumberInBlock() < 1 ORDER BY k LIMIT 10)
WHERE explain ILIKE '%FUNCTION \_\_topKFilter%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT k FROM t_topk_prewhere PREWHERE blockSize() > 100 ORDER BY k LIMIT 10)
WHERE explain ILIKE '%FUNCTION \_\_topKFilter%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT k FROM t_topk_prewhere PREWHERE currentUser() != '' ORDER BY k LIMIT 10)
WHERE explain ILIKE '%FUNCTION \_\_topKFilter%';

DROP TABLE t_topk_prewhere;

-- The rows of a per-block condition depend on the randomized block size, so the two arms are compared: in
-- the PREWHERE, in a WHERE left above the read, and in the sort key.
DROP TABLE IF EXISTS t_topk_blocksize;
CREATE TABLE t_topk_blocksize (k UInt32, grp UInt32)
ENGINE = MergeTree ORDER BY intHash32(k) SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
INSERT INTO t_topk_blocksize SELECT number, number % 100 FROM numbers(100000);

SELECT
    (SELECT groupArray(k) FROM (SELECT k FROM t_topk_blocksize PREWHERE blockSize() > 100 ORDER BY k LIMIT 20) SETTINGS use_top_k_dynamic_filtering = 0)
  = (SELECT groupArray(k) FROM (SELECT k FROM t_topk_blocksize PREWHERE blockSize() > 100 ORDER BY k LIMIT 20) SETTINGS use_top_k_dynamic_filtering = 1);
SELECT
    (SELECT groupArray(k) FROM (SELECT k FROM t_topk_blocksize PREWHERE k % 10 < 9 WHERE blockSize() > 100 ORDER BY k LIMIT 20) SETTINGS use_top_k_dynamic_filtering = 0)
  = (SELECT groupArray(k) FROM (SELECT k FROM t_topk_blocksize PREWHERE k % 10 < 9 WHERE blockSize() > 100 ORDER BY k LIMIT 20) SETTINGS use_top_k_dynamic_filtering = 1);
SELECT
    (SELECT groupArray(k) FROM (SELECT k FROM t_topk_blocksize PREWHERE k % 10 < 9 ORDER BY grp, rowNumberInBlock() LIMIT 20) SETTINGS use_top_k_dynamic_filtering = 0)
  = (SELECT groupArray(k) FROM (SELECT k FROM t_topk_blocksize PREWHERE k % 10 < 9 ORDER BY grp, rowNumberInBlock() LIMIT 20) SETTINGS use_top_k_dynamic_filtering = 1);

DROP TABLE t_topk_blocksize;

-- A stored column named like the filter, first in the PREWHERE, then in the output of a read without one.
DROP TABLE IF EXISTS t_topk_collide;
CREATE TABLE t_topk_collide (k UInt32, pred UInt32, `__topKFilter(k)` UInt8)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
INSERT INTO t_topk_collide SELECT number, number % 10, 0 FROM numbers(50000);

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT k FROM t_topk_collide WHERE pred = 3 AND `__topKFilter(k)` = 0 ORDER BY k LIMIT 5)
WHERE explain ILIKE '%Prewhere filter column: %equals(%\_\_topKFilter(k)%';
SELECT groupArray(k) FROM (SELECT k FROM t_topk_collide WHERE pred = 3 AND `__topKFilter(k)` = 0 ORDER BY k LIMIT 5)
SETTINGS use_top_k_dynamic_filtering = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_topk_collide WHERE pred = 3 AND `__topKFilter(k)` = 0 ORDER BY k LIMIT 5)
SETTINGS use_top_k_dynamic_filtering = 1;

DROP TABLE t_topk_collide;

DROP TABLE IF EXISTS t_topk_collide_no_prewhere;
DROP TABLE IF EXISTS t_topk_no_prewhere;
CREATE TABLE t_topk_collide_no_prewhere (k UInt32, `__topKFilter(k)` UInt8)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
CREATE TABLE t_topk_no_prewhere (k UInt32, other UInt8)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
INSERT INTO t_topk_collide_no_prewhere SELECT number, number * 37 % 251 FROM numbers(50000);
INSERT INTO t_topk_no_prewhere SELECT number, number * 37 % 251 FROM numbers(50000);

SELECT groupArray((k, x)) FROM (SELECT k, `__topKFilter(k)` AS x FROM t_topk_collide_no_prewhere ORDER BY k LIMIT 5)
SETTINGS query_plan_optimize_lazy_materialization = 0, use_top_k_dynamic_filtering = 0;
SELECT groupArray((k, x)) FROM (SELECT k, `__topKFilter(k)` AS x FROM t_topk_collide_no_prewhere ORDER BY k LIMIT 5)
SETTINGS query_plan_optimize_lazy_materialization = 0, use_top_k_dynamic_filtering = 1;
SELECT groupArray((k, x)) FROM (SELECT k, `__topKFilter(k)` AS x FROM t_topk_collide_no_prewhere ORDER BY k LIMIT 5)
SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10000,
    use_top_k_dynamic_filtering = 1;

-- The same read of a column with another name does get the filter.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT k, other FROM t_topk_no_prewhere ORDER BY k LIMIT 5 SETTINGS query_plan_optimize_lazy_materialization = 0)
WHERE explain ILIKE '%Prewhere filter column: \_\_topKFilter(k)%';

DROP TABLE t_topk_collide_no_prewhere;
DROP TABLE t_topk_no_prewhere;

-- A read served by a normal projection gets the filter as the first condition of its PREWHERE.
DROP TABLE IF EXISTS t_topk_proj;
CREATE TABLE t_topk_proj (id UInt64, a UInt64, s UInt64, PROJECTION p (SELECT id, a, s ORDER BY a))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
INSERT INTO t_topk_proj SELECT number, cityHash64(number) % 200000, 199999 - number FROM numbers(200000);
OPTIMIZE TABLE t_topk_proj FINAL;

SELECT count() > 0 FROM (EXPLAIN projections = 1 SELECT id, s FROM t_topk_proj WHERE a < 2000 ORDER BY s ASC LIMIT 10)
WHERE explain ILIKE '%ReadFromMergeTree (p)%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id, s FROM t_topk_proj WHERE a < 2000 ORDER BY s ASC LIMIT 10)
WHERE explain ILIKE '%Prewhere filter column: and(\_\_topKFilter(s), \_projection\_filter)%';
SELECT groupArray(id) FROM (SELECT id, s FROM t_topk_proj WHERE a < 2000 ORDER BY s ASC LIMIT 20)
SETTINGS use_top_k_dynamic_filtering = 0;
SELECT groupArray(id) FROM (SELECT id, s FROM t_topk_proj WHERE a < 2000 ORDER BY s ASC LIMIT 20)
SETTINGS use_top_k_dynamic_filtering = 1;

-- A projection sorted by the sort column is read in that order and gets no filter.
DROP TABLE IF EXISTS t_topk_proj_sorted;
CREATE TABLE t_topk_proj_sorted (id UInt64, a UInt64, s UInt64, PROJECTION ps (SELECT id, a, s ORDER BY s))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
INSERT INTO t_topk_proj_sorted SELECT number, cityHash64(number) % 200000, intHash64(number) % 100000 FROM numbers(200000);
OPTIMIZE TABLE t_topk_proj_sorted FINAL;

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id, s FROM t_topk_proj_sorted PREWHERE a < 100000 ORDER BY s ASC, id ASC LIMIT 10)
WHERE explain ILIKE '%ReadType: InOrder%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id, s FROM t_topk_proj_sorted PREWHERE a < 100000 ORDER BY s ASC, id ASC LIMIT 10)
WHERE explain ILIKE '%__topKFilter%';
-- Without the projection the read is not in that order and gets the filter.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id, s FROM t_topk_proj_sorted PREWHERE a < 100000 ORDER BY s ASC, id ASC LIMIT 10 SETTINGS optimize_use_projections = 0)
WHERE explain ILIKE '%__topKFilter%';
SELECT groupArray((id, s)) FROM (SELECT id, s FROM t_topk_proj_sorted PREWHERE a < 100000 ORDER BY s ASC, id ASC LIMIT 20)
SETTINGS use_top_k_dynamic_filtering = 0;
SELECT groupArray((id, s)) FROM (SELECT id, s FROM t_topk_proj_sorted PREWHERE a < 100000 ORDER BY s ASC, id ASC LIMIT 20)
SETTINGS use_top_k_dynamic_filtering = 1;

DROP TABLE t_topk_proj_sorted;

-- A projection in only some parts leaves two reads under a union, and both get the filter.
DROP TABLE IF EXISTS t_topk_proj_partial;
CREATE TABLE t_topk_proj_partial (id UInt64, a UInt64, s UInt64)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_topk_proj_partial;
INSERT INTO t_topk_proj_partial SELECT number, cityHash64(number) % 200000, 199999 - number FROM numbers(100000);
ALTER TABLE t_topk_proj_partial ADD PROJECTION p (SELECT id, a, s ORDER BY a);
INSERT INTO t_topk_proj_partial SELECT number, cityHash64(number) % 200000, 199999 - number FROM numbers(100000, 100000);

SELECT count() = 2 FROM (EXPLAIN actions = 1 SELECT id, s FROM t_topk_proj_partial WHERE a < 2000 ORDER BY s ASC LIMIT 10)
WHERE explain ILIKE '%Prewhere filter column: and(\_\_topKFilter(s),%';
SELECT groupArray(id) FROM (SELECT id, s FROM t_topk_proj_partial WHERE a < 2000 ORDER BY s ASC LIMIT 20)
SETTINGS use_top_k_dynamic_filtering = 0;
SELECT groupArray(id) FROM (SELECT id, s FROM t_topk_proj_partial WHERE a < 2000 ORDER BY s ASC LIMIT 20)
SETTINGS use_top_k_dynamic_filtering = 1;

DROP TABLE t_topk_proj_partial;
DROP TABLE t_topk_proj;
