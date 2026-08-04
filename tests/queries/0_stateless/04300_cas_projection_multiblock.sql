-- Tags: no-fasttest
-- ^ cas is an object-storage metadata type; keep it off the minimal fasttest image.

-- A projection built across MULTIPLE temp projection blocks (spill-and-merge) must read its own staged
-- temp blocks back on a content-addressed disk (B59). MergeProjectionPartsTask only EXERCISES the
-- read-back path when it has >1 temp projection part to merge (selected_parts.size() > 1); with a single
-- temp part it just renames it. The temp-part flush threshold is min_insert_block_size_rows, and the
-- background merge/mutation runs in the server's background context (NOT the client query settings), so
-- the threshold is the server default (DEFAULT_INSERT_BLOCK_SIZE = 1048449). We therefore make the
-- projection emit MORE rows than that: a high-cardinality GROUP BY key (1.3M distinct groups) forces >=2
-- temp projection parts for BOTH an OPTIMIZE merge and an ALTER ... MATERIALIZE PROJECTION rebuild.

DROP TABLE IF EXISTS t_pmb;
CREATE TABLE t_pmb (a UInt64, b UInt64, PROJECTION p_by_b (SELECT b, sum(a) GROUP BY b))
ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(type = object_storage, object_storage_type = local, metadata_type = cas,
    name = '04300_pmb', server_root_id = '04300', path = '04300_pmb_pool/');

-- 1.3M distinct b values, each appearing twice: a = number and a = number + 1300000, so for group b the
-- two rows are b and b + 1300000 -> sum(a) = 2*b + 1300000. The projection emits 1.3M rows > 1048449 ->
-- >= 2 temp projection parts on rebuild.
INSERT INTO t_pmb SELECT number, number FROM numbers(1300000);
INSERT INTO t_pmb SELECT number + 1300000, number FROM numbers(1300000);

SELECT 'count', count() FROM t_pmb;
SELECT 'by_b_top', b, sum(a) AS s FROM t_pmb GROUP BY b ORDER BY s DESC, b LIMIT 3;

-- MERGE the parts: the projection rebuild merges >1 temp projection part (multi-block read-back).
OPTIMIZE TABLE t_pmb FINAL;
SELECT 'after_merge_by_b_top', b, sum(a) AS s FROM t_pmb GROUP BY b ORDER BY s DESC, b LIMIT 3;

-- MUTATION that rebuilds the projection across >1 temp projection block:
ALTER TABLE t_pmb MATERIALIZE PROJECTION p_by_b SETTINGS mutations_sync = 2;
SELECT 'after_materialize_count', count() FROM t_pmb;
SELECT 'projection_active', countDistinct(name) FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_pmb' AND active;

-- Prove the projection is actually selected by the optimizer (not a silent base-table fallback).
SET optimize_use_projections = 1, force_optimize_projection = 1;
SELECT 'uses_projection', countIf(explain LIKE '%p_by_b%') > 0
FROM (EXPLAIN actions = 1 SELECT b, sum(a) FROM t_pmb GROUP BY b);
SET force_optimize_projection = 0;

-- survives reload:
DETACH TABLE t_pmb; ATTACH TABLE t_pmb;
SELECT 'after_reload_by_b_top', b, sum(a) AS s FROM t_pmb GROUP BY b ORDER BY s DESC, b LIMIT 3;

DROP TABLE t_pmb;
SELECT 'dropped_ok';
