-- Tags: no-fasttest, no-parallel-replicas, no-random-merge-tree-settings
-- no-fasttest: needs the JSON type.
-- no-parallel-replicas: EXPLAIN indexes output differs under parallel replicas.
-- no-random-merge-tree-settings: the granule counts assume a pinned index_granularity and wide parts.

SET enable_json_lazy_type_hints = 1;

-- A skip index materialized over a JSON type hint that the (wide) part has not materialized yet: the
-- column stays Dynamic on disk while the index granules are built against the hinted type. The part
-- records the built-against type so the read path can still prune with it.
SELECT '-- fresh index over an unmaterialized lazy hint (wide part)';
DROP TABLE IF EXISTS t_lazy_idx;
CREATE TABLE t_lazy_idx (id UInt32, j JSON) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_lazy_idx SELECT number, toJSONString(map('a', toString(number * 3))) FROM numbers(64);
ALTER TABLE t_lazy_idx MODIFY COLUMN j JSON(a UInt64) SETTINGS alter_sync = 2;
ALTER TABLE t_lazy_idx ADD INDEX idx j.a TYPE minmax GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_lazy_idx MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;

SELECT count() FROM t_lazy_idx WHERE j.a = 30;
SELECT count() FROM t_lazy_idx WHERE j.a > 1000000;
SELECT count() FROM t_lazy_idx WHERE j.a = 30 SETTINGS use_skip_indexes = 0;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_lazy_idx WHERE j.a = 30)
WHERE explain LIKE '%Granules: 1/16%';
DROP TABLE t_lazy_idx;

-- The parent JSON type changes (an unrelated path is added) but the indexed subcolumn keeps its type,
-- so the index remains valid and still prunes.
SELECT '-- parent type changed, indexed subcolumn unchanged';
DROP TABLE IF EXISTS t_lazy_idx2;
CREATE TABLE t_lazy_idx2 (id UInt32, j JSON) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_lazy_idx2 SELECT number, toJSONString(map('a', toString(number * 3))) FROM numbers(64);
ALTER TABLE t_lazy_idx2 MODIFY COLUMN j JSON(a UInt64) SETTINGS alter_sync = 2;
ALTER TABLE t_lazy_idx2 ADD INDEX idx j.a TYPE minmax GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_lazy_idx2 MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;
ALTER TABLE t_lazy_idx2 MODIFY COLUMN j JSON(a UInt64, b String) SETTINGS alter_sync = 2;
SELECT count() FROM t_lazy_idx2 WHERE j.a = 30;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_lazy_idx2 WHERE j.a = 30)
WHERE explain LIKE '%Granules: 1/16%';
DROP TABLE t_lazy_idx2;

-- Compact part: MATERIALIZE INDEX rewrites the part and materializes the hint, so the index prunes
-- through the ordinary path (no override recorded). Kept as a regression check.
SELECT '-- compact part';
DROP TABLE IF EXISTS t_lazy_idx_compact;
CREATE TABLE t_lazy_idx_compact (id UInt32, j JSON) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_lazy_idx_compact SELECT number, toJSONString(map('a', toString(number * 3))) FROM numbers(64);
ALTER TABLE t_lazy_idx_compact MODIFY COLUMN j JSON(a UInt64) SETTINGS alter_sync = 2;
ALTER TABLE t_lazy_idx_compact ADD INDEX idx j.a TYPE minmax GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_lazy_idx_compact MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;
SELECT count() FROM t_lazy_idx_compact WHERE j.a = 30;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_lazy_idx_compact WHERE j.a = 30)
WHERE explain LIKE '%Granules: 1/16%';
DROP TABLE t_lazy_idx_compact;

-- The top-k (ORDER BY ... LIMIT) minmax optimization also uses the index on the unmaterialized part.
SELECT '-- top-k over an unmaterialized lazy hint';
DROP TABLE IF EXISTS t_lazy_topk;
CREATE TABLE t_lazy_topk (id UInt32, j JSON) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_lazy_topk SELECT number, toJSONString(map('a', toString(number * 3))) FROM numbers(64);
ALTER TABLE t_lazy_topk MODIFY COLUMN j JSON(a UInt64) SETTINGS alter_sync = 2;
ALTER TABLE t_lazy_topk ADD INDEX idx j.a TYPE minmax GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_lazy_topk MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;
SELECT j.a FROM t_lazy_topk ORDER BY j.a LIMIT 1
SETTINGS use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100000, allow_suspicious_types_in_order_by = 1;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT j.a FROM t_lazy_topk ORDER BY j.a DESC LIMIT 1
    SETTINGS use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100000, allow_suspicious_types_in_order_by = 1)
WHERE explain ILIKE '%TopK%';
DROP TABLE t_lazy_topk;

-- A pending on-the-fly ALTER UPDATE rewrites the column's values, so a minmax index (built on the old
-- values) must NOT be used, or it would prune away rows the update moved into a new range. Correctness
-- must hold whether or not the part carries a built-against-type record.
SELECT '-- pending value update: no override';
DROP TABLE IF EXISTS t_upd;
CREATE TABLE t_upd (id UInt32, v UInt64, INDEX idx v TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0;
INSERT INTO t_upd SELECT number, number * 3 FROM numbers(64);
SYSTEM STOP MERGES t_upd;
ALTER TABLE t_upd UPDATE v = 999999 WHERE id = 0 SETTINGS mutations_sync = 0;
SELECT count() FROM t_upd WHERE v = 999999 SETTINGS apply_mutations_on_fly = 1;
SELECT count() FROM t_upd WHERE v = 999999 SETTINGS apply_mutations_on_fly = 1, use_skip_indexes = 0;
DROP TABLE t_upd;

-- Same, but the part also carries a built-against-type record (an index materialized over a lazy hint).
-- The record certifies the type, not the values, so a pending value update must still disable pruning.
SELECT '-- pending value update: with override';
DROP TABLE IF EXISTS t_upd_ov;
CREATE TABLE t_upd_ov (id UInt32, j JSON) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_upd_ov SELECT number, toJSONString(map('a', toString(number * 3))) FROM numbers(64);
ALTER TABLE t_upd_ov MODIFY COLUMN j JSON(a UInt64) SETTINGS alter_sync = 2;
ALTER TABLE t_upd_ov ADD INDEX idx j.a TYPE minmax GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_upd_ov MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;
SELECT 'prunes before update', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_upd_ov WHERE j.a = 30)
WHERE explain LIKE '%Granules: 1/16%';
SYSTEM STOP MERGES t_upd_ov;
ALTER TABLE t_upd_ov UPDATE j = '{"a":1}'::JSON WHERE id = 63 SETTINGS mutations_sync = 0;
SELECT 'does not prune after update', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_upd_ov WHERE j.a = 30 SETTINGS apply_mutations_on_fly = 1)
WHERE explain LIKE '%Granules: 1/16%';
DROP TABLE t_upd_ov;

-- Regression: a plain skip index over a normal column still prunes.
SELECT '-- normal table still prunes';
DROP TABLE IF EXISTS t_plain;
CREATE TABLE t_plain (id UInt32, v UInt64, INDEX idx v TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0;
INSERT INTO t_plain SELECT number, number * 3 FROM numbers(64);
SELECT count() FROM t_plain WHERE v = 30;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_plain WHERE v = 30)
WHERE explain LIKE '%Granules: 1/16%';
DROP TABLE t_plain;
