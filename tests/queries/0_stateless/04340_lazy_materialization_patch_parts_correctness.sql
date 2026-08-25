-- Tags: no-shared-merge-tree
-- Lazy materialization selects the top-k in the eager phase and re-reads deferred columns only for
-- the rows it already chose. The selection must therefore see patched filter values and respect the
-- lightweight-delete mask; otherwise a deferred-column re-read would resurrect rows that the full
-- query excludes. Each block first asserts (via EXPLAIN) that lazy materialization is actually applied.

SET enable_analyzer = 1;
SET enable_lightweight_update = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 10;

-- A patch on the filter column moves rows OUT of the result.
CREATE TABLE t_lazy_upd_out (id UInt64, filt UInt64, lazy String)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0;

INSERT INTO t_lazy_upd_out SELECT number, 1, 'v' || toString(number) FROM numbers(10);
INSERT INTO t_lazy_upd_out SELECT number + 10, 1, 'v' || toString(number + 10) FROM numbers(10);
UPDATE t_lazy_upd_out SET filt = 0 WHERE id IN (1, 2);
OPTIMIZE TABLE t_lazy_upd_out FINAL;

SELECT trimLeft(explain) AS s FROM (EXPLAIN SELECT lazy FROM t_lazy_upd_out WHERE filt > 0 ORDER BY id LIMIT 5) WHERE s ILIKE 'LazilyRead%';
SELECT lazy FROM t_lazy_upd_out WHERE filt > 0 ORDER BY id LIMIT 5;

DROP TABLE t_lazy_upd_out;

-- A patch on the filter column moves a row INTO the result, making it the new top row.
CREATE TABLE t_lazy_upd_in (id UInt64, filt UInt64, lazy String)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0;

INSERT INTO t_lazy_upd_in SELECT number, if(number = 0, 0, 1), 'v' || toString(number) FROM numbers(10);
INSERT INTO t_lazy_upd_in SELECT number + 10, 1, 'v' || toString(number + 10) FROM numbers(10);
UPDATE t_lazy_upd_in SET filt = 1 WHERE id = 0;
OPTIMIZE TABLE t_lazy_upd_in FINAL;

SELECT trimLeft(explain) AS s FROM (EXPLAIN SELECT lazy FROM t_lazy_upd_in WHERE filt > 0 ORDER BY id LIMIT 5) WHERE s ILIKE 'LazilyRead%';
SELECT lazy FROM t_lazy_upd_in WHERE filt > 0 ORDER BY id LIMIT 5;

DROP TABLE t_lazy_upd_in;

-- A lightweight delete (routed through patch parts) removes rows that would otherwise be in the top-k.
SET lightweight_delete_mode = 'lightweight_update_force';

CREATE TABLE t_lazy_del (id UInt64, filt UInt64, lazy String)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0;

INSERT INTO t_lazy_del SELECT number, 1, 'v' || toString(number) FROM numbers(10);
INSERT INTO t_lazy_del SELECT number + 10, 1, 'v' || toString(number + 10) FROM numbers(10);
DELETE FROM t_lazy_del WHERE id IN (0, 1, 2);
OPTIMIZE TABLE t_lazy_del FINAL;

SELECT trimLeft(explain) AS s FROM (EXPLAIN SELECT lazy FROM t_lazy_del WHERE filt > 0 ORDER BY id LIMIT 5) WHERE s ILIKE 'LazilyRead%';
SELECT lazy FROM t_lazy_del WHERE filt > 0 ORDER BY id LIMIT 5;

DROP TABLE t_lazy_del;
