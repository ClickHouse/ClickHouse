-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: the cases rely on fixed granule boundaries (index_granularity = 128).

-- `DROP INDEX ix, ADD INDEX ix <other column>` must not prune with the old index's granules
-- while the `DROP INDEX` mutation has not rewritten the part yet.

SET alter_sync = 0, mutations_sync = 0;

DROP TABLE IF EXISTS t_ixr;
CREATE TABLE t_ixr (a UInt32, b UInt32, INDEX ix a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 128;

-- The values of `a` are above all values of `b`, so granules of the old index never contain them.
INSERT INTO t_ixr SELECT number + 1000000, 999999 - number FROM numbers(10000);

-- Keeps the `DROP INDEX` mutation pending.
SYSTEM STOP MERGES t_ixr;

ALTER TABLE t_ixr DROP INDEX ix, ADD INDEX ix b TYPE minmax GRANULARITY 1;

SELECT '-- filter';
SELECT count() FROM t_ixr WHERE b = 995000;
SELECT count() FROM t_ixr WHERE b = 995000 SETTINGS use_skip_indexes = 0;

SELECT '-- top-k at analysis time';
SELECT b FROM t_ixr ORDER BY b DESC LIMIT 1
SETTINGS use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 1,
         query_plan_max_limit_for_top_k_optimization = 100000;

SELECT '-- top-k at read time';
-- max_rows_to_read is reset because the read-time path is not used when it is set.
SELECT b FROM t_ixr WHERE a > 0 ORDER BY b LIMIT 1
SETTINGS use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 1,
         use_skip_indexes_on_data_read = 1, query_plan_max_limit_for_top_k_optimization = 100000,
         max_rows_to_read = 0;

SELECT '-- after the mutations are applied, the new index is used';
SYSTEM START MERGES t_ixr;
ALTER TABLE t_ixr MATERIALIZE INDEX ix SETTINGS mutations_sync = 2;
SELECT count() FROM t_ixr WHERE b = 995000 SETTINGS max_rows_to_read = 128;

DROP TABLE t_ixr;
