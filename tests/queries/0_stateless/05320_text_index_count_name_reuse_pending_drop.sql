-- `DROP INDEX ix, ADD INDEX ix <other column>` must not let `SELECT count()` be answered from the
-- old text index's postings while the `DROP INDEX` mutation has not rewritten the part yet. The
-- rewrite to `ReadFromTextIndexCount` locates the index by file name and splits the parts only by
-- whether that file is there, so the postings of the previous definition would be counted as if they
-- belonged to the new one.

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET optimize_trivial_count_query = 1;
SET query_plan_optimize_count_from_text_index = 1;
SET max_rows_to_group_by = 0; -- make_distributed_plan rejects a nonzero limit
SET make_distributed_plan = 0;
SET serialize_query_plan = 0;
SET alter_sync = 0, mutations_sync = 0;

DROP TABLE IF EXISTS t_txr;
CREATE TABLE t_txr (a String, b String, INDEX ix a TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY tuple();

-- `needle` is a token of `b` only, so the old index of `a` holds no posting for it at all.
INSERT INTO t_txr SELECT 'alpha', if(number = 500, 'needle', 'beta') FROM numbers(1000);

-- Keeps the `DROP INDEX` mutation pending.
SYSTEM STOP MERGES t_txr;

ALTER TABLE t_txr DROP INDEX ix, ADD INDEX ix b TYPE text(tokenizer = splitByNonAlpha);

SELECT '-- count over the re-added index';
SELECT count() FROM t_txr WHERE hasToken(b, 'needle');
SELECT count() FROM t_txr WHERE hasToken(b, 'needle') SETTINGS use_skip_indexes = 0;

SELECT '-- after the mutations are applied, the count is still right';
SYSTEM START MERGES t_txr;
-- Wait the pending `DROP INDEX` out on its own, with a mutation that leaves `ix` alone. One
-- mutation carrying both the drop and the materialization records `ix` in `indices_to_drop_names`
-- and then skips rebuilding the index of that name, so it would remove the index instead of
-- building it and the count below would pass on a row scan, without the index ever being read.
ALTER TABLE t_txr UPDATE a = a WHERE 0 SETTINGS mutations_sync = 2;
ALTER TABLE t_txr MATERIALIZE INDEX ix SETTINGS mutations_sync = 2;
SELECT count() = 0 FROM system.parts
WHERE database = currentDatabase() AND table = 't_txr' AND active AND secondary_indices_marks_bytes = 0;
SELECT count() FROM t_txr WHERE hasToken(b, 'needle');

DROP TABLE t_txr;
