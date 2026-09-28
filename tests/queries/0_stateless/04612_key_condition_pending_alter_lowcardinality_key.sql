-- An `ALTER ... MODIFY COLUMN` that turns a primary-key column into `LowCardinality` updates the
-- metadata at once and rewrites the parts in a mutation afterwards. In between, the metadata says
-- `LowCardinality(String)` while the parts' primary index is still a plain `ColumnString`.
-- `KeyCondition` built the monotonic chain's `CAST` from the metadata type and then applied it to
-- the part's column, so it handed a `ColumnString` to a cast declared for `LowCardinality`:
-- `Bad cast from type DB::ColumnString to DB::ColumnLowCardinality`. It self-heals once the
-- mutation lands, which is why it shows up as a burst rather than a permanent failure.

DROP TABLE IF EXISTS t_lc_pk_pending;

CREATE TABLE t_lc_pk_pending (s String, n UInt32)
ENGINE = MergeTree ORDER BY s
SETTINGS index_granularity = 4;

INSERT INTO t_lc_pk_pending
SELECT concat('k', leftPad(toString(number % 40), 2, '0')), number FROM numbers(400);

-- Keep the mutation from running, so the window under test stays open.
SYSTEM STOP MERGES t_lc_pk_pending;

SET mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_lc_pk_pending MODIFY COLUMN s LowCardinality(String);

-- Assert the window is actually open; otherwise the query below would prove nothing.
SELECT 'mutation pending', countIf(NOT is_done) = 1
FROM system.mutations WHERE table = 't_lc_pk_pending' AND database = currentDatabase();

SELECT 'part column type', any(type)
FROM system.parts_columns
WHERE table = 't_lc_pk_pending' AND database = currentDatabase() AND active AND column = 's';

SELECT 'matching rows', count() FROM t_lc_pk_pending WHERE CAST(s, 'String') < 'k05'
SETTINGS log_comment = 't_lc_pk_pending_probe',
         optimize_trivial_count_query = 0,
         optimize_use_implicit_projections = 0;

SYSTEM FLUSH LOGS query_log;

-- Not merely "it does not throw": the key condition must still prune. Silently falling back to a
-- full scan would satisfy the row count above while losing what the primary key is for.
SELECT 'pruned', read_rows < 400
FROM system.query_log
WHERE log_comment = 't_lc_pk_pending_probe'
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t_lc_pk_pending;
