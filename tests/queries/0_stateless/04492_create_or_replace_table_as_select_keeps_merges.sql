-- Tags: no-ordinary-database
-- no-ordinary-database: CREATE OR REPLACE TABLE requires an Atomic database.

-- `CREATE OR REPLACE TABLE ... AS SELECT` used to leave the published table unable to merge.
-- The replace creates a temporary table, populates it (which caches the temporary name -> temporary
-- storage in the query context), then atomically swaps it with the target via EXCHANGE. The internal
-- DROP of the replaced table then resolved the temporary name through that stale cache entry and got
-- the new, live table instead, so `flushAndPrepareForShutdown` cancelled its merges forever: every
-- later OPTIMIZE failed with ABORTED and background merges never ran again.

DROP TABLE IF EXISTS t_replace_merges;

CREATE OR REPLACE TABLE t_replace_merges (k UInt64, dt DateTime)
ENGINE = ReplacingMergeTree(dt) ORDER BY k AS SELECT number, now() FROM numbers(100);

-- The first replace is the one that publishes over an existing table, so it is the one that used to
-- poison the newly published table.
CREATE OR REPLACE TABLE t_replace_merges (k UInt64, dt DateTime)
ENGINE = ReplacingMergeTree(dt) ORDER BY k AS SELECT number, now() FROM numbers(100);

OPTIMIZE TABLE t_replace_merges FINAL;
SELECT count() FROM t_replace_merges;

DROP TABLE t_replace_merges;
