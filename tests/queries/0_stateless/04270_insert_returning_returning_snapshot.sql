-- Issue #21697: the INSERT ... RETURNING subquery must read the target table as of *after* the INSERT.
-- The RETURNING subquery context is created with `Context::createCopy`, which shares the outer query's
-- `QueryMetadataCache`. With `enable_shared_storage_snapshot_in_query=1` that cache pins one storage snapshot per
-- table for the whole query. For `INSERT INTO t SELECT ... FROM t`, the source read pins a pre-INSERT snapshot of `t`;
-- if the RETURNING subquery reused it, it would not observe the freshly inserted rows. The subquery must get a fresh
-- snapshot so its count reflects the rows just inserted.

SET async_insert = 0;
SET enable_shared_storage_snapshot_in_query = 1;

DROP TABLE IF EXISTS t_returning_snapshot;
CREATE TABLE t_returning_snapshot (id UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_returning_snapshot VALUES (1);

-- The INSERT reads `t_returning_snapshot` (1 row) as its source, then appends a row. The RETURNING subquery counts
-- the table after the INSERT and must see both rows (2), not the pre-INSERT snapshot (1).
INSERT INTO t_returning_snapshot SELECT id + 1 FROM t_returning_snapshot RETURNING (SELECT count() FROM t_returning_snapshot);

SELECT 'rows after returning';
SELECT count() FROM t_returning_snapshot;

DROP TABLE t_returning_snapshot;
