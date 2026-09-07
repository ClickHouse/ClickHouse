-- Tags: no-replicated-database, no-ordinary-database

-- An empty part created inside a transaction is not visible to any other transaction, so
-- `clearEmptyParts` refuses to drop it. A mutation of such a part must therefore be attempted rather
-- than postponed in favour of a removal that cannot happen.

-- Bounds the in-transaction wait, so a postponed mutation fails here instead of hanging.
SET max_execution_time = 60;

DROP TABLE IF EXISTS t_txn_empty_part;

CREATE TABLE t_txn_empty_part (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_txn_empty_part VALUES (1);

-- Both mutations wait for completion because they run inside a transaction. The first one has a
-- non-empty source part, so only the second one can be postponed.
BEGIN TRANSACTION;
ALTER TABLE t_txn_empty_part DELETE WHERE 1;
ALTER TABLE t_txn_empty_part DELETE WHERE a = 1;
COMMIT;

-- The count is asserted too, so that losing the mutation entries cannot satisfy the second column.
SELECT count(), countIf(is_done = 0) FROM system.mutations
WHERE database = currentDatabase() AND table = 't_txn_empty_part';

DROP TABLE t_txn_empty_part;
