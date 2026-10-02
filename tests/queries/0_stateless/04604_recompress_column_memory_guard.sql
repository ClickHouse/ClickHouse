-- RECOMPRESS COLUMN re-compresses a column's on-disk data streams. A Memory table keeps column data in
-- RAM and has no on-disk streams to recompress, so the statement must be rejected up front (in
-- checkMutationIsPossible) rather than silently dropped in mutate, which would make it succeed as a
-- no-op and break the feature's contract.

DROP TABLE IF EXISTS t_recompress_memory;

CREATE TABLE t_recompress_memory (id UInt64, s String) ENGINE = Memory;
INSERT INTO t_recompress_memory SELECT number, repeat('a', 10) FROM numbers(10);

ALTER TABLE t_recompress_memory RECOMPRESS COLUMN s; -- { serverError NOT_IMPLEMENTED }

-- Combined with a supported mutation the whole ALTER is rejected, so the UPDATE is not applied either.
ALTER TABLE t_recompress_memory UPDATE s = 'x' WHERE 1, RECOMPRESS COLUMN s; -- { serverError NOT_IMPLEMENTED }

SELECT 'unchanged', count(), countIf(s = repeat('a', 10)) FROM t_recompress_memory;

-- A supported mutation on the same Memory table still works (the guard only rejects RECOMPRESS COLUMN).
ALTER TABLE t_recompress_memory UPDATE s = 'updated' WHERE id = 0 SETTINGS mutations_sync = 2;
SELECT 'update works', countIf(s = 'updated') FROM t_recompress_memory;

DROP TABLE t_recompress_memory;
