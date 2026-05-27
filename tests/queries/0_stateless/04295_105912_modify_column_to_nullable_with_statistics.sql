-- Regression test for #105912: SIGSEGV in canSkipConversionToNullable
-- when metadata_snapshot used by a mutation no longer contains a column
-- that the queued ALTER MODIFY command targets (race vs a concurrent
-- ALTER DROP COLUMN observed in stress tests).
--
-- This test exercises the non-race path that goes through
-- `canSkipConversionToNullable` for a column with `STATISTICS`:
-- MODIFY COLUMN to a Nullable type. With the fix, the tryGet result is
-- guarded; without the fix and a concurrent DROP the deref crashes.

SET allow_statistics = 1;
SET use_statistics = 1;
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_105912 SYNC;

CREATE TABLE t_105912 (x UInt8, y UInt8 STATISTICS(tdigest))
ENGINE = MergeTree ORDER BY x;

INSERT INTO t_105912 SELECT number, number FROM numbers(100);

-- READ_COLUMN mutation: x's type stays the same, y becomes Nullable.
-- canSkipConversionToNullable is consulted for the y command and reads
-- `metadata_snapshot->getColumns().tryGet("y")` to decide whether the
-- statistics force a real rewrite. The fix prevents a SIGSEGV if y was
-- concurrently removed from the metadata snapshot.
ALTER TABLE t_105912 MODIFY COLUMN y Nullable(UInt8);

SELECT count(), sum(x), sum(y) FROM t_105912;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_105912' AND name = 'y';

DROP TABLE t_105912 SYNC;
