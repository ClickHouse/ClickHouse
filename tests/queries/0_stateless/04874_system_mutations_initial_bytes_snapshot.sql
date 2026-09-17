-- Regression for the byte-weighted `progress` denominator of https://github.com/ClickHouse/ClickHouse/issues/114678:
-- the weight the mutation's scope had at submission must survive registering the entry, otherwise the
-- denominator collapses back onto the parts that are still left and `progress` under-reports.
-- Merges (and therefore mutation tasks) are stopped throughout, so nothing here is timing dependent:
-- the scope shrinks only because a part is dropped out of it, never because it was rewritten.

SET mutations_sync = 0;

DROP TABLE IF EXISTS t_mut_initial_bytes SYNC;
CREATE TABLE t_mut_initial_bytes (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
SYSTEM STOP MERGES t_mut_initial_bytes;

INSERT INTO t_mut_initial_bytes SELECT number, number FROM numbers(100000);
INSERT INTO t_mut_initial_bytes SELECT number, number FROM numbers(1000);

ALTER TABLE t_mut_initial_bytes UPDATE v = v + 1 WHERE 1;

-- Nothing has been rewritten yet.
SELECT parts_to_do, progress FROM system.mutations
WHERE database = currentDatabase() AND table = 't_mut_initial_bytes' AND NOT is_done;

-- Take the large part out of the mutation's scope without running the mutation. The remaining work is
-- now a small fraction of what it was at submission, so `progress` must be close to 1; with a lost
-- snapshot the denominator equals the remainder and it reads 0 instead.
ALTER TABLE t_mut_initial_bytes DROP PART 'all_1_1_0';

SELECT parts_to_do, progress > 0.5 FROM system.mutations
WHERE database = currentDatabase() AND table = 't_mut_initial_bytes' AND NOT is_done;

DROP TABLE t_mut_initial_bytes SYNC;
