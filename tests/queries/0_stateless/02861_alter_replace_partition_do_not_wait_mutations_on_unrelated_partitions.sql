-- https://github.com/ClickHouse/ClickHouse/issues/45328
-- Check that replacing one partition on a table with `ALTER TABLE REPLACE PARTITION`
-- doesn't wait for mutations on other partitions.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1
(
    `p` UInt8,
    `i` UInt64
)
ENGINE = MergeTree
PARTITION BY p
ORDER BY tuple();

INSERT INTO t1 VALUES (1, 1), (2, 2);

CREATE TABLE t2 AS t1;
INSERT INTO t2 VALUES (2, 2000);

-- mutation that is supposed to be running in background while REPLACE is performed.
-- sleep(1) is arbitrary here, we just need mutation to take long enough to be noticeable.
ALTER TABLE t1 UPDATE i = if(sleep(1), 0, 9000) IN PARTITION id '1' WHERE p == 1;
-- check that mutation is started
SELECT not(is_done) as is_running FROM system.mutations WHERE database==currentDatabase() AND table=='t1';

ALTER TABLE t1 REPLACE PARTITION id '2' FROM t2;

-- check that mutation is still running
SELECT is_done FROM system.mutations WHERE database==currentDatabase() AND table=='t1';

-- Expecting that mutation hasn't finished yet (since ALTER TABLE .. REPLACE wasn't waiting for it),
-- so row with `p == 1` still has the old value of `i == 1`.
SELECT * FROM t1 ORDER BY p;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
