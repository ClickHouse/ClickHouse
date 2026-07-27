SET max_threads = 1;
SET lock_acquire_timeout = 1;
-- async_insert=0: the test expects DEADLOCK_AVOIDED from PARALLEL WITH; async INSERT returns
-- before acquiring the write lock, so the lock conflict never fires.
SET async_insert = 0;

CREATE TABLE t0 (c0 Int) ENGINE = Memory();

INSERT INTO TABLE t0 (c0) SELECT 1 PARALLEL WITH TRUNCATE t0; -- { serverError DEADLOCK_AVOIDED }
