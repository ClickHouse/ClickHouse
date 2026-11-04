-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings
-- no-parallel -- enables failpoint
-- no-random-settings -- depend on type of part, should always fail
drop table if exists prefetched_table;

CREATE TABLE prefetched_table(key UInt64, s String) Engine = MergeTree() order by key;

INSERT INTO prefetched_table SELECT rand(), randomString(5) from numbers(1000);
INSERT INTO prefetched_table SELECT rand(), randomString(5) from numbers(1000);
INSERT INTO prefetched_table SELECT rand(), randomString(5) from numbers(1000);
INSERT INTO prefetched_table SELECT rand(), randomString(5) from numbers(1000);
INSERT INTO prefetched_table SELECT rand(), randomString(5) from numbers(1000);

SET local_filesystem_read_prefetch=1;
SET allow_prefetched_read_pool_for_remote_filesystem=1;
SET allow_prefetched_read_pool_for_local_filesystem=1;

SYSTEM ENABLE FAILPOINT prefetched_reader_pool_failpoint;

SELECT * FROM prefetched_table FORMAT Null; --{serverError BAD_ARGUMENTS}

SYSTEM DISABLE FAILPOINT prefetched_reader_pool_failpoint;

drop table if exists prefetched_table;
