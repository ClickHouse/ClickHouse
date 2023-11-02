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

SYSTEM ENABLE FAILPOINT prefeteched_reader_pool_failpoint;

SELECT * FROM prefetched_table FORMAT Null;

drop table if exists prefetched_table;
