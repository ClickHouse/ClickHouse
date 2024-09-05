set max_threads = 1, read_in_order_two_level_merge_threshold = 1;

CREATE OR REPLACE TABLE test(
    key String,
    val Map(String, String)
) engine = MergeTree
order by key settings min_bytes_for_wide_part = 0;
insert into test VALUES ('', {'x':'xx'});
insert into test VALUES ('', {});
SELECT key, arrayJoin(mapValues(val)) as v FROM test ORDER BY key, v;
DROP TABLE test;

