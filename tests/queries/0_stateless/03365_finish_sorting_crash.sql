CREATE TABLE test(
    key String,
    val Array(String)
) engine = MergeTree
order by key;

insert into test VALUES ('', ['xx']);
insert into test VALUES ('', []);

SELECT key, arrayJoin(val) as res FROM test ORDER BY ALL settings max_threads = 1, read_in_order_two_level_merge_threshold = 0 format Null;
