# Tags: no-fasttest
# no-fasttest: requires object storage

CREATE TABLE test_3(id UInt64, a Array(Int64)) ENGINE = MergeTree ORDER BY id;

insert into test_3 select number, [number] from numbers(0, 100000);

SELECT count()
FROM test_3 AS a, test_3 AS b, test_3 AS c, test_3 AS d
WHERE (a.id = (b.id + 1)) AND (b.id = (c.id + 100)) AND ((c.id % 11111) = ((d.id % 12345) + 17));


SELECT count()
FROM test_3 AS a, test_3 AS b, test_3 AS c, test_3 AS d
WHERE (a.id = (b.id + 1)) AND (b.id = (c.id + 100)) AND ((c.id % 11111) = ((d.id % 12345) + 17))
SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, default_shuffle_join_bucket_count = 5,
    query_plan_use_new_logical_join_step=1, force_exchange_kind='Persisted';

SELECT count()
FROM test_3 AS a, test_3 AS b, test_3 AS c, test_3 AS d
WHERE (a.id = (b.id + 1)) AND (b.id = (c.id + 100)) AND ((c.id % 11111) = ((d.id % 12345) + 17))
SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, default_shuffle_join_bucket_count = 3,
    query_plan_use_new_logical_join_step=1, force_exchange_kind='Streaming';
