drop table if exists query_coordination_fallback;
drop table if exists query_coordination_fallback_all;

set allow_experimental_query_coordination = 1;

CREATE TABLE query_coordination_fallback
(
    f0 UInt64,
    f1 Float64
) Engine = MergeTree() order by f0;

CREATE TABLE query_coordination_fallback_all AS query_coordination_fallback
Engine Distributed('test_shard_localhost', 'default', 'query_coordination_fallback', rand());

INSERT INTO query_coordination_fallback Values(1, 1.0);

Explain select count(*) from query_coordination_fallback_all;

Explain select * from query_coordination_fallback;

drop table if exists query_coordination_fallback;
drop table if exists query_coordination_fallback_all;