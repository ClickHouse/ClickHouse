set max_threads = 16;
set prefer_localhost_replica = 1;
set optimize_aggregation_in_order = 0;

-- { echoOn }

explain pipeline select * from (select * from numbers_mt(1e8) group by number) group by number;

explain pipeline select * from (select * from numbers_mt(1e8) group by number) order by number;

explain pipeline select number from remote('127.0.0.{1,2,3}', system, numbers_mt) group by number settings distributed_aggregation_memory_efficient = 1;

explain pipeline select number from remote('127.0.0.{1,2,3}', system, numbers_mt) group by number settings distributed_aggregation_memory_efficient = 0;

-- { echoOff }

DROP TABLE IF EXISTS proj_agg_02343;

CREATE TABLE proj_agg_02343
(
    k1 UInt32,
    k2 UInt32,
    k3 UInt32,
    value UInt32,
    PROJECTION aaaa
    (
        SELECT
            k1,
            k2,
            k3,
            sum(value)
        GROUP BY k1, k2, k3
    )
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO proj_agg_02343 SELECT 1, number % 2, number % 4, number FROM numbers(100000);
OPTIMIZE TABLE proj_agg_02343 FINAL;

-- { echoOn }

explain pipeline SELECT k1, k3, sum(value) v FROM remote('127.0.0.{1,2}', currentDatabase(), proj_agg_02343) GROUP BY k1, k3 SETTINGS distributed_aggregation_memory_efficient = 0;

explain pipeline SELECT k1, k3, sum(value) v FROM remote('127.0.0.{1,2}', currentDatabase(), proj_agg_02343) GROUP BY k1, k3 SETTINGS distributed_aggregation_memory_efficient = 1;

-- { echoOff }

create table t(a UInt64) engine = MergeTree order by (a);
system stop merges t;
create table dist_t as t engine = Distributed(test_cluster_two_shards, currentDatabase(), t, a % 2);
system stop merges dist_t;
insert into dist_t select number from numbers_mt(10);
insert into dist_t select number from numbers_mt(10);

-- { echoOn }

explain pipeline select a from remote('127.0.0.{1,2}', currentDatabase(), dist_t) group by a settings max_threads = 2, distributed_aggregation_memory_efficient = 1;
