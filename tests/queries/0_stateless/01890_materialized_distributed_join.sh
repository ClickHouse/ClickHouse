#!/usr/bin/env bash
# Tags: distributed

# FIXME: this is an .sh test because JOIN with Distributed in the left will use default database for the right.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
    drop table if exists test_distributed;
    drop table if exists test_source;
    drop table if exists test_shard;
    drop table if exists test_local;

    create table test_shard  (k UInt64, v UInt64) ENGINE Memory();
    create table test_local  (k UInt64, v UInt64) ENGINE Memory();
    create table test_source (k UInt64, v UInt64) ENGINE Memory();

    insert into test_shard  values (1, 1);
    insert into test_local  values (1, 2);

    create materialized view test_distributed engine Distributed('test_cluster_two_shards', $CLICKHOUSE_DATABASE, 'test_shard', k) as select k, v from test_source;

    select * from test_distributed td asof join $CLICKHOUSE_DATABASE.test_local tl on td.k = tl.k and td.v < tl.v;
    select * from test_distributed td asof join $CLICKHOUSE_DATABASE.test_local tl on td.k = tl.k and td.v < tl.v order by td.v;
    select * from test_distributed td asof join $CLICKHOUSE_DATABASE.test_local tl on td.k = tl.k and td.v < tl.v order by tl.v;
    select sum(td.v) from test_distributed td asof join $CLICKHOUSE_DATABASE.test_local tl on td.k = tl.k and td.v < tl.v group by tl.k;
    select sum(tl.v) from test_distributed td asof join $CLICKHOUSE_DATABASE.test_local tl on td.k = tl.k and td.v < tl.v group by td.k;
    select td.k, tl.* from test_distributed td join $CLICKHOUSE_DATABASE.test_local tl on td.k = tl.k;

    drop table test_distributed;
    drop table test_source;
    drop table test_shard;
    drop table test_local;
"
