#!/usr/bin/env bash
# Tags: shard

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=trace

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --optimize_skip_unused_shards=1 -nm -q "
create table dist_01758 as system.one engine=Distributed(test_cluster_two_shards, system, one, dummy);
select * from dist_01758 where dummy = 0 format Null;
" |& grep -o "StorageDistributed (dist_01758).*"

$CLICKHOUSE_CLIENT -q "drop table dist_01758" 2>/dev/null
