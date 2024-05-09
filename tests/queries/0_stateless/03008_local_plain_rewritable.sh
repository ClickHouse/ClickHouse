#!/usr/bin/env bash
# Tags: no-random-settings, no-s3-storage, no-replicated-database, no-shared-merge-tree
# Tag no-random-settings: enable after root causing flakiness

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "drop table if exists test_mt sync"

${CLICKHOUSE_CLIENT} -nm --query "
create table test_mt (a Int32, b Int64, c Int64) engine = MergeTree() partition by intDiv(a, 1000) order by tuple(a, b)
settings disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = plain_rewritable,
    path = '/var/lib/clickhouse/disks/local_plain_rewritable/')
"

${CLICKHOUSE_CLIENT} -nm --query "
insert into test_mt (*) values (1, 2, 0), (2, 2, 2), (3, 1, 9), (4, 7, 7), (5, 10, 2), (6, 12, 5);
insert into test_mt (*) select number, number, number from numbers_mt(10000);
"

${CLICKHOUSE_CLIENT} -nm --query "
select count(*) from test_mt;
select (*) from test_mt order by tuple(a, b) limit 10;
"

${CLICKHOUSE_CLIENT} --query "optimize table test_mt final"

${CLICKHOUSE_CLIENT} -nm --query "
select count(*) from test_mt;
select (*) from test_mt order by tuple(a, b) limit 10;
"
