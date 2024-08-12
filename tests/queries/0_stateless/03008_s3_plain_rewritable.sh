#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree
# Tag no-fasttest: requires S3
# Tag no-shared-merge-tree: does not support replication

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "drop table if exists test_mt"

${CLICKHOUSE_CLIENT} -nm --query "
create table test_mt (a Int32, b Int64, c Int64) engine = MergeTree() partition by intDiv(a, 1000) order by tuple(a, b)
settings disk = disk(
    name = s3_plain_rewritable,
    type = s3_plain_rewritable,
    endpoint = 'http://localhost:11111/test/test_mt/',
    access_key_id = clickhouse,
    secret_access_key = clickhouse);
"

${CLICKHOUSE_CLIENT} -nm --query "
insert into test_mt (*) values (1, 2, 0), (2, 2, 2), (3, 1, 9), (4, 7, 7), (5, 10, 2), (6, 12, 5);
insert into test_mt (*) select number, number, number from numbers_mt(10000);
select count(*) from test_mt;
select (*) from test_mt order by tuple(a, b) limit 10;
"

${CLICKHOUSE_CLIENT} --query "optimize table test_mt final"

${CLICKHOUSE_CLIENT} -m --query "
alter table test_mt add projection test_mt_projection (select * order by b)" 2>&1 | grep -Fq "SUPPORT_IS_DISABLED"

${CLICKHOUSE_CLIENT} -nm --query "
alter table test_mt update c = 0 where a % 2 = 1;
alter table test_mt add column d Int64 after c;
alter table test_mt drop column c;
" 2>&1 | grep -Fq "SUPPORT_IS_DISABLED"

${CLICKHOUSE_CLIENT} -nm --query "
detach table test_mt;
attach table test_mt;
"

${CLICKHOUSE_CLIENT} --query "drop table if exists test_mt_dst"

${CLICKHOUSE_CLIENT} -m --query "
create table test_mt_dst (a Int32, b Int64, c Int64) engine = MergeTree() partition by intDiv(a, 1000) order by tuple(a, b)
settings disk = 's3_plain_rewritable'
"

${CLICKHOUSE_CLIENT} -m --query "
alter table test_mt move partition 0 to table test_mt_dst" 2>&1 | grep -Fq "SUPPORT_IS_DISABLED"
