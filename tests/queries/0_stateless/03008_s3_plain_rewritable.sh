#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree
# Tag no-fasttest: requires S3
# Tag no-shared-merge-tree: does not support replication

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "drop table if exists test_s3_mt"

${CLICKHOUSE_CLIENT} -m --query "
create table test_s3_mt (a Int32, b Int64, c Int64) engine = MergeTree() partition by intDiv(a, 1000) order by tuple(a, b)
settings disk = disk(
    name = 03008_s3_plain_rewritable,
    type = s3_plain_rewritable,
    endpoint = 'http://localhost:11111/test/03008_test_s3_mt/',
    access_key_id = clickhouse,
    secret_access_key = clickhouse);
"

${CLICKHOUSE_CLIENT} -m --query "
insert into test_s3_mt (*) values (1, 2, 0), (2, 2, 2), (3, 1, 9), (4, 7, 7), (5, 10, 2), (6, 12, 5);
insert into test_s3_mt (*) select number, number, number from numbers_mt(10000);
select count(*) from test_s3_mt;
select (*) from test_s3_mt order by tuple(a, b) limit 10;
"

${CLICKHOUSE_CLIENT} --query "optimize table test_s3_mt final"

${CLICKHOUSE_CLIENT} -m --query "
alter table test_s3_mt add projection test_s3_mt_projection (select * order by b)" 2>&1 | grep -Fq "SUPPORT_IS_DISABLED"

${CLICKHOUSE_CLIENT} -m --query "
alter table test_s3_mt update c = 0 where a % 2 = 1;
alter table test_s3_mt add column d Int64 after c;
alter table test_s3_mt drop column c;
" 2>&1 | grep -Fq "SUPPORT_IS_DISABLED"

${CLICKHOUSE_CLIENT} -m --query "
detach table test_s3_mt;
attach table test_s3_mt;
"

${CLICKHOUSE_CLIENT} --query "drop table if exists test_s3_mt_dst"

${CLICKHOUSE_CLIENT} -m --query "
create table test_s3_mt_dst (a Int32, b Int64, c Int64) engine = MergeTree() partition by intDiv(a, 1000) order by tuple(a, b)
settings disk = disk(
    name = 03008_s3_plain_rewritable,
    type = s3_plain_rewritable,
    endpoint = 'http://localhost:11111/test/03008_test_s3_mt/',
    access_key_id = clickhouse,
    secret_access_key = clickhouse);
"

${CLICKHOUSE_CLIENT} -m --query "
alter table test_s3_mt move partition 0 to table test_s3_mt_dst" 2>&1 | grep -Fq "SUPPORT_IS_DISABLED"

${CLICKHOUSE_CLIENT} --query "drop table test_s3_mt sync"
