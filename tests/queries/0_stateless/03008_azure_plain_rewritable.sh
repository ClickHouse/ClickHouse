#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree, no-distributed-cache
# Tag no-fasttest: requires Azure
# Tag no-shared-merge-tree: does not support replication
# Tag no-distributed-cache: Not supported auth type

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

container="cont-$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | tr _ -)"

${CLICKHOUSE_CLIENT} --query "drop table if exists test_azure_mt"

${CLICKHOUSE_CLIENT} -nm --query "
create table test_azure_mt (a Int32, b Int64, c Int64) engine = MergeTree() partition by intDiv(a, 1000) order by tuple(a, b)
settings disk = disk(
    type = object_storage,
    metadata_type = plain_rewritable,
    object_storage_type = azure_blob_storage,
    name = '${container}',
    path='/var/lib/clickhouse/disks/${container}/tables',
    container_name = '${container}',
    endpoint = 'http://localhost:10000/devstoreaccount1/${container}/plain-tables',
    account_name = 'devstoreaccount1',
    account_key = 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==');
"

${CLICKHOUSE_CLIENT} -nm --query "
insert into test_azure_mt (*) values (1, 2, 0), (2, 2, 2), (3, 1, 9), (4, 7, 7), (5, 10, 2), (6, 12, 5);
insert into test_azure_mt (*) select number, number, number from numbers_mt(10000);
select count(*) from test_azure_mt;
select (*) from test_azure_mt order by tuple(a, b) limit 10;
"

${CLICKHOUSE_CLIENT} --query "optimize table test_azure_mt final"

${CLICKHOUSE_CLIENT} -m --query "
alter table test_azure_mt add projection test_azure_mt_projection (select * order by b)" 2>&1 | grep -Fq "SUPPORT_IS_DISABLED"

${CLICKHOUSE_CLIENT} -nm --query "
alter table test_azure_mt update c = 0 where a % 2 = 1;
alter table test_azure_mt add column d Int64 after c;
alter table test_azure_mt drop column c;
" 2>&1 | grep -Fq "SUPPORT_IS_DISABLED"

${CLICKHOUSE_CLIENT} -nm --query "
detach table test_azure_mt;
attach table test_azure_mt;
"

${CLICKHOUSE_CLIENT} --query "drop table test_azure_mt sync"
