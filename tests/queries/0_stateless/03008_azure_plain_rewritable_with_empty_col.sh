#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree, no-distributed-cache, no-replicated-database
# Tag no-fasttest: requires Azure
# Tag no-shared-merge-tree: does not support replication
# Tag no-distributed-cache: Not supported auth type
# Tag no-replicated-database: plain rewritable should not be shared between replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

container="cont-$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | tr _ -)"

${CLICKHOUSE_CLIENT} --query "drop table if exists test_azure_mt"

${CLICKHOUSE_CLIENT} -nm --query "
create table test_azure_mt (a Int32, b Int64, empty String) engine = MergeTree() order by tuple(a, b)
settings min_bytes_for_wide_part = 0,
disk = disk(
    type = object_storage,
    metadata_type = plain_rewritable,
    object_storage_type = azure_blob_storage,
    name = '${container}',
    path='/var/lib/clickhouse/disks/${container}/tables',
    container_name = '${container}',
    endpoint = 'http://localhost:10000/devstoreaccount1/${container}/tables',
    account_name = 'devstoreaccount1',
    account_key = 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==');
"

${CLICKHOUSE_CLIENT} --query "optimize table test_azure_mt final"

${CLICKHOUSE_CLIENT} -nm --query "
insert into test_azure_mt (a, b) select number, number from numbers_mt(10000);
select count(*) from test_azure_mt;
select (*) from test_azure_mt order by tuple(a, b) limit 10;
"

${CLICKHOUSE_CLIENT} --query "drop table if exists test_azure_mt sync"
