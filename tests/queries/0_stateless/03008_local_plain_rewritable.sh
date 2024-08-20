#!/usr/bin/env bash
# Tags: no-random-settings, no-object-storage, no-replicated-database, no-shared-merge-tree
# Tag no-random-settings: enable after root causing flakiness

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "drop table if exists 03008_test_local_mt sync"

${CLICKHOUSE_CLIENT} -m --query "
create table 03008_test_local_mt (a Int32, b Int64, c Int64)
engine = MergeTree() partition by intDiv(a, 1000) order by tuple(a, b)
settings disk = disk(
    name = 03008_local_plain_rewritable,
    type = object_storage,
    object_storage_type = local,
    metadata_type = plain_rewritable,
    path = '/var/lib/clickhouse/disks/local_plain_rewritable/')
"

${CLICKHOUSE_CLIENT} -m --query "
insert into 03008_test_local_mt (*) values (1, 2, 0), (2, 2, 2), (3, 1, 9), (4, 7, 7), (5, 10, 2), (6, 12, 5);
insert into 03008_test_local_mt (*) select number, number, number from numbers_mt(10000);
"

${CLICKHOUSE_CLIENT} -m --query "
select count(*) from 03008_test_local_mt;
select (*) from 03008_test_local_mt order by tuple(a, b) limit 10;
"

${CLICKHOUSE_CLIENT} --query "optimize table 03008_test_local_mt final;"

${CLICKHOUSE_CLIENT} -m --query "
alter table 03008_test_local_mt modify setting disk = '03008_local_plain_rewritable', old_parts_lifetime = 3600;
select engine_full from system.tables WHERE database = currentDatabase() AND name = '03008_test_local_mt';
" | grep -c "old_parts_lifetime = 3600"

${CLICKHOUSE_CLIENT} -m --query "
select count(*) from 03008_test_local_mt;
select (*) from 03008_test_local_mt order by tuple(a, b) limit 10;
"

${CLICKHOUSE_CLIENT} -m --query "
alter table 03008_test_local_mt update c = 0 where a % 2 = 1;
alter table 03008_test_local_mt add column d Int64 after c;
alter table 03008_test_local_mt drop column c;
" 2>&1 | grep -Fq "SUPPORT_IS_DISABLED"

${CLICKHOUSE_CLIENT} -m --query "
truncate table 03008_test_local_mt;
select count(*) from 03008_test_local_mt;
"

${CLICKHOUSE_CLIENT} --query "drop table 03008_test_local_mt sync"
