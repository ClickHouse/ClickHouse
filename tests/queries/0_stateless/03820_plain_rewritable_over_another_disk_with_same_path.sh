#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

settings=(
    # Not compatible with DC
    --write_through_distributed_cache=0
    --read_through_distributed_cache=0
)

echo "cache_on_write_operations=0"
endpoint="http://localhost:11111/test/s3_plain_rewritable_${CLICKHOUSE_DATABASE}_cache_on_write_operations_0"
cache_path="${CLICKHOUSE_DATABASE}_cache_disk_cache_on_write_operations_0"
$CLICKHOUSE_CLIENT "${settings[@]}" -nm -q "
drop table if exists mt1;
drop table if exists mt1;
drop table if exists mt3;
drop table if exists mt4;

create table mt1 (key Int) engine=MergeTree order by () settings disk=disk(cache_on_write_operations=0, type='s3_plain_rewritable', endpoint='$endpoint', access_key_id=clickhouse, secret_access_key=clickhouse);
-- cache disk on top of s3_plain_rewritable is allowed
create table mt2 (key Int) engine=MergeTree order by () settings disk=disk(cache_on_write_operations=0, type='cache', path='$cache_path', max_size=100000, disk=disk(cache_on_write_operations=0, type='s3_plain_rewritable', endpoint='$endpoint', access_key_id=clickhouse, secret_access_key=clickhouse));
-- encrypted disk with the same path as original s3_plain_rewritable disk is allowed
create table mt3 (key Int) engine=MergeTree order by () settings disk=disk(cache_on_write_operations=0, type='encrypted', key='1234567812345678', disk=disk(cache_on_write_operations=0, type='s3_plain_rewritable', endpoint='$endpoint', access_key_id=clickhouse, secret_access_key=clickhouse));
-- encrypted disk over cache is allowed
create table mt4 (key Int) engine=MergeTree order by () settings disk=disk(cache_on_write_operations=0, type='encrypted', key='1234567812345678', disk=disk(cache_on_write_operations=0, type='cache', path='$cache_path', max_size=100000, disk=disk(cache_on_write_operations=0, type='s3_plain_rewritable', endpoint='$endpoint', access_key_id=clickhouse, secret_access_key=clickhouse)));

drop table mt1;
drop table mt2;
drop table mt3;
drop table mt4;
"

echo "cache_on_write_operations=1"
endpoint="http://localhost:11111/test/s3_plain_rewritable_${CLICKHOUSE_DATABASE}_cache_on_write_operations_1"
cache_path="${CLICKHOUSE_DATABASE}_cache_disk_cache_on_write_operations_1"
$CLICKHOUSE_CLIENT "${settings[@]}" -nm -q "
drop table if exists mt1;
drop table if exists mt1;
drop table if exists mt3;
drop table if exists mt4;

create table mt1 (key Int) engine=MergeTree order by () settings disk=disk(cache_on_write_operations=1, type='s3_plain_rewritable', endpoint='$endpoint', access_key_id=clickhouse, secret_access_key=clickhouse);
-- cache disk on top of s3_plain_rewritable is allowed
create table mt2 (key Int) engine=MergeTree order by () settings disk=disk(cache_on_write_operations=1, type='cache', path='$cache_path', max_size=100000, disk=disk(cache_on_write_operations=1, type='s3_plain_rewritable', endpoint='$endpoint', access_key_id=clickhouse, secret_access_key=clickhouse));
-- encrypted disk with the same path as original s3_plain_rewritable disk is allowed
create table mt3 (key Int) engine=MergeTree order by () settings disk=disk(cache_on_write_operations=1, type='encrypted', key='1234567812345678', disk=disk(cache_on_write_operations=1, type='s3_plain_rewritable', endpoint='$endpoint', access_key_id=clickhouse, secret_access_key=clickhouse));
-- encrypted disk over cache is allowed
create table mt4 (key Int) engine=MergeTree order by () settings disk=disk(cache_on_write_operations=1, type='encrypted', key='1234567812345678', disk=disk(cache_on_write_operations=1, type='cache', path='$cache_path', max_size=100000, disk=disk(cache_on_write_operations=1, type='s3_plain_rewritable', endpoint='$endpoint', access_key_id=clickhouse, secret_access_key=clickhouse)));

drop table mt1;
drop table mt2;
drop table mt3;
drop table mt4;
"
