#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
drop table if exists mt1;
drop table if exists mt1;
drop table if exists mt3;
drop table if exists mt4;

create table mt1 (key Int) engine=MergeTree order by () settings disk=disk(type='s3_plain_rewritable', endpoint='http://localhost:11111/test/s3_plain_rewritable_$CLICKHOUSE_DATABASE', access_key_id=clickhouse, secret_access_key=clickhouse);
-- cache disk on top of s3_plain_rewritable is allowed
create table mt2 (key Int) engine=MergeTree order by () settings disk=disk(type='cache', path='disk1_$CLICKHOUSE_DATABASE', max_size=100000, disk=disk(type='s3_plain_rewritable', endpoint='http://localhost:11111/test/s3_plain_rewritable_$CLICKHOUSE_DATABASE', access_key_id=clickhouse, secret_access_key=clickhouse));
-- encrypted disk with the same path as original s3_plain_rewritable disk is allowed
create table mt3 (key Int) engine=MergeTree order by () settings disk=disk(type='encrypted', key='1234567812345678', disk=disk(type='s3_plain_rewritable', endpoint='http://localhost:11111/test/s3_plain_rewritable_$CLICKHOUSE_DATABASE', access_key_id=clickhouse, secret_access_key=clickhouse));
-- encrypted disk over cache is allowed
create table mt4 (key Int) engine=MergeTree order by () settings disk=disk(type='encrypted', key='1234567812345678', disk=disk(type='cache', path='disk1_$CLICKHOUSE_DATABASE', max_size=100000, disk=disk(type='s3_plain_rewritable', endpoint='http://localhost:11111/test/s3_plain_rewritable_$CLICKHOUSE_DATABASE', access_key_id=clickhouse, secret_access_key=clickhouse)));

drop table mt1;
drop table mt2;
drop table mt3;
drop table mt4;
"
