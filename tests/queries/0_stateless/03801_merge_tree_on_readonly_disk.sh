#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
create table mt_ro (key Int) settings disk=disk(readonly = 1, type = 's3_plain_rewritable', endpoint = 'http://localhost:11111/test/s3_plain_rewritable_$CLICKHOUSE_DATABASE', access_key_id = clickhouse, secret_access_key = clickhouse);

insert into mt_ro values (1); -- { serverError TABLE_IS_READ_ONLY }

drop table mt_ro;
"

$CLICKHOUSE_CLIENT -nm -q "
create table mt_ro (key Int) settings disk=disk(type='encrypted', readonly=1, key='1234567812345678', disk=disk(readonly = 1, type = 's3_plain_rewritable', endpoint = 'http://localhost:11111/test/s3_plain_rewritable_encrypted_$CLICKHOUSE_DATABASE', access_key_id = clickhouse, secret_access_key = clickhouse));

insert into mt_ro values (1); -- { serverError TABLE_IS_READ_ONLY }

drop table mt_ro;
"
