#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}_memory"

$CLICKHOUSE_CLIENT -nm -q "
drop database if exists $db;
create database $db engine=Memory;
use $db;

create table old_keys (key Int) engine=MergeTree() order by ();
create table keys (key Int) engine=MergeTree() order by ();
alter table keys add column old_keys int default in(key, keys);
show create keys;

use $CLICKHOUSE_DATABASE;
drop database $db;
"
