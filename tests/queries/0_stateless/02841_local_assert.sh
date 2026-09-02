#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "create table test (x UInt64) engine=Memory;
insert into test from infile 'data'; -- {clientError BAD_ARGUMENTS}" | $CLICKHOUSE_LOCAL -m 

echo "create table test (x UInt64) engine=Memory;
insert into test from infile 'data';" | $CLICKHOUSE_LOCAL -m --ignore-error

echo "create table test (x UInt64) engine=Memory;
insert into test from infile 'data'; -- {clientError BAD_ARGUMENTS}
select 1" | $CLICKHOUSE_LOCAL -m 

