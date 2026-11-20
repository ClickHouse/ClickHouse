#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "create table test (x UInt64) engine=Memory;
insert into test from infile 'data'; -- {clientError BAD_ARGUMENTS}" | $CLICKHOUSE_LOCAL -m 

# Without the test hint the statement just fails, and `--ignore-error` reports it on stderr and
# carries on. The error text is discarded: this test is about the statement not tripping an
# assertion, and the report itself is covered by 05182_local_ignore_error_reports_execution_errors.
echo "create table test (x UInt64) engine=Memory;
insert into test from infile 'data';" | $CLICKHOUSE_LOCAL -m --ignore-error 2>/dev/null

echo "create table test (x UInt64) engine=Memory;
insert into test from infile 'data'; -- {clientError BAD_ARGUMENTS}
select 1" | $CLICKHOUSE_LOCAL -m 

