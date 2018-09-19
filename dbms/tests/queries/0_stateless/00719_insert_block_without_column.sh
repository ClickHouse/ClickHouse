#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "select number as SomeID, number+1 as OtherID from system.numbers limit 1000 into outfile '${CLICKHOUSE_TMP}/test_squashing_block_without_column.out' format Native"

${CLICKHOUSE_CLIENT} --query "drop table if exists test.squashed_numbers"
${CLICKHOUSE_CLIENT} --query "create table test.squashed_numbers (SomeID UInt64, DifferentID UInt64, OtherID UInt64) engine Memory"

cat ${CLICKHOUSE_TMP}/test_squashing_block_without_column.out | ${CLICKHOUSE_CLIENT} --query "insert into test.squashed_numbers format Native"

${CLICKHOUSE_CLIENT} --query "select 'Still alive'"
