#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: depends on bzip2

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} --aggregate_functions_null_for_empty=1 --query "create table test_date (date Int32) ENGINE = MergeTree ORDER BY (date) as select 20220920; SELECT max(date) FROM test_date";
