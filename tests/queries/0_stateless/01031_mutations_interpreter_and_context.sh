#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS json_test"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE json_test (id UInt32, metadata String) ENGINE = MergeTree() ORDER BY id"

${CLICKHOUSE_CLIENT} --query="INSERT INTO json_test VALUES (1, '{\"date\": \"2018-01-01\", \"task_id\": \"billing_history__billing_history.load_history_payments_data__20180101\"}'), (2, '{\"date\": \"2018-01-02\", \"task_id\": \"billing_history__billing_history.load_history_payments_data__20180101\"}')"

${CLICKHOUSE_CLIENT} --query="SELECT COUNT() FROM json_test"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE json_test DELETE WHERE JSONExtractString(metadata, 'date') = '2018-01-01'" --mutations_sync=1

${CLICKHOUSE_CLIENT} --query="SELECT COUNT() FROM json_test"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS json_test"
