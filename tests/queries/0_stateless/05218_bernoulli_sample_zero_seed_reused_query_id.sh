#!/usr/bin/env bash
# Tags: no-random-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `bernoulli_sample_seed = 0` promises a fresh random sample per query. The seed is derived from
# the forwarded `ClientInfo` so that remote reads agree with the initiator, but a client may
# deliberately reuse the same `query_id` for retries, so the query id alone is not enough: the
# initial query start time is mixed in, and two executions under one query id must still draw
# different samples.

QUERY_ID="05218_bernoulli_reused_${CLICKHOUSE_DATABASE}"
TABLE="t_bernoulli_reused_query_id"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $TABLE"
$CLICKHOUSE_CLIENT -q "CREATE TABLE $TABLE (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT --max_insert_threads 1 -q "INSERT INTO $TABLE SELECT number FROM numbers(100000)"

SAMPLE_QUERY="SELECT sum(x) FROM $TABLE SAMPLE 0.1 SETTINGS allow_experimental_bernoulli_sample = 1, bernoulli_sample_seed = 0"

first=$($CLICKHOUSE_CLIENT --query_id "$QUERY_ID" -q "$SAMPLE_QUERY")
second=$($CLICKHOUSE_CLIENT --query_id "$QUERY_ID" -q "$SAMPLE_QUERY")

echo "two executions under one query id draw different samples"
if [ "$first" != "$second" ]; then echo 1; else echo "0 ($first = $second)"; fi

echo "an explicit seed is still reproducible under one query id"
EXPLICIT_QUERY="SELECT sum(x) FROM $TABLE SAMPLE 0.1 SETTINGS allow_experimental_bernoulli_sample = 1, bernoulli_sample_seed = 42"
first=$($CLICKHOUSE_CLIENT --query_id "$QUERY_ID" -q "$EXPLICIT_QUERY")
second=$($CLICKHOUSE_CLIENT --query_id "$QUERY_ID" -q "$EXPLICIT_QUERY")
if [ "$first" == "$second" ]; then echo 1; else echo "0 ($first != $second)"; fi

$CLICKHOUSE_CLIENT -q "DROP TABLE $TABLE"
