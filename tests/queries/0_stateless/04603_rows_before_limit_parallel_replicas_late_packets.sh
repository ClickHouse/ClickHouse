#!/usr/bin/env bash
# Tags: long, no-parallel, no-random-settings, no-random-merge-tree-settings
# no-parallel: enables a global server-side failpoint that delays the trailing
# packets of every secondary (remote) query on the server, which would slow down
# unrelated concurrently-running tests.
# long: each failpoint-enabled query deliberately sleeps for a second per remote
# connection before the trailing packets are sent.
#
# Deterministic reproducer for issue #85785 and its rows-before-* sibling: when a
# query with `LIMIT` reads from remote connections (parallel replicas or the
# `remote` table function), the statistics (`rows_read`, `bytes_read`) and
# `rows_before_limit_at_least` reported in JSON/XML output could be lost (reported
# as 0), because the remote servers' trailing `Progress` and `ProfileInfo` packets
# arrived after the initiator had cancelled the query on the early `LIMIT` break,
# and the output format had already written its trailer.
#
# The failpoint `tcp_handler_sleep_before_secondary_query_trailing_packets` makes
# the race deterministic: each remote server sends its data blocks, then sleeps
# before sending the trailing `ProfileInfo` / `Progress` / `EndOfStream`. The
# initiator therefore reaches the post-`LIMIT` cancellation before any trailing
# packet arrives, and only the connection drain in `RemoteQueryExecutor::finish`
# together with the deferred (two-phase) statistics finalization can produce the
# correct values. On a build without the fix the test fails: the drain is absent
# and the trailer is written before the late packets arrive.
#
# `rows_read` is checked on the parallel-replicas path, where the replicas send
# their reading progress in trailing `Progress` packets. `rows_before_limit_at_least`
# is checked on the `remote` table function path, where the counter is fed by the
# shard's trailing `ProfileInfo` (with parallel replicas the counter is attached to
# the initiator's own `LimitTransform`, so it is not sensitive to the drain there).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_NAME="t_04603_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS ${TABLE_NAME}"
# 1000 rows fit into a single granule (default index_granularity = 8192), so the
# whole table is read as one unit and rows_read / rows_before_limit_at_least are
# single deterministic values, independent of how the remote reading splits the work.
$CLICKHOUSE_CLIENT --query="CREATE TABLE ${TABLE_NAME} (number UInt64) ENGINE = MergeTree ORDER BY number"
$CLICKHOUSE_CLIENT --query="INSERT INTO ${TABLE_NAME} SELECT * FROM system.numbers LIMIT 1000"

PR_SETTINGS="enable_parallel_replicas=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas', parallel_replicas_for_non_replicated_merge_tree=1"
REMOTE_TABLE="remote('127.0.0.2', ${CLICKHOUSE_DATABASE}.${TABLE_NAME})"

# Ground truth: the same query without any remote connections. The single-node read
# has no remote-drain race, so these values are correct on any build and must equal
# the remote-read values on a correct build.
REFERENCE=$($CLICKHOUSE_CLIENT --query="SELECT number FROM ${TABLE_NAME} LIMIT 10 FORMAT JSON SETTINGS enable_parallel_replicas=0")
EXPECTED_ROWS_READ=$(echo "$REFERENCE" | grep -o '"rows_read": [0-9]*' | grep -o '[0-9]*')
EXPECTED_ROWS_BEFORE_LIMIT=$(echo "$REFERENCE" | grep -o '"rows_before_limit_at_least": [0-9]*' | grep -o '[0-9]*')

function check_field()
{
    local label=$1
    local field=$2
    local expected=$3
    local output=$4
    local value
    value=$(echo "$output" | grep -o "\"${field}\": [0-9]*" | grep -o '[0-9]*')
    if [ "$value" = "$expected" ]; then
        echo "${label} ${field} OK"
    else
        echo "${label} ${field} FAIL: ${value} (expected ${expected})"
    fi
}

function check_field_xml()
{
    local label=$1
    local field=$2
    local expected=$3
    local output=$4
    local value
    value=$(echo "$output" | grep -o "<${field}>[0-9]*</${field}>" | grep -o '[0-9]*')
    if [ "$value" = "$expected" ]; then
        echo "${label} ${field} OK"
    else
        echo "${label} ${field} FAIL: ${value} (expected ${expected})"
    fi
}

# The JSON*EachRowWithProgress trailer rows have no space after the colon.
function check_field_compact()
{
    local label=$1
    local field=$2
    local expected=$3
    local output=$4
    local value
    value=$(echo "$output" | grep -o "\"${field}\":[0-9]*" | grep -o '[0-9]*')
    if [ "$value" = "$expected" ]; then
        echo "${label} ${field} OK"
    else
        echo "${label} ${field} FAIL: ${value} (expected ${expected})"
    fi
}

# In the WithProgress formats the progress rows are the only place where the reading
# statistics appear, so the last progress row must carry the post-drain read_rows.
function check_last_progress_read_rows()
{
    local label=$1
    local expected=$2
    local output=$3
    local value
    value=$(echo "$output" | grep '"progress"' | tail -1 | grep -o '"read_rows":"[0-9]*"' | grep -o '[0-9]*')
    if [ "$value" = "$expected" ]; then
        echo "${label} last progress read_rows OK"
    else
        echo "${label} last progress read_rows FAIL: ${value} (expected ${expected})"
    fi
}

# The Template trailer parts are printed as name=value by the format string below.
function check_template_field()
{
    local label=$1
    local field=$2
    local expected=$3
    local output=$4
    local value
    value=$(echo "$output" | grep -o "${field}=[0-9]*" | grep -o '[0-9]*')
    if [ "$value" = "$expected" ]; then
        echo "${label} ${field} OK"
    else
        echo "${label} ${field} FAIL: ${value} (expected ${expected})"
    fi
}

# Make sure the global failpoint does not leak into subsequent tests even if this
# test fails in the middle.
trap '$CLICKHOUSE_CLIENT --query="SYSTEM DISABLE FAILPOINT tcp_handler_sleep_before_secondary_query_trailing_packets"' EXIT

$CLICKHOUSE_CLIENT --query="SYSTEM ENABLE FAILPOINT tcp_handler_sleep_before_secondary_query_trailing_packets"

# --- Parallel replicas: the replicas' trailing Progress packets carry the reading
# statistics; check rows_read on server-side (HTTP) and client-side (TCP) formatting.
OUTPUT=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM ${TABLE_NAME} LIMIT 10 FORMAT JSON SETTINGS ${PR_SETTINGS}")
check_field "parallel replicas JSON HTTP" "rows_read" "$EXPECTED_ROWS_READ" "$OUTPUT"

OUTPUT=$($CLICKHOUSE_CLIENT --query="SELECT number FROM ${TABLE_NAME} LIMIT 10 FORMAT JSON SETTINGS ${PR_SETTINGS}")
check_field "parallel replicas JSON TCP" "rows_read" "$EXPECTED_ROWS_READ" "$OUTPUT"

OUTPUT=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM ${TABLE_NAME} LIMIT 10 FORMAT XML SETTINGS ${PR_SETTINGS}")
check_field_xml "parallel replicas XML HTTP" "rows_read" "$EXPECTED_ROWS_READ" "$OUTPUT"

# --- remote table function: the shard's trailing ProfileInfo carries
# rows_before_limit_at_least; the trailing Progress carries the reading statistics.
OUTPUT=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM ${REMOTE_TABLE} LIMIT 10 FORMAT JSON")
check_field "remote JSON HTTP" "rows_read" "$EXPECTED_ROWS_READ" "$OUTPUT"
check_field "remote JSON HTTP" "rows_before_limit_at_least" "$EXPECTED_ROWS_BEFORE_LIMIT" "$OUTPUT"

OUTPUT=$($CLICKHOUSE_CLIENT --query="SELECT number FROM ${REMOTE_TABLE} LIMIT 10 FORMAT JSON")
check_field "remote JSON TCP" "rows_read" "$EXPECTED_ROWS_READ" "$OUTPUT"
check_field "remote JSON TCP" "rows_before_limit_at_least" "$EXPECTED_ROWS_BEFORE_LIMIT" "$OUTPUT"

OUTPUT=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM ${REMOTE_TABLE} LIMIT 10 FORMAT JSONColumnsWithMetadata")
check_field "remote JSONColumnsWithMetadata HTTP" "rows_read" "$EXPECTED_ROWS_READ" "$OUTPUT"
check_field "remote JSONColumnsWithMetadata HTTP" "rows_before_limit_at_least" "$EXPECTED_ROWS_BEFORE_LIMIT" "$OUTPUT"

OUTPUT=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM ${REMOTE_TABLE} LIMIT 10 FORMAT XML")
check_field_xml "remote XML HTTP" "rows_read" "$EXPECTED_ROWS_READ" "$OUTPUT"
check_field_xml "remote XML HTTP" "rows_before_limit_at_least" "$EXPECTED_ROWS_BEFORE_LIMIT" "$OUTPUT"

# --- output_format_write_statistics=0: rows_before_limit_at_least is emitted outside the
# "statistics" object, so it is still printed even when the statistics object is disabled. It must
# reflect the post-drain value, which means the trailer must be deferred regardless of
# write_statistics (the two-phase finalization must not key itself off write_statistics alone).
OUTPUT=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM ${REMOTE_TABLE} LIMIT 10 FORMAT JSON SETTINGS output_format_write_statistics=0")
check_field "remote JSON HTTP no-stats" "rows_before_limit_at_least" "$EXPECTED_ROWS_BEFORE_LIMIT" "$OUTPUT"

OUTPUT=$($CLICKHOUSE_CLIENT --query="SELECT number FROM ${REMOTE_TABLE} LIMIT 10 FORMAT JSON SETTINGS output_format_write_statistics=0")
check_field "remote JSON TCP no-stats" "rows_before_limit_at_least" "$EXPECTED_ROWS_BEFORE_LIMIT" "$OUTPUT"

OUTPUT=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM ${REMOTE_TABLE} LIMIT 10 FORMAT JSONColumnsWithMetadata SETTINGS output_format_write_statistics=0")
check_field "remote JSONColumnsWithMetadata HTTP no-stats" "rows_before_limit_at_least" "$EXPECTED_ROWS_BEFORE_LIMIT" "$OUTPUT"

OUTPUT=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM ${REMOTE_TABLE} LIMIT 10 FORMAT XML SETTINGS output_format_write_statistics=0")
check_field_xml "remote XML HTTP no-stats" "rows_before_limit_at_least" "$EXPECTED_ROWS_BEFORE_LIMIT" "$OUTPUT"

# --- Formats that print the whole trailer from finalizeImpl: JSONEachRowWithProgress /
# JSONCompactEachRowWithProgress (rows_before_limit_at_least row and the final progress row)
# and Template (${rows_before_limit}, ${rows_read}). Their trailer must also be deferred
# until after the drain.
OUTPUT=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM ${REMOTE_TABLE} LIMIT 10 FORMAT JSONEachRowWithProgress")
check_field_compact "remote JSONEachRowWithProgress HTTP" "rows_before_limit_at_least" "$EXPECTED_ROWS_BEFORE_LIMIT" "$OUTPUT"
check_last_progress_read_rows "remote JSONEachRowWithProgress HTTP" "$EXPECTED_ROWS_READ" "$OUTPUT"

OUTPUT=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM ${REMOTE_TABLE} LIMIT 10 FORMAT JSONCompactEachRowWithProgress")
check_field_compact "remote JSONCompactEachRowWithProgress HTTP" "rows_before_limit_at_least" "$EXPECTED_ROWS_BEFORE_LIMIT" "$OUTPUT"

OUTPUT=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM ${REMOTE_TABLE} LIMIT 10 FORMAT Template SETTINGS format_template_row_format='\${number:Escaped}', format_template_resultset_format='\${data}\nrows_before_limit=\${rows_before_limit:Escaped}\nrows_read=\${rows_read:Escaped}\n'")
check_template_field "remote Template HTTP" "rows_before_limit" "$EXPECTED_ROWS_BEFORE_LIMIT" "$OUTPUT"
check_template_field "remote Template HTTP" "rows_read" "$EXPECTED_ROWS_READ" "$OUTPUT"

# --- async_socket_for_remote=0: the synchronous read path. Here the reading thread may be
# blocked in `receivePacket` without holding the cancellation mutex when the post-`LIMIT`
# cancellation arrives, so `RemoteQueryExecutor::finish` must not drain the connections from
# the cancelling thread; it hands the drain off to the reading thread instead. The trailing
# statistics must be exactly as correct as on the default asynchronous path.
OUTPUT=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM ${REMOTE_TABLE} LIMIT 10 FORMAT JSON SETTINGS async_socket_for_remote=0")
check_field "remote JSON HTTP sync-read" "rows_read" "$EXPECTED_ROWS_READ" "$OUTPUT"
check_field "remote JSON HTTP sync-read" "rows_before_limit_at_least" "$EXPECTED_ROWS_BEFORE_LIMIT" "$OUTPUT"

OUTPUT=$($CLICKHOUSE_CLIENT --query="SELECT number FROM ${REMOTE_TABLE} LIMIT 10 FORMAT JSON SETTINGS async_socket_for_remote=0")
check_field "remote JSON TCP sync-read" "rows_read" "$EXPECTED_ROWS_READ" "$OUTPUT"
check_field "remote JSON TCP sync-read" "rows_before_limit_at_least" "$EXPECTED_ROWS_BEFORE_LIMIT" "$OUTPUT"

OUTPUT=$($CLICKHOUSE_CLIENT --query="SELECT number FROM ${TABLE_NAME} LIMIT 10 FORMAT JSON SETTINGS ${PR_SETTINGS}, async_socket_for_remote=0")
check_field "parallel replicas JSON TCP sync-read" "rows_read" "$EXPECTED_ROWS_READ" "$OUTPUT"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS ${TABLE_NAME}"
