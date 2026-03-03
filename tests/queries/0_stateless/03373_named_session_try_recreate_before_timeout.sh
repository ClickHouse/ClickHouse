#!/usr/bin/env bash
# Tags: no-encrypted-storage

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

export DATA_FILE="$CLICKHOUSE_TMP/03373_session_test.tsv"
export SESSION="03373_session_${CLICKHOUSE_DATABASE}"
export TABLE_NAME="03373_session_test"
export SESSION_ID="${SESSION}_$RANDOM.$RANDOM"
export SETTINGS="session_id=$SESSION_ID&session_timeout=3&throw_on_unsupported_query_inside_transaction=0"

$CLICKHOUSE_CLIENT -q 'select * from numbers(1000000) format TSV' > $DATA_FILE
$CLICKHOUSE_CLIENT -q "create table $TABLE_NAME (A Int64) Engine = MergeTree order by sin(A) partition by intDiv(A, 100000)"

# set a setting to distinguish newly created named session from a reused one
$CLICKHOUSE_CURL -sS -d 'set http_max_tries=3373' "$CLICKHOUSE_URL&$SETTINGS"
$CLICKHOUSE_CURL -sS -d "select value, changed from system.settings where name = 'http_max_tries'" "$CLICKHOUSE_URL&$SETTINGS"

$CLICKHOUSE_CURL -sS -d 'begin transaction' "$CLICKHOUSE_URL&$SETTINGS"
$CLICKHOUSE_CURL -sS -d 'commit' "$CLICKHOUSE_URL&$SETTINGS&close_session=1"

$CLICKHOUSE_CURL -sS -X POST --data-binary @- \
  "$CLICKHOUSE_URL&$SETTINGS&session_check=1&query=insert+into+$TABLE_NAME+format+TSV" \
  < "$DATA_FILE" 2>&1 | {
    response=$(cat)
    echo "$response" | grep -Faq "SESSION_NOT_FOUND" || {
        echo "Expected SESSION_NOT_FOUND error"
        echo "---- FULL RESPONSE START ----"
        echo "$response"
        echo "---- FULL RESPONSE END ----"
        exit 1
    }
  }
$CLICKHOUSE_CLIENT --implicit_transaction=1 -q "select throwIf(count() != 0) from $TABLE_NAME" \
  || $CLICKHOUSE_CLIENT -q "select name, rows, active, visible, creation_tid, creation_csn from system.parts where database=currentDatabase()"

# sleep a bit more than a session timeout (3) to make sure there's enough time to close it using close time buckets
sleep 5

$CLICKHOUSE_CURL -sS -d "select value, changed from system.settings where name = 'http_max_tries'" "$CLICKHOUSE_URL&$SETTINGS"
$CLICKHOUSE_CURL -sS -X POST --data-binary @- "$CLICKHOUSE_URL&$SETTINGS&query=insert+into+$TABLE_NAME+format+TSV" < $DATA_FILE
$CLICKHOUSE_CLIENT --implicit_transaction=1 -q "select throwIf(count() != 1000000) from $TABLE_NAME" \
  || $CLICKHOUSE_CLIENT -q "select name, rows, active, visible, creation_tid, creation_csn from system.parts where database=currentDatabase()"

$CLICKHOUSE_CLIENT -q "drop table $TABLE_NAME"
