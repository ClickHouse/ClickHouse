#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

QUERY_ID_PREFIX="05292_${CLICKHOUSE_DATABASE}_"
QUERIES=(
    "CREATE DATABASE db_05292_hdr ENGINE = Backup('', S3('u', headers(concat('SEKRIT_05292_HDR', 'x') = 'v')))"
    "BACKUP TABLE nonexistent_05292 TO S3(nc_05292_missing, filename = 'x?X-Amz-Signature=SEKRIT_05292_NAMED')"
    "CREATE DATABASE db_05292_named ENGINE = Backup('', S3(nc_05292_missing, filename = 'x?X-Amz-Signature=SEKRIT_05292_NAMED'))"
    "BACKUP TABLE nonexistent_05292 TO S3(nc_05292_missing, 'x?X-Amz-Signature=SEKRIT_05292_POSITIONAL')"
    "CREATE DATABASE db_05292_positional ENGINE = Backup('', S3(nc_05292_missing, 'x?X-Amz-Signature=SEKRIT_05292_POSITIONAL'))"
    "CREATE DATABASE db_05292_role ENGINE = Backup('', S3('u', extra_credentials(role_arn = ['SEKRIT_05292_ROLE'])))"
    "CREATE DATABASE db_05292_key ENGINE = Backup('', S3(nc_05292_missing, concat('SEKRIT_05292_KEY', 'x') = 'v'))"
    "BACKUP TABLE nonexistent_05292 TO S3(nc_05292_missing, concat('file', 'name') = 'SEKRIT_05292_BACKUP_KEY')"
    "BACKUP TABLE nonexistent_05292 TO S3(nc_05292_missing, 925292)"
    "CREATE DATABASE db_05292_number ENGINE = Backup('', S3(nc_05292_missing, 925292))"
)

for i in "${!QUERIES[@]}"; do
    ${CLICKHOUSE_CURL} -sS --max-time 10 "${CLICKHOUSE_URL}&query_id=${QUERY_ID_PREFIX}${i}&log_queries=1&log_formatted_queries=1" \
        --data-binary "${QUERIES[i]}" > /dev/null
done

JSON=$($CLICKHOUSE_CLIENT -q "SELECT parseQueryToJSON('CREATE DATABASE db_05292_json ENGINE = Backup('''', S3(''u'', ''ak'', ''SEKRIT_05292_JSON''))') FORMAT TSVRaw")
[[ "$JSON" == *'"kind":"BACKUP_NAME"'* ]] && echo ok_tagged || echo FAIL_tagged
UNTAGGED_JSON=$(printf '%s' "$JSON" | sed 's/,"kind":"BACKUP_NAME"//')
[[ "$UNTAGGED_JSON" != "$JSON" && "$UNTAGGED_JSON" != *'"kind":"BACKUP_NAME"'* ]] && echo ok_untagged || echo FAIL_untagged
DOUBLE_UNTAGGED_JSON=$(printf '%s' "$UNTAGGED_JSON" | sed 's/,"kind":"DATABASE_ENGINE"//')
[[ "$DOUBLE_UNTAGGED_JSON" != "$UNTAGGED_JSON" && "$DOUBLE_UNTAGGED_JSON" != *'"kind":"DATABASE_ENGINE"'* ]] && echo ok_double_untagged || echo FAIL_double_untagged

JSON_URL="${CLICKHOUSE_URL}&enable_json_ast_dialect=1&dialect=clickhouse_json&log_queries=1&log_formatted_queries=1"
${CLICKHOUSE_CURL} -sS --max-time 10 "${JSON_URL}&query_id=${QUERY_ID_PREFIX}tagged" --data-binary "$JSON" > /dev/null
${CLICKHOUSE_CURL} -sS --max-time 10 "${JSON_URL}&query_id=${QUERY_ID_PREFIX}untagged" --data-binary "$UNTAGGED_JSON" > /dev/null
${CLICKHOUSE_CURL} -sS --max-time 10 "${JSON_URL}&query_id=${QUERY_ID_PREFIX}double_untagged" --data-binary "$DOUBLE_UNTAGGED_JSON" > /dev/null

LOG_FILTER="current_database = currentDatabase()
  AND query_id LIKE '${QUERY_ID_PREFIX}%'
  AND type != 'QueryStart'
  AND event_date >= yesterday() AND event_time > now() - INTERVAL 5 MINUTE"

# The query_log row of an HTTP query is written after its response (#84364): flush until all 13 have landed.
for _ in {1..60}; do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log" > /dev/null
    [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.query_log WHERE ${LOG_FILTER}") -ge 13 ]] && break
    sleep 0.5
done

$CLICKHOUSE_CLIENT -q "SELECT count() >= 13,
    countIf(query NOT LIKE '%[HIDDEN]%'),
    countIf(position(concat(query, formatted_query, exception), 'SEKRIT_05292') > 0),
    countIf(position(query, '925292') > 0),
    countIf(position(query, 'concat(''file'', ''name'')') > 0)
FROM system.query_log
WHERE ${LOG_FILTER}"
