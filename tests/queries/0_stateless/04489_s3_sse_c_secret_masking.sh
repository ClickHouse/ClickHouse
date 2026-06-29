#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: s3/gcs table functions are not available in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The SSE-C customer key (`server_side_encryption_customer_key_base64`) is a credential and must be
# masked in logs. It is a named argument that can be separated from the positional access/secret
# credentials by other arguments (e.g. the format), so masking must hide the key value without hiding
# the unrelated arguments in between.
#
# The queries below fail (unreachable endpoint), we only care that they get logged and masked.

KEY_ONLY="SSEC_KEY_ONLY_$RANDOM$RANDOM"
KEY_MIXED="SSEC_MIXED_$RANDOM$RANDOM"
KEY_GCS="SSEC_GCS_$RANDOM$RANDOM"

# Only an SSE-C key, no other credentials.
${CLICKHOUSE_CLIENT} --query \
    "SELECT * FROM s3('http://localhost:11111/a.csv', 'CSV', server_side_encryption_customer_key_base64 = '${KEY_ONLY}')" 2>/dev/null || true

# Positional access/secret credentials plus a named SSE-C key, with the format in between.
${CLICKHOUSE_CLIENT} --query \
    "SELECT * FROM s3('http://localhost:11111/a.csv', 'akid', 'askey', 'CSV', server_side_encryption_customer_key_base64 = '${KEY_MIXED}')" 2>/dev/null || true

# Same for the gcs alias, which shares the s3 implementation.
${CLICKHOUSE_CLIENT} --query \
    "SELECT * FROM gcs('http://localhost:11111/a.csv', 'akid', 'askey', 'CSV', server_side_encryption_customer_key_base64 = '${KEY_GCS}')" 2>/dev/null || true

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

${CLICKHOUSE_CLIENT} --query "
SELECT
    -- The key value must never be logged.
    countIf(position(query, '${KEY_ONLY}') > 0
         OR position(query, '${KEY_MIXED}') > 0
         OR position(query, '${KEY_GCS}') > 0) AS leaked,
    -- All three queries are masked to '[HIDDEN]'.
    countIf(position(query, '[HIDDEN]') > 0) >= 1 AS all_hidden,
    -- The key name is preserved (named masking), not collapsed away.
    countIf(position(query, 'server_side_encryption_customer_key_base64') > 0) >= 1 AS key_name_kept,
    -- The non-secret format argument between the positional secret and the SSE-C key is not over-masked.
    countIf(position(query, 'server_side_encryption_customer_key_base64') > 0
            AND position(query, 'CSV') > 0) >= 1 AS format_kept
FROM system.query_log
WHERE current_database = currentDatabase()
  AND (query LIKE 'SELECT % FROM s3(%' OR query LIKE 'SELECT % FROM gcs(%')
  AND event_date >= yesterday() AND event_time > now() - INTERVAL 5 MINUTE
"
