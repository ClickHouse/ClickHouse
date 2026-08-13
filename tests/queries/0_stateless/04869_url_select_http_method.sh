#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Tests for https://github.com/ClickHouse/ClickHouse/issues/62352:
# `http_method='POST'` makes `SELECT` through the `url` table function / `URL` engine use POST.
#
# The inner queries below go to this server's own HTTP interface (${CLICKHOUSE_URL}), so the
# method that was actually used on the wire is recorded in `system.query_log.http_method`
# (1 = GET, 2 = POST). Each inner query is tagged with a unique `log_comment`.
#
# `http_make_head_request=0`: with the default of 1, the delayed read buffer first issues a
# HEAD pre-request for the file info, which the HTTP handler also executes and logs (with
# `http_method` = 0), polluting the per-tag method sets below.
# `schema_inference_use_cache_for_url=0` (last query): a schema-cache hit would probe the URL
# for `Last-Modified` with an extra method-unaware request.
SETTINGS_OPT=--http_make_head_request=0

TAG_GET="${CLICKHOUSE_DATABASE}_62352_get"
TAG_POST="${CLICKHOUSE_DATABASE}_62352_post"
TAG_PUT_READ="${CLICKHOUSE_DATABASE}_62352_put_read"
TAG_ENGINE="${CLICKHOUSE_DATABASE}_62352_engine"
TAG_INFER="${CLICKHOUSE_DATABASE}_62352_infer"

# 1. Default: SELECT through url() uses GET.
$CLICKHOUSE_CLIENT $SETTINGS_OPT -q "SELECT * FROM url('${CLICKHOUSE_URL}&query=SELECT+1&log_comment=${TAG_GET}', 'LineAsString', 'line String')"

# 2. http_method='POST' switches the read to POST.
$CLICKHOUSE_CLIENT $SETTINGS_OPT -q "SELECT * FROM url('${CLICKHOUSE_URL}&query=SELECT+2&log_comment=${TAG_POST}', 'LineAsString', 'line String', http_method='POST')"

# 3. PUT applies to writes only: a SELECT with http_method='PUT' still reads with GET.
$CLICKHOUSE_CLIENT $SETTINGS_OPT -q "SELECT * FROM url('${CLICKHOUSE_URL}&query=SELECT+3&log_comment=${TAG_PUT_READ}', 'LineAsString', 'line String', http_method='PUT')"

# 4. Unsupported methods are rejected before any connection is made.
$CLICKHOUSE_CLIENT -q "SELECT * FROM url('http://localhost:1/', 'LineAsString', 'line String', http_method='DELETE')" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'

# 5. The URL engine accepts the same argument, uses it for SELECT, and persists it in the table DDL.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS url_post_62352"
$CLICKHOUSE_CLIENT -q "CREATE TABLE url_post_62352 (line String) ENGINE = URL('${CLICKHOUSE_URL}&query=SELECT+4&log_comment=${TAG_ENGINE}', 'LineAsString', http_method='POST')"
$CLICKHOUSE_CLIENT $SETTINGS_OPT -q "SELECT * FROM url_post_62352"
$CLICKHOUSE_CLIENT -q "SHOW CREATE TABLE url_post_62352" | grep -c "http_method"
$CLICKHOUSE_CLIENT -q "DROP TABLE url_post_62352"

# 6. Schema inference follows the configured method: no explicit structure here,
#    so both the inference request and the data request must use POST.
$CLICKHOUSE_CLIENT $SETTINGS_OPT --schema_inference_use_cache_for_url=0 -q "SELECT * FROM url('${CLICKHOUSE_URL}&query=SELECT+5+AS+x+FORMAT+TSVWithNamesAndTypes&log_comment=${TAG_INFER}', 'TSVWithNamesAndTypes', http_method='POST')"

# Verify the methods that were actually used, per tag.
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT
        replaceOne(log_comment, '${CLICKHOUSE_DATABASE}_', ''),
        arraySort(groupUniqArray(http_method))
    FROM system.query_log
    WHERE event_date >= yesterday()
      AND type = 'QueryFinish'
      AND current_database = currentDatabase()
      AND log_comment IN ('${TAG_GET}', '${TAG_POST}', '${TAG_PUT_READ}', '${TAG_ENGINE}', '${TAG_INFER}')
    GROUP BY log_comment
    ORDER BY log_comment"
