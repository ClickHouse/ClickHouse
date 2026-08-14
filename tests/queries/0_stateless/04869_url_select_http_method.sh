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
# `schema_inference_use_cache_for_url=0` (query 6): keeps that query a plain fresh-inference
# check, independent of the schema-cache behavior that query 8 covers.
SETTINGS_OPT=--http_make_head_request=0

TAG_GET="${CLICKHOUSE_DATABASE}_62352_get"
TAG_POST="${CLICKHOUSE_DATABASE}_62352_post"
TAG_PUT_READ="${CLICKHOUSE_DATABASE}_62352_put_read"
TAG_ENGINE="${CLICKHOUSE_DATABASE}_62352_engine"
TAG_INFER="${CLICKHOUSE_DATABASE}_62352_infer"
TAG_CACHE="${CLICKHOUSE_DATABASE}_62352_cache"
TAG_KVORDER="${CLICKHOUSE_DATABASE}_62352_kvorder"
TAG_METHODALIAS="${CLICKHOUSE_DATABASE}_62352_methodalias"
TAG_ALIASCAP="${CLICKHOUSE_DATABASE}_62352_aliascap"
TAG_WINSERT="${CLICKHOUSE_DATABASE}_62352_winsert"

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

# 7. http_method='POST' is rejected with `*`/`**` wildcards expanded from HTTP index pages
#    (for both the url table function and the URL engine), instead of silently probing the
#    literal `*` URL.
$CLICKHOUSE_CLIENT -q "SELECT * FROM url('http://localhost:1/files/*.csv', 'CSV', 'x String', http_method='POST')" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT -q "CREATE TABLE url_wild_62352 (x String) ENGINE = URL('http://localhost:1/files/*.csv', CSV, http_method='POST')" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
# An INSERT with auto-deduced structure infers the schema through the storage constructor,
# which would probe the literal `*` URL with POST — rejected as well.
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION url('http://localhost:1/files/*.csv', 'CSV', http_method='POST') VALUES ('a')" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
# urlCluster never supports index-page wildcards; a configured http_method must not bypass that.
$CLICKHOUSE_CLIENT -q "SELECT * FROM urlCluster('test_cluster_two_shards_localhost', 'http://localhost:1/files/*.csv', 'CSV', 'x String', http_method='PUT')" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED'
# http_method='PUT' keeps the engine (unlike the read-only url() function) on the writable
# literal-URL backend: the CREATE succeeds without the experimental index-page setting.
$CLICKHOUSE_CLIENT -q "CREATE TABLE url_wild_put_62352 (x String) ENGINE = URL('http://localhost:1/files/*.csv', CSV, http_method='PUT')"
$CLICKHOUSE_CLIENT -q "SHOW CREATE TABLE url_wild_put_62352" | grep -c "http_method"
$CLICKHOUSE_CLIENT -q "DROP TABLE url_wild_put_62352"
# A pre-existing table can carry POST + wildcard (a named collection edited after the table
# was created): ATTACH keeps loading it, and the read path rejects it at use time.
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS nc_62352"
$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION nc_62352 AS url = 'http://localhost:1/plain.csv', format = 'CSV', http_method = 'POST'"
$CLICKHOUSE_CLIENT -q "CREATE TABLE url_nc_62352 (x String) ENGINE = URL(nc_62352)"
$CLICKHOUSE_CLIENT -q "ALTER NAMED COLLECTION nc_62352 SET url = 'http://localhost:1/files/*.csv'"
$CLICKHOUSE_CLIENT -q "DETACH TABLE url_nc_62352"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE url_nc_62352"
$CLICKHOUSE_CLIENT -q "SELECT * FROM url_nc_62352" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT -q "DROP TABLE url_nc_62352"
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION nc_62352"

# 8. The schema-inference cache is method-aware: with the cache enabled (the default), a
#    repeated POST inference stays all-POST. For POST reads no cache-validation probe is sent
#    at all (HEAD/GET metadata would describe the GET representation, not the POST response):
#    with the default `schema_inference_cache_require_modification_time_for_url=1` the cache
#    conservatively re-infers, so both requests must be POST.
$CLICKHOUSE_CLIENT $SETTINGS_OPT -q "SELECT * FROM url('${CLICKHOUSE_URL}&query=SELECT+6+AS+x+FORMAT+TSVWithNamesAndTypes&log_comment=${TAG_CACHE}', 'TSVWithNamesAndTypes', http_method='POST')"
$CLICKHOUSE_CLIENT $SETTINGS_OPT -q "SELECT * FROM url('${CLICKHOUSE_URL}&query=SELECT+6+AS+x+FORMAT+TSVWithNamesAndTypes&log_comment=${TAG_CACHE}', 'TSVWithNamesAndTypes', http_method='POST')"

# 9. Key-value arguments can be combined and used out of tail position: `http_method` in the
#    middle of the list together with `headers(...)` keeps the positional meaning of the
#    remaining arguments.
$CLICKHOUSE_CLIENT $SETTINGS_OPT -q "SELECT * FROM url('${CLICKHOUSE_URL}&query=SELECT+7&log_comment=${TAG_KVORDER}', http_method='POST', 'LineAsString', 'line String', headers('X-ClickHouse-Test'='1'))"

# 10. `method='POST'` is an alias spelling of `http_method='POST'`, mirroring the named
#     collection keys of the same names.
$CLICKHOUSE_CLIENT $SETTINGS_OPT -q "SELECT * FROM url('${CLICKHOUSE_URL}&query=SELECT+8&log_comment=${TAG_METHODALIAS}', 'LineAsString', 'line String', method='POST')"

# 11. A projection alias named `http_method` must not be captured into the table-function
#     argument by the analyzer: the query works and the read still uses POST.
$CLICKHOUSE_CLIENT $SETTINGS_OPT -q "SELECT 1 AS http_method FROM url('${CLICKHOUSE_URL}&query=SELECT+9&log_comment=${TAG_ALIASCAP}', 'LineAsString', 'line String', http_method='POST')"

# 12. INSERT through url() uses POST by default on the wire.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_ins_62352 (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT $SETTINGS_OPT -q "INSERT INTO FUNCTION url('${CLICKHOUSE_URL}&query=INSERT+INTO+${CLICKHOUSE_DATABASE}.t_ins_62352+FORMAT+TSV&log_comment=${TAG_WINSERT}', 'TSV', 'x UInt64') VALUES (42)"
$CLICKHOUSE_CLIENT -q "SELECT x FROM t_ins_62352"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_ins_62352"

# 13. The http_method argument survives a DETACH/ATTACH round-trip of the URL engine.
$CLICKHOUSE_CLIENT -q "CREATE TABLE url_roundtrip_62352 (line String) ENGINE = URL('http://localhost:1/', 'LineAsString', http_method='POST')"
$CLICKHOUSE_CLIENT -q "DETACH TABLE url_roundtrip_62352"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE url_roundtrip_62352"
$CLICKHOUSE_CLIENT -q "SHOW CREATE TABLE url_roundtrip_62352" | grep -c "http_method"
$CLICKHOUSE_CLIENT -q "DROP TABLE url_roundtrip_62352"

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
      AND log_comment IN ('${TAG_GET}', '${TAG_POST}', '${TAG_PUT_READ}', '${TAG_ENGINE}', '${TAG_INFER}', '${TAG_CACHE}', '${TAG_KVORDER}', '${TAG_METHODALIAS}', '${TAG_ALIASCAP}', '${TAG_WINSERT}')
    GROUP BY log_comment
    ORDER BY log_comment"
