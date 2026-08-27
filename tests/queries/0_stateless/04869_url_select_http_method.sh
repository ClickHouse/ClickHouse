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
#
# Statements that share settings and cannot throw are sent in one client invocation: the queries
# themselves are trivial, so the run time is dominated by client startup under sanitizers.
SETTINGS_OPT=--http_make_head_request=0

TAG_GET="${CLICKHOUSE_DATABASE}_62352_get"
TAG_POST="${CLICKHOUSE_DATABASE}_62352_post"
TAG_PUT_READ="${CLICKHOUSE_DATABASE}_62352_put_read"
TAG_ENGINE="${CLICKHOUSE_DATABASE}_62352_engine"
TAG_INFER="${CLICKHOUSE_DATABASE}_62352_infer"
TAG_CACHE="${CLICKHOUSE_DATABASE}_62352_cache"
TAG_CACHEKEY="${CLICKHOUSE_DATABASE}_62352_cachekey"
TAG_CLUSTER="${CLICKHOUSE_DATABASE}_62352_cluster"
# Named collections are server-global: derive unique-per-run names to survive the
# flaky check running this test repeatedly and concurrently.
NC_MAIN="nc_${CLICKHOUSE_DATABASE}_62352"
NC_DISP="nc_disp_${CLICKHOUSE_DATABASE}_62352"
NC_ENGINE="nc_eng_${CLICKHOUSE_DATABASE}_62352"
# Fixed UUIDs collide when the flaky check runs this test concurrently; generate per run.
read -r UUID_ATTACH UUID_ACC UUID_WILD <<< "$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4(), generateUUIDv4(), generateUUIDv4()")"
TAG_KVORDER="${CLICKHOUSE_DATABASE}_62352_kvorder"
TAG_METHODALIAS="${CLICKHOUSE_DATABASE}_62352_methodalias"
TAG_ALIASCAP="${CLICKHOUSE_DATABASE}_62352_aliascap"
TAG_WINSERT="${CLICKHOUSE_DATABASE}_62352_winsert"

# 1. Default: SELECT through url() uses GET.
# 2. http_method='POST' switches the read to POST.
# 3. PUT applies to writes only: a SELECT with http_method='PUT' still reads with GET.
$CLICKHOUSE_CLIENT $SETTINGS_OPT -q "
    SELECT * FROM url('${CLICKHOUSE_URL}&query=SELECT+1&log_comment=${TAG_GET}', 'LineAsString', 'line String');
    SELECT * FROM url('${CLICKHOUSE_URL}&query=SELECT+2&log_comment=${TAG_POST}', 'LineAsString', 'line String', http_method='POST');
    SELECT * FROM url('${CLICKHOUSE_URL}&query=SELECT+3&log_comment=${TAG_PUT_READ}', 'LineAsString', 'line String', http_method='PUT')"

# 4. Unsupported methods are rejected before any connection is made.
$CLICKHOUSE_CLIENT -q "SELECT * FROM url('http://localhost:1/', 'LineAsString', 'line String', http_method='DELETE')" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'

# 5. The URL engine accepts the same argument, uses it for SELECT, and persists it in the table DDL.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS url_post_62352;
    CREATE TABLE url_post_62352 (line String) ENGINE = URL('${CLICKHOUSE_URL}&query=SELECT+4&log_comment=${TAG_ENGINE}', 'LineAsString', http_method='POST')"
$CLICKHOUSE_CLIENT $SETTINGS_OPT -q "SELECT * FROM url_post_62352"
$CLICKHOUSE_CLIENT -q "SHOW CREATE TABLE url_post_62352; DROP TABLE url_post_62352" | grep -c "http_method"

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
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE url_wild_put_62352 (x String) ENGINE = URL('http://localhost:1/files/*.csv', CSV, http_method='PUT');
    SHOW CREATE TABLE url_wild_put_62352;
    DROP TABLE url_wild_put_62352" | grep -c "http_method"
# A pre-existing table can carry POST + wildcard (a named collection edited after the table
# was created): ATTACH keeps loading it, and the read path rejects it at use time.
$CLICKHOUSE_CLIENT -q "
    DROP NAMED COLLECTION IF EXISTS ${NC_MAIN};
    CREATE NAMED COLLECTION ${NC_MAIN} AS url = 'http://localhost:1/plain.csv', format = 'CSV', http_method = 'POST';
    CREATE TABLE url_${NC_MAIN} (x String) ENGINE = URL(${NC_MAIN});
    ALTER NAMED COLLECTION ${NC_MAIN} SET url = 'http://localhost:1/files/*.csv';
    DETACH TABLE url_${NC_MAIN};
    ATTACH TABLE url_${NC_MAIN}"
$CLICKHOUSE_CLIENT -q "SELECT * FROM url_${NC_MAIN}" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT -q "DROP TABLE url_${NC_MAIN}; DROP NAMED COLLECTION ${NC_MAIN}"
# A collection-sourced http_method must not break scheme dispatch (collections could always
# carry the key with any URL): url(nc) delegates with the key ignored — the error, if any,
# comes from the delegate backend, never from the http_method guard. The inline key-value
# argument is new syntax and is still rejected before dispatch.
$CLICKHOUSE_CLIENT -q "
    DROP NAMED COLLECTION IF EXISTS ${NC_DISP};
    CREATE NAMED COLLECTION ${NC_DISP} AS url = 'file:///nonexistent_62352.csv', format = 'CSV', structure = 'x String', http_method = 'PUT'"
$CLICKHOUSE_CLIENT -q "SELECT * FROM url(${NC_DISP})" 2>&1 | grep -c 'does not support http_method'
$CLICKHOUSE_CLIENT -q "SELECT * FROM url('file:///nonexistent_62352.csv', 'CSV', 'x String', http_method='POST')" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
# A query-time override of a collection is new syntax too — rejected like the inline form
# (only the value STORED in the collection gets the compatibility exemption above).
$CLICKHOUSE_CLIENT -q "SELECT * FROM url(${NC_DISP}, http_method='POST')" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
# Overriding the other alias leaves the stored `http_method` in use (it takes precedence), so the
# exemption still applies and dispatch proceeds.
$CLICKHOUSE_CLIENT -q "SELECT * FROM url(${NC_DISP}, method='POST')" 2>&1 | grep -c 'does not support http_method'
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION ${NC_DISP}"
# The engine mirrors the exemption: a fresh CREATE over a collection with a STORED
# http_method delegates to the scheme backend with the key ignored.
$CLICKHOUSE_CLIENT -q "
    DROP NAMED COLLECTION IF EXISTS ${NC_ENGINE};
    CREATE NAMED COLLECTION ${NC_ENGINE} AS url = 'file:///nonexistent_62352.csv', format = 'CSV', http_method = 'PUT'"
$CLICKHOUSE_CLIENT -q "CREATE TABLE url_nc_file_62352 (x String) ENGINE = URL(${NC_ENGINE})" 2>&1 | grep -c 'does not support http_method'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS url_nc_file_62352; DROP NAMED COLLECTION ${NC_ENGINE}"
# A full-definition ATTACH is fresh user input: the engine guards apply to it, unlike the
# short-syntax ATTACH of stored metadata. (Atomic databases require an explicit UUID for
# the full-definition form; the guard fires before anything is registered under it.)
$CLICKHOUSE_CLIENT -q "ATTACH TABLE url_attach_full_62352 UUID '${UUID_ATTACH}' (x String) ENGINE = URL('file:///nonexistent_62352.csv', CSV, http_method='POST')" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT -q "ATTACH TABLE url_attach_wild_62352 UUID '${UUID_WILD}' (x String) ENGINE = URL('http://localhost:1/files/*.csv', CSV)" 2>&1 | grep -o -m1 'SUPPORT_IS_DISABLED'
# The delegated engine's TABLE_ENGINE privilege is enforced for full-definition ATTACH too:
# a user granted URL but not File must not reach the File backend through dispatch.
acc_user="u_04869_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS $acc_user;
    CREATE USER $acc_user IDENTIFIED WITH no_password;
    GRANT CREATE TABLE, DROP TABLE ON ${CLICKHOUSE_DATABASE}.* TO $acc_user;
    GRANT TABLE ENGINE ON URL TO $acc_user"
$CLICKHOUSE_CLIENT --user "$acc_user" -q "ATTACH TABLE url_acc_62352 UUID '${UUID_ACC}' (x String) ENGINE = URL('file:///nonexistent_62352.csv', CSV)" 2>&1 | grep -o -m1 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT -q "DROP USER $acc_user"

# 8. The schema-inference cache is method-aware: with the cache enabled (the default), a
#    repeated POST inference stays all-POST. For POST reads no cache-validation probe is sent
#    at all (HEAD/GET metadata would describe the GET representation, not the POST response):
#    with the default `schema_inference_cache_require_modification_time_for_url=1` the cache
#    conservatively re-infers, so both requests must be POST.
#
# 9. Key-value arguments can be combined and used out of tail position: `http_method` in the
#    middle of the list together with `headers(...)` keeps the positional meaning of the
#    remaining arguments.
# 10. `method='POST'` is an alias spelling of `http_method='POST'`, mirroring the named
#     collection keys of the same names.
# 11. A projection alias named `http_method` must not be captured into the table-function
#     argument by the analyzer: the query works and the read still uses POST.
$CLICKHOUSE_CLIENT $SETTINGS_OPT -q "
    SELECT * FROM url('${CLICKHOUSE_URL}&query=SELECT+6+AS+x+FORMAT+TSVWithNamesAndTypes&log_comment=${TAG_CACHE}', 'TSVWithNamesAndTypes', http_method='POST');
    SELECT * FROM url('${CLICKHOUSE_URL}&query=SELECT+6+AS+x+FORMAT+TSVWithNamesAndTypes&log_comment=${TAG_CACHE}', 'TSVWithNamesAndTypes', http_method='POST');
    SELECT * FROM url('${CLICKHOUSE_URL}&query=SELECT+7&log_comment=${TAG_KVORDER}', http_method='POST', 'LineAsString', 'line String', headers('X-ClickHouse-Test'='1'));
    SELECT * FROM url('${CLICKHOUSE_URL}&query=SELECT+8&log_comment=${TAG_METHODALIAS}', 'LineAsString', 'line String', method='POST');
    SELECT 1 AS http_method FROM url('${CLICKHOUSE_URL}&query=SELECT+9&log_comment=${TAG_ALIASCAP}', 'LineAsString', 'line String', http_method='POST')"

# 12. INSERT through url() uses POST by default on the wire.
$CLICKHOUSE_CLIENT $SETTINGS_OPT -q "
    CREATE TABLE t_ins_62352 (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO FUNCTION url('${CLICKHOUSE_URL}&query=INSERT+INTO+${CLICKHOUSE_DATABASE}.t_ins_62352+FORMAT+TSV&log_comment=${TAG_WINSERT}', 'TSV', 'x UInt64') VALUES (42);
    SELECT x FROM t_ins_62352;
    DROP TABLE t_ins_62352"

# 13. The http_method argument survives a DETACH/ATTACH round-trip of the URL engine.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE url_roundtrip_62352 (line String) ENGINE = URL('http://localhost:1/', 'LineAsString', http_method='POST');
    DETACH TABLE url_roundtrip_62352;
    ATTACH TABLE url_roundtrip_62352"
$CLICKHOUSE_CLIENT -q "SHOW CREATE TABLE url_roundtrip_62352; DROP TABLE url_roundtrip_62352" | grep -c "http_method"

# 14. The schema-cache key includes the effective read method. Checked with the
#     modification-time requirement disabled, i.e. on the path where entries are actually
#     reused: the same URL must still keep separate entries for GET and POST.
CACHE_URL="${CLICKHOUSE_URL}&query=SELECT+9+AS+x+FORMAT+TSVWithNamesAndTypes&log_comment=${TAG_CACHEKEY}"
CACHE_OPT="--schema_inference_use_cache_for_url=1 --schema_inference_cache_require_modification_time_for_url=0"
$CLICKHOUSE_CLIENT $SETTINGS_OPT $CACHE_OPT -q "
    SELECT * FROM url('${CACHE_URL}', 'TSVWithNamesAndTypes');
    SELECT * FROM url('${CACHE_URL}', 'TSVWithNamesAndTypes', http_method='POST')"
$CLICKHOUSE_CLIENT -q "SELECT countDistinct(source), countIf(source LIKE 'POST:%') FROM system.schema_inference_cache WHERE storage = 'URL' AND source LIKE '%${TAG_CACHEKEY}%'"

# 15. urlCluster carries the method to the cluster paths: the initiator's schema inference and
#     the worker's read both go out as POST (asserted per tag below). How many shards fetch a
#     single URL depends on task distribution, so assert only that rows arrived.
$CLICKHOUSE_CLIENT $SETTINGS_OPT --schema_inference_use_cache_for_url=0 -q "SELECT count() > 0 FROM urlCluster('test_shard_localhost', '${CLICKHOUSE_URL}&query=SELECT+10+AS+x+FORMAT+TSVWithNamesAndTypes&log_comment=${TAG_CLUSTER}', 'TSVWithNamesAndTypes', http_method='POST')" 2>&1 | tail -1

# Verify the methods that were actually used, per tag.
$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT
        replaceOne(log_comment, '${CLICKHOUSE_DATABASE}_', ''),
        arraySort(groupUniqArray(http_method))
    FROM system.query_log
    WHERE event_date >= yesterday()
      AND type = 'QueryFinish'
      AND current_database = currentDatabase()
      AND log_comment IN ('${TAG_GET}', '${TAG_POST}', '${TAG_PUT_READ}', '${TAG_ENGINE}', '${TAG_INFER}', '${TAG_CACHE}', '${TAG_KVORDER}', '${TAG_METHODALIAS}', '${TAG_ALIASCAP}', '${TAG_WINSERT}', '${TAG_CLUSTER}')
    GROUP BY log_comment
    ORDER BY log_comment"
