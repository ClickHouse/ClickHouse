#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database
# Tag no-fasttest: needs Parquet
# Tag no-replicated-database: a table of the test is created from a named collection, which exists only on
# the server it was created on: the other replicas of the database fail to create the table.
# Tag no-parallel: asserts `QueryConditionCacheHits` on the instance-wide query condition cache,
# which a parallel sibling test can wipe at any moment (see 04498_query_condition_cache_local_files).

# The query condition cache for Parquet files read through the `URL` engine. The entries of a file are
# keyed by its URL and version - `Last-Modified` and the size, as a web server reports no `ETag` - and
# are used only once the modification is long enough in the past that a later write must change it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${DIR}"
trap 'rm -rf "${DIR}"; kill ${HTTP_PID} 2>/dev/null; wait ${HTTP_PID} 2>/dev/null' EXIT

# A static file server with `Last-Modified` (from the modification time of the file), `HEAD` and
# `Range` requests, like a typical web server. The file `date_skew` shifts the `Date` of its responses
# by the given number of seconds, as if the clock of the server was off, or with `none` omits it.
# The path `latest.parquet` redirects to the file named in the file `redirect`. The path `auth.parquet`
# redirects to the absolute URL of `export.parquet`, which serves `d.parquet` to the user `alice` and
# `e.parquet` to the user `bob`. The path `token.parquet` serves the same files depending on the header `X-Token`.
HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

python3 -c "
import base64, os, re, time
from email.utils import formatdate
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

ROOT = '${DIR}'

class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def send_response(self, code, message=None):
        self.send_response_only(code, message)
        skew_path = os.path.join(ROOT, 'date_skew')
        skew = open(skew_path).read().strip() if os.path.isfile(skew_path) else '0'
        if skew != 'none':
            self.send_header('Date', formatdate(time.time() + int(skew), usegmt=True))

    def serve(self, with_body):
        name = os.path.basename(self.path.split('?')[0])
        if name == 'latest.parquet':
            self.send_response(302)
            self.send_header('Location', '/' + open(os.path.join(ROOT, 'redirect')).read().strip())
            self.send_header('Content-Length', '0')
            self.end_headers()
            return
        if name == 'auth.parquet':
            self.send_response(302)
            self.send_header('Location', 'http://127.0.0.1:${HTTP_PORT}/export.parquet')
            self.send_header('Content-Length', '0')
            self.end_headers()
            return
        if name == 'export.parquet':
            auth = self.headers.get('Authorization', '')
            user = base64.b64decode(auth[len('Basic '):]).decode().split(':')[0] if auth.startswith('Basic ') else ''
            name = {'alice': 'd.parquet', 'bob': 'e.parquet'}.get(user, '')
        if name == 'token.parquet':
            name = {'alice': 'd.parquet', 'bob': 'e.parquet'}.get(self.headers.get('X-Token', ''), '')
        path = os.path.join(ROOT, name)
        if not os.path.isfile(path):
            self.send_response(404)
            self.send_header('Content-Length', '0')
            self.end_headers()
            return
        with open(path, 'rb') as f:
            data = f.read()
        begin, end = 0, len(data) - 1
        match = re.match(r'bytes=(\d+)-(\d*)', self.headers.get('Range', ''))
        if match:
            begin = int(match.group(1))
            if match.group(2):
                end = min(int(match.group(2)), end)
            self.send_response(206)
            self.send_header('Content-Range', 'bytes %d-%d/%d' % (begin, end, len(data)))
        else:
            self.send_response(200)
        self.send_header('Accept-Ranges', 'bytes')
        self.send_header('Content-Type', 'application/octet-stream')
        self.send_header('Last-Modified', formatdate(os.path.getmtime(path), usegmt=True))
        self.send_header('Content-Length', str(end - begin + 1))
        self.end_headers()
        if with_body:
            self.wfile.write(data[begin:end + 1])

    def do_GET(self):
        self.serve(True)

    def do_HEAD(self):
        self.serve(False)

    def log_message(self, *args):
        pass

class Server(ThreadingHTTPServer):
    # The server closes connections it no longer needs, e.g. after a range read; do not print that.
    def handle_error(self, request, client_address):
        pass

Server(('127.0.0.1', $HTTP_PORT), Handler).serve_forever()
" &
HTTP_PID=$!

for _ in $(seq 1 50); do
    curl -s -o /dev/null "http://127.0.0.1:${HTTP_PORT}/" && break
    sleep 0.1
done

# Writes `$1.parquet` with the values `$2`..`$2 + 999` in the row group `$3` of 100.
function write_file()
{
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION file('${DIR}/$1.parquet')
        SELECT if(intDiv(number, 1000) = $3, $2 + number % 1000, number % 1000) AS k, toString(k) AS s FROM numbers(100000)
        SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1"
}
write_file a 1000000 0
write_file b 100000 0

# Backdate the files so their versions have settled and the cache engages.
touch -d '2020-01-01 00:00:00' "${DIR}"/*.parquet

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05346"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05346 (k Int64, s String) ENGINE = URL('http://127.0.0.1:${HTTP_PORT}/{a,b}.parquet', 'Parquet')"

# The reader learns which row groups have no matching row only from a filter pushed into it as `PREWHERE`;
# a filter left above the read counts every row group as matched, and no entry is written.
SETTINGS="use_query_condition_cache = 1, input_format_parquet_use_native_reader_v3 = 1, input_format_parquet_filter_push_down = 0,
    optimize_move_to_prewhere = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000"

function run()
{
    local name=$1
    local query=$2
    local extra_settings=${3:-}
    ${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}" --query "$query SETTINGS ${SETTINGS}${extra_settings}"
}

# Per query: whether it consulted the cache at all, and whether it found entries there.
function events()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    for name in "$@"
    do
        ${CLICKHOUSE_CLIENT} --query "
            SELECT '${name}',
                ProfileEvents['QueryConditionCacheHits'] + ProfileEvents['QueryConditionCacheMisses'] > 0,
                ProfileEvents['QueryConditionCacheHits'] > 0
            FROM system.query_log
            WHERE query_id = '${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}' AND current_database = currentDatabase() AND type = 'QueryFinish'"
    done
}

echo "--- a predicate"
run where_1 "SELECT count(), min(k) FROM t_05346 WHERE s LIKE '10009%'"
run where_2 "SELECT count(), min(k) FROM t_05346 WHERE s LIKE '10009%'"
# The entries of the predicate apply to a TopN read with it as well.
run where_top_k "SELECT k FROM t_05346 WHERE s LIKE '10009%' ORDER BY k DESC LIMIT 3"

echo "--- a TopN read does not write entries"
# With the TopN filter a row group can end up without a returned row only because the threshold had
# excluded it: such a read must not record it as having no match for the predicate alone.
run top_k_1 "SELECT k FROM t_05346 WHERE k % 2 = 1 ORDER BY k DESC LIMIT 3"
run top_k_plain "SELECT count() FROM t_05346 WHERE k % 2 = 1"

echo "--- a file is rewritten"
# The values 1000900..1000999 move to the last row group of `b.parquet`, whose entries recorded that
# it has none of them.
write_file a 0 0
write_file b 1000000 99
touch -d '2020-01-02 00:00:00' "${DIR}/a.parquet"
touch -d '2020-01-02 00:00:01' "${DIR}/b.parquet"
run rewritten_1 "SELECT count(), min(k) FROM t_05346 WHERE s LIKE '10009%'"
run rewritten_2 "SELECT count(), min(k) FROM t_05346 WHERE s LIKE '10009%'"

echo "--- a row policy"
# A row policy is not part of the key: a read under it neither consults nor writes the cache. A fresh
# predicate, which no read has warmed: the policy rejects all its rows, so had the read under it written
# entries, the last row group of `b.parquet` would be recorded as having no match, the next unrestricted
# read would find it in the cache and return nothing.
POLICY_USER="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${POLICY_USER}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${POLICY_USER}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05346, READ ON URL TO ${POLICY_USER}"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY ${POLICY_USER}_policy ON ${CLICKHOUSE_DATABASE}.t_05346 USING k < 1000950 TO ${POLICY_USER}"
${CLICKHOUSE_CLIENT} --user ${POLICY_USER} --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_policy" --query "SELECT count(), min(k) FROM t_05346 WHERE s LIKE '100096%' SETTINGS ${SETTINGS}"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY ${POLICY_USER}_policy ON ${CLICKHOUSE_DATABASE}.t_05346"
${CLICKHOUSE_CLIENT} --query "DROP USER ${POLICY_USER}"
run after_policy "SELECT count(), min(k) FROM t_05346 WHERE s LIKE '100096%'"

echo "--- a recently modified file"
# Only the just touched file is read, so that the settled entries of `a.parquet` do not show up in
# the events of the query: a read of it neither consults nor writes the cache.
touch "${DIR}/b.parquet"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05346_b"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05346_b (k Int64, s String) ENGINE = URL('http://127.0.0.1:${HTTP_PORT}/b.parquet', 'Parquet')"
run recent_1 "SELECT count(), min(k) FROM t_05346_b WHERE s LIKE '10009%'"
run recent_2 "SELECT count(), min(k) FROM t_05346_b WHERE s LIKE '10009%'"

echo "--- an origin with a lagging clock"
# The clock of the web server is two minutes behind: by the clock of this server a file it has just
# written looks settled, but not by the `Date` of its response. Had the cache been used, the first read
# would record that the second row group of `c.parquet` has none of the values, and the rewrite below,
# which swaps its two row groups, keeps the size and lands in the same second of the clock of the web
# server, would keep the version of the file: the second read would skip the row group with the values.
function write_small_file()
{
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION file('${DIR}/c.parquet')
        SELECT if(intDiv(number, 1000) = $1, 1000000 + number % 1000, number % 1000) AS k, toString(k) AS s FROM numbers(2000)
        SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1"
}
echo -120 > "${DIR}/date_skew"
MODIFIED=$(( $(date +%s) - 120 ))
write_small_file 0
touch -d "@${MODIFIED}" "${DIR}/c.parquet"
SIZE_BEFORE=$(stat -c %s "${DIR}/c.parquet")
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05346_c"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05346_c (k Int64, s String) ENGINE = URL('http://127.0.0.1:${HTTP_PORT}/c.parquet', 'Parquet')"
run skew_1 "SELECT count(), min(k) FROM t_05346_c WHERE s LIKE '100099%'"
write_small_file 1
touch -d "@${MODIFIED}" "${DIR}/c.parquet"
[ "$(stat -c %s "${DIR}/c.parquet")" = "${SIZE_BEFORE}" ] || echo "The size of the rewritten file has changed"
run skew_2 "SELECT count(), min(k) FROM t_05346_c WHERE s LIKE '100099%'"

echo "--- a redirect to another file"
# `latest.parquet` redirects to `d.parquet`, then to `e.parquet`: both files have the same size and
# modification time, but the values are in different row groups. The entries recorded for `d.parquet`
# must not apply to `e.parquet`, though the URL of the table is the same.
function write_small_file_to()
{
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION file('${DIR}/$1.parquet')
        SELECT if(intDiv(number, 1000) = $2, 1000000 + number % 1000, number % 1000) AS k, toString(k) AS s FROM numbers(2000)
        SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1"
}
write_small_file_to d 0
write_small_file_to e 1
touch -d '2020-01-01 00:00:00' "${DIR}/d.parquet" "${DIR}/e.parquet"
[ "$(stat -c %s "${DIR}/d.parquet")" = "$(stat -c %s "${DIR}/e.parquet")" ] || echo "The sizes of the redirect targets differ"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05346_latest"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05346_latest (k Int64, s String) ENGINE = URL('http://127.0.0.1:${HTTP_PORT}/latest.parquet', 'Parquet')"
echo d.parquet > "${DIR}/redirect"
run redirect_1 "SELECT count(), min(k) FROM t_05346_latest WHERE s LIKE '100099%'" ", max_http_get_redirects = 1"
echo e.parquet > "${DIR}/redirect"
run redirect_2 "SELECT count(), min(k) FROM t_05346_latest WHERE s LIKE '100099%'" ", max_http_get_redirects = 1"
run redirect_3 "SELECT count(), min(k) FROM t_05346_latest WHERE s LIKE '100099%'" ", max_http_get_redirects = 1"

echo "--- credentials redirected to the same URL"
# Both URLs of the glob redirect to the same `export.parquet`, which serves different files with the same
# size and modification time depending on the credentials: the requests keep the credentials of the original
# URL after the redirect. The entries recorded for the file of one user must not apply to the file of the other.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05346_auth"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05346_auth (k Int64, s String) ENGINE = URL('http://{alice,bob}:pw@127.0.0.1:${HTTP_PORT}/auth.parquet', 'Parquet')"
run auth_1 "SELECT count(), min(k) FROM t_05346_auth WHERE s LIKE '100099%'" ", max_http_get_redirects = 1"
run auth_2 "SELECT count(), min(k) FROM t_05346_auth WHERE s LIKE '100099%'" ", max_http_get_redirects = 1"

echo "--- request headers of a named collection"
# The headers of a table can change while it keeps its UUID: here the header of the named collection is
# changed and the table reattached. `token.parquet` then serves another file with the same size and
# modification time, so the entries recorded for the file of one header must not apply to the other.
NC="nc_${CLICKHOUSE_TEST_UNIQUE_NAME}"
${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION IF EXISTS ${NC}"
${CLICKHOUSE_CLIENT} --query "CREATE NAMED COLLECTION ${NC} AS url = 'http://127.0.0.1:${HTTP_PORT}/token.parquet', format = 'Parquet',
    \`headers.header.name\` = 'X-Token', \`headers.header.value\` = 'alice'"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05346_token"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05346_token (k Int64, s String) ENGINE = URL(${NC})"
run header_1 "SELECT count(), min(k) FROM t_05346_token WHERE s LIKE '100099%'"
${CLICKHOUSE_CLIENT} --query "ALTER NAMED COLLECTION ${NC} SET \`headers.header.value\` = 'bob'"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_05346_token"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_05346_token"
run header_2 "SELECT count(), min(k) FROM t_05346_token WHERE s LIKE '100099%'"
run header_3 "SELECT count(), min(k) FROM t_05346_token WHERE s LIKE '100099%'"

echo "--- an origin without a date"
# Without the `Date` of the response the age of the modification is unknown: the cache is not used.
echo none > "${DIR}/date_skew"
touch -d '2020-01-03 00:00:00' "${DIR}/b.parquet"
run no_date_1 "SELECT count(), min(k) FROM t_05346_b WHERE s LIKE '100099%'"
run no_date_2 "SELECT count(), min(k) FROM t_05346_b WHERE s LIKE '100099%'"
rm "${DIR}/date_skew"

echo "--- the url table function"
# A table without a UUID has no cache entries.
URL="url('http://127.0.0.1:${HTTP_PORT}/a.parquet', 'Parquet')"
run function_1 "SELECT count() FROM ${URL} WHERE s LIKE '9%'"
run function_2 "SELECT count() FROM ${URL} WHERE s LIKE '9%'"

echo "--- query condition cache lookups"
events where_1 where_2 where_top_k top_k_1 top_k_plain rewritten_1 rewritten_2 policy after_policy recent_1 recent_2 skew_1 skew_2 redirect_1 redirect_2 redirect_3 auth_1 auth_2 header_1 header_2 header_3 no_date_1 no_date_2 function_1 function_2

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05346"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05346_b"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05346_c"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05346_latest"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05346_auth"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05346_token"
${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION ${NC}"
