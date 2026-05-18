#!/usr/bin/env bash

# Tags: no-replicated-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Base URL without query string (the trailing "?<params>" is stripped).
BASE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
DB="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${DB}.hits"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.hits (a UInt32, b String) ENGINE=Memory"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DB}.hits VALUES (1,'one'),(2,'two'),(3,'three')"

# Helper to issue a curl GET and print the response body.
http_get() {
    curl -sS "$@"
}

echo "===== http_allow_table_as_file ====="
echo "-- /hits"
http_get "${BASE_URL}/${DB}/hits"
echo "-- /hits.JSONEachRow"
http_get "${BASE_URL}/${DB}/hits.JSONEachRow"
echo "-- /hits.CSV"
http_get "${BASE_URL}/${DB}/hits.CSV"

echo "-- /hits.CSV.gz (compressed payload, decompressed):"
http_get -o /tmp/04245_hits.csv.gz "${BASE_URL}/${DB}/hits.CSV.gz" && zcat /tmp/04245_hits.csv.gz

echo "===== http_allow_database_as_path ====="
echo "-- /<db>/hits"
http_get "${BASE_URL}/${DB}/hits"

echo "===== query construction: limit/offset/page ====="
echo "-- limit=2"
http_get "${BASE_URL}/${DB}/hits?limit=2"
echo "-- limit=1&offset=1"
http_get "${BASE_URL}/${DB}/hits?limit=1&offset=1"
echo "-- limit=2&page=2"
http_get "${BASE_URL}/${DB}/hits?limit=2&page=2"
echo "-- limit=-1 (negative: last row)"
http_get "${BASE_URL}/${DB}/hits?limit=-1"
echo "-- limit=0.5 (fractional: half of the rows)"
http_get "${BASE_URL}/${DB}/hits?limit=0.5"
echo "-- limit=0.5&offset=0.5 (fractional limit and offset)"
http_get "${BASE_URL}/${DB}/hits?limit=0.5&offset=0.5"
echo "-- limit=1&page=-1 (negative page: last page)"
http_get "${BASE_URL}/${DB}/hits?limit=1&page=-1"
echo "-- limit=1&page=-2 (negative page: second-to-last)"
http_get "${BASE_URL}/${DB}/hits?limit=1&page=-2"

echo "===== query construction: select, order, sort, filter ====="
echo "-- select=a"
http_get "${BASE_URL}/${DB}/hits?select=a"
echo "-- order=-a"
http_get "${BASE_URL}/${DB}/hits?order=-a"
echo "-- sort=-a"
http_get "${BASE_URL}/${DB}/hits?sort=-a"
echo "-- sort=a (default ASC)"
http_get "${BASE_URL}/${DB}/hits?sort=a"
echo "-- sort=1,-2 (positional: column 1 ASC, column 2 DESC)"
http_get "${BASE_URL}/${DB}/hits?sort=1,-2"
echo "-- sort=-1 (positional: column 1 DESC)"
http_get "${BASE_URL}/${DB}/hits?sort=-1"
echo "-- filter=a>1"
http_get "${BASE_URL}/${DB}/hits?filter=a%3E1"
echo "-- filter=a>1&filter=b='two' (multiple)"
http_get "${BASE_URL}/${DB}/hits?filter=a%3E1&filter=b%3D%27two%27"

echo "===== http_allow_filters_as_unrecognized_url_parameters ====="
# This feature is intentionally NOT enabled in the global stateless-test profile (it would
# turn typos like `?profile=` or a misspelt setting name into a SQL `WHERE` filter and break
# tests such as `02152_invalid_setting_with_hints_in_http_request`). Enable it per-request
# for the cases below.
UNREC="http_allow_filters_as_unrecognized_url_parameters=1"
echo "-- /hits?a=2"
http_get "${BASE_URL}/${DB}/hits?${UNREC}&a=2"
echo "-- /hits?a!=2  (HTMLForm splits '=' off the '!=' operator; we reassemble)"
http_get "${BASE_URL}/${DB}/hits?${UNREC}&a!=2"
echo "-- /hits?a>=2 (operator's '=' is the form separator)"
http_get "${BASE_URL}/${DB}/hits?${UNREC}&a>=2"
echo "-- /hits?a<>2 (operator survives intact when there's no '=' at all)"
http_get "${BASE_URL}/${DB}/hits?${UNREC}&a<>2"

echo "===== http_allow_filters_as_path ====="
echo "-- /a=2/hits"
http_get "${BASE_URL}/${DB}/a=2/hits"
echo "-- /a>=2/hits (URL-encoded)"
http_get "${BASE_URL}/${DB}/a%3E%3D2/hits"
echo "-- /a<3/hits (URL-encoded)"
http_get "${BASE_URL}/${DB}/a%3C3/hits"
echo "-- /a!=2/hits (URL-encoded)"
http_get "${BASE_URL}/${DB}/a%21%3D2/hits"

echo "===== format / output_format / input_format / default_format ====="
echo "-- format URL setting wins over path file extension (path=CSV, format=JSONEachRow):"
http_get "${BASE_URL}/${DB}/hits.CSV?format=JSONEachRow"
echo "-- default_format setting alone (no FORMAT clause, no path):"
http_get "${BASE_URL}/?query=SELECT%201&default_format=CSV"
echo "-- output_format wins over default_format:"
http_get "${BASE_URL}/?query=SELECT%201&default_format=CSV&output_format=JSONEachRow"
echo "-- explicit default_format wins over path file extension (path .CSV, default_format=JSONEachRow):"
http_get "${BASE_URL}/${DB}/hits.CSV?default_format=JSONEachRow"

echo "===== compression setting ====="
echo "-- compression=gz on /?query (decompressed):"
http_get -o /tmp/04245_compress.gz "${BASE_URL}/?query=SELECT+1&compression=gz" && zcat /tmp/04245_compress.gz

echo "===== Content-Disposition: attachment for binary/compressed ====="
echo "-- /hits.Parquet (binary):"
http_get -D - -o /dev/null "${BASE_URL}/${DB}/hits.Parquet" | grep -i "^Content-Disposition" | tr -d '\r'
echo "-- /hits.CSV.gz (compressed):"
http_get -D - -o /dev/null "${BASE_URL}/${DB}/hits.CSV.gz" | grep -i "^Content-Disposition" | tr -d '\r'
echo "-- /?compression=gz&query=... (compressed, no filename in path):"
http_get -D - -o /dev/null "${BASE_URL}/?query=SELECT+1&compression=gz" | grep -i "^Content-Disposition" | tr -d '\r'
echo "-- /?query=SELECT+1 (text TabSeparated, no disposition):"
http_get -D - -o /dev/null "${BASE_URL}/?query=SELECT+1" | grep -i "^Content-Disposition" | tr -d '\r'
echo "(expected: no Content-Disposition above)"

echo "===== implicit_table_at_top_level ====="
echo "-- /hits?query=SELECT+a+(no FROM)"
http_get "${BASE_URL}/${DB}/hits?query=SELECT+a"
echo "-- /hits.csv?query=SELECT+a+(no FROM, format from path)"
http_get "${BASE_URL}/${DB}/hits.CSV?query=SELECT+a"

echo "===== compose: path-table + URL filters + order ====="
echo "-- /hits?filter=a>1&order=-a"
http_get "${BASE_URL}/${DB}/hits?filter=a%3E1&order=-a"

echo "===== error cases ====="
echo "-- page without limit:"
http_get "${BASE_URL}/${DB}/hits?page=2" 2>&1 | grep -oE "Setting .page. requires .limit. to be set" | head -1
echo "-- page with offset:"
http_get "${BASE_URL}/${DB}/hits?limit=2&offset=1&page=2" 2>&1 | grep -oE "Setting .page. cannot be combined with .offset." | head -1
echo "-- sort and order together:"
http_get "${BASE_URL}/${DB}/hits?sort=a&order=a" 2>&1 | grep -oE "Settings .sort. and .order. cannot be specified together" | head -1
echo "-- compression conflict (path .gz vs setting br):"
http_get "${BASE_URL}/${DB}/hits.CSV.gz?compression=br" 2>&1 | grep -oE "Conflicting compression" | head -1
echo "-- sort with space:"
http_get "${BASE_URL}/${DB}/hits?sort=a%20b" 2>&1 | grep -oE "Invalid character . . in identifier" | head -1
echo "-- compression without format in path:"
http_get "${BASE_URL}/${DB}/hits.unknownformat.gz" 2>&1 | grep -oE "Unknown format" | head -1

echo "===== per-user disable of path features ====="
echo "-- with all http_allow_* disabled per-request, the path is ignored (no implicit-table parse)"
http_get "${BASE_URL}/${DB}/hits?http_allow_database_as_path=0&http_allow_table_as_file=0&http_allow_filters_as_path=0&http_allow_filters_as_unrecognized_url_parameters=0&query=SELECT+42"

echo "===== url_prefix: handler mounted at /api/v1 strips the prefix ====="
echo "-- /api/v1/<db>/hits"
http_get "${BASE_URL}/api/v1/${DB}/hits"
echo "-- /api/v1/<db>/hits.CSV"
http_get "${BASE_URL}/api/v1/${DB}/hits.CSV"
echo "-- /api/v1/<db>/hits?filter=a>1"
http_get "${BASE_URL}/api/v1/${DB}/hits?filter=a%3E1"
echo "-- /api/v1?query=SELECT+1 (prefix alone, with query param)"
http_get "${BASE_URL}/api/v1?query=SELECT+1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${DB}.hits"

rm -f /tmp/04245_hits.csv.gz /tmp/04245_compress.gz
