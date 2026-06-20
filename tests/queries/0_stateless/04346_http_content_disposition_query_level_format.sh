#!/usr/bin/env bash

# Tags: no-replicated-database

# Regression test: the `Content-Disposition` attachment filename must reflect the *effective* output
# format. A query-level `SETTINGS output_format = …` / `format = …` is applied inside `executeQuery`,
# after the HTTP handler captured the URL/profile format override, so it is only visible via the
# effective format of the result. The filename must still honor the "explicit override wins over the
# path extension" contract — i.e. `/db/hits.CSV?query=... SETTINGS output_format='Native'` must send a
# `hits.Native` filename, not the stale `hits.CSV` from the path.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
DB="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${DB}.hits"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.hits (a UInt32, b String) ENGINE=Memory"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DB}.hits VALUES (1,'one')"

disp() { curl -sS -D - -o /dev/null "$@" | grep -i "^Content-Disposition" | tr -d '\r'; }

echo "-- query-level SETTINGS output_format='Native' overrides the path .CSV extension in the filename"
disp "${BASE_URL}/${DB}/hits.CSV?query=SELECT%20*%20FROM%20hits%20SETTINGS%20output_format%3D%27Native%27"

echo "-- query-level SETTINGS format='Native' (the generic format setting) is reflected too"
disp "${BASE_URL}/${DB}/hits.CSV?query=SELECT%20*%20FROM%20hits%20SETTINGS%20format%3D%27Native%27"

echo "-- URL-level output_format override still wins over the path extension (unchanged behavior)"
disp "${BASE_URL}/${DB}/hits.CSV?output_format=Native"

echo "-- no override: the path .Native extension is kept as-is"
disp "${BASE_URL}/${DB}/hits.Native"

echo "-- compression alias 'gzip' does not duplicate the path's '.gz' extension"
disp "${BASE_URL}/${DB}/hits.Native.gz?compression=gzip"

echo "-- compression alias 'gzip' on a path without a compression extension is canonicalized to '.gz'"
disp "${BASE_URL}/${DB}/hits.Native?compression=gzip"

echo "-- compression alias 'zstd' is canonicalized to '.zst'"
disp "${BASE_URL}/${DB}/hits.Native?compression=zstd"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${DB}.hits"
