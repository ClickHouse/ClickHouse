#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `compression` value inherited from the session (or a user profile) is a fallback, not a
# per-request override: the file extension in the URL path is more specific and must win over it,
# the same way the path extension wins over `default_format`. Only a compression supplied by the
# request itself (URL parameter or header) conflicts with the path extension.

BASE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
DB="${CLICKHOUSE_DATABASE}"
SESSION="04822_${CLICKHOUSE_DATABASE}_$RANDOM"
OUT_GZ="${CLICKHOUSE_TMP}/04822_out.gz"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${DB}.t"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.t (a UInt32) ENGINE=Memory"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DB}.t VALUES (1),(2),(3)"

echo "-- set a session-level compression default"
curl -sS "${BASE_URL}/?session_id=${SESSION}&query=SET+compression%3D%27zstd%27"

echo "-- the path extension wins over the session default (gzip payload, decompressed):"
curl -sS -o "${OUT_GZ}" "${BASE_URL}/${DB}/t.CSV.gz?session_id=${SESSION}" && gzip -dc "${OUT_GZ}"

echo "-- an explicit request parameter still conflicts:"
curl -sS "${BASE_URL}/${DB}/t.CSV.gz?session_id=${SESSION}&compression=br" 2>&1 | grep -oE "Conflicting compression" | head -1

echo "-- an explicit request parameter matching the path is not a conflict (gzip payload, decompressed):"
curl -sS -o "${OUT_GZ}" "${BASE_URL}/${DB}/t.CSV.gz?session_id=${SESSION}&compression=gzip" && gzip -dc "${OUT_GZ}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE ${DB}.t"
