#!/usr/bin/env bash
set -euo pipefail

# Tags: no-replicated-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
DB="${CLICKHOUSE_DATABASE}"
TABLE="multipart_table_05080"
NO_INSERT_USER="multipart_no_insert_05080_${DB}"
TMP_DIR=$(mktemp -d)

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS \`${NO_INSERT_USER}\`"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${DB}.${TABLE}"
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.${TABLE} (a UInt32, b String) ENGINE=Memory"
${CLICKHOUSE_CLIENT} -q "CREATE USER \`${NO_INSERT_USER}\` IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${DB}.${TABLE} TO \`${NO_INSERT_USER}\`"

printf '1,"one"\n2,"two"\n' > "$TMP_DIR/data.csv"
printf '{"a":3,"b":"three"}\n' > "$TMP_DIR/data.json"
printf '4,"four"\n' > "$TMP_DIR/compressed.csv"
gzip -c "$TMP_DIR/compressed.csv" > "$TMP_DIR/compressed.csv.gz"
printf '5\n7\n' > "$TMP_DIR/numbers.tsv"
: > "$TMP_DIR/empty.csv"

echo "-- one CSV file with database URL parameter"
curl -fsS -F "file=@${TMP_DIR}/data.csv;filename=${TABLE}.CSV" "${BASE_URL}/?database=${DB}"
echo "-- JSONEachRow filename format"
curl -fsS -F "file=@${TMP_DIR}/data.json;filename=${TABLE}.JSONEachRow" "${BASE_URL}/?database=${DB}"
echo "-- filename with database prefix"
curl -fsS -F "file=@${TMP_DIR}/data.csv;filename=${DB}.${TABLE}.CSV" "${BASE_URL}/"
echo "-- gzip file suffix"
curl -fsS -F "file=@${TMP_DIR}/compressed.csv.gz;filename=${TABLE}.CSV.gz" "${BASE_URL}/?database=${DB}"
echo "-- rows inserted"
${CLICKHOUSE_CLIENT} -q "SELECT a, b FROM ${DB}.${TABLE} ORDER BY a, b"

echo "-- a form query after a file still uses external table data"
curl -fsS -F "data=@${TMP_DIR}/numbers.tsv;filename=numbers.TabSeparated" \
    -F 'data_structure=n UInt32' -F 'query=SELECT sum(n) FROM data' "${BASE_URL}/"
echo "-- a URL query retains its external table semantics"
curl -fsS -F "data=@${TMP_DIR}/numbers.tsv;filename=numbers.TabSeparated" \
    "${BASE_URL}/?query=SELECT%20sum%28n%29%20FROM%20data&data_structure=n%20UInt32"

echo "-- multiple uploaded files are rejected"
curl -sS -F "first=@${TMP_DIR}/data.csv;filename=${TABLE}.CSV" \
    -F "second=@${TMP_DIR}/data.csv;filename=${TABLE}.CSV" \
    "${BASE_URL}/?database=${DB}" | grep -oE 'HTTP multipart table uploads require exactly one file'
echo "-- no uploaded files are rejected"
curl -sS -F 'field=value' "${BASE_URL}/?database=${DB}" \
    | grep -oE 'HTTP multipart table uploads require exactly one file'
echo "-- empty uploaded file is rejected"
curl -sS -F "file=@${TMP_DIR}/empty.csv;filename=${TABLE}.CSV" \
    "${BASE_URL}/?database=${DB}" | grep -oE 'HTTP multipart table uploads require a non-empty file'
echo "-- missing filename format is rejected"
curl -sS -F "file=@${TMP_DIR}/data.csv;filename=${TABLE}" \
    "${BASE_URL}/?database=${DB}" | grep -oE 'HTTP multipart table uploads require a known format in the filename'
echo "-- ambiguous dotted filename is rejected"
curl -sS -F "file=@${TMP_DIR}/data.csv;filename=my.db.${TABLE}.CSV" \
    "${BASE_URL}/" | grep -oE 'Ambiguous multipart upload filename'
echo "-- conflicting databases are rejected"
curl -sS -F "file=@${TMP_DIR}/data.csv;filename=other_db.${TABLE}.CSV" \
    "${BASE_URL}/?database=${DB}" | grep -oE 'Conflicting database specification'
echo "-- missing table is rejected"
curl -sS -F "file=@${TMP_DIR}/data.csv;filename=missing_table_05080.CSV" \
    "${BASE_URL}/?database=${DB}" | grep -oE 'UNKNOWN_TABLE' | head -n1
echo "-- INSERT privilege is required"
curl -sS -F "file=@${TMP_DIR}/data.csv;filename=${TABLE}.CSV" \
    "${BASE_URL}/?database=${DB}&user=${NO_INSERT_USER}" | grep -oE 'Not enough privileges'
echo "-- readonly setting is enforced"
curl -sS -F "file=@${TMP_DIR}/data.csv;filename=${TABLE}.CSV" \
    "${BASE_URL}/?database=${DB}&readonly=1" | grep -oE 'Cannot execute query in readonly mode'
echo "-- multipart file size limit is enforced"
curl -sS -F "file=@${TMP_DIR}/data.csv;filename=${TABLE}.CSV" \
    "${BASE_URL}/?database=${DB}&http_max_multipart_form_data_size=2" \
    | grep -oE 'http_max_multipart_form_data_size'
echo "-- rejected uploads did not add rows"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${DB}.${TABLE}"
