#!/usr/bin/env bash
# Tags: no-fasttest
# A DateTime text value read from SQLite must not have trailing characters silently dropped
# (e.g. '2024-01-01 12:00:00 junk' truncated to the timestamp). A fractional-seconds tail is
# tolerated, because SQLite commonly stores it and a plain DateTime has no use for it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH=${CLICKHOUSE_TMP}/04813_${CLICKHOUSE_DATABASE}.sqlite
rm -f "${DB_PATH}"

sqlite3 "${DB_PATH}" "CREATE TABLE good (d TEXT); INSERT INTO good VALUES ('2024-01-01 12:00:00'), ('2024-01-01 12:00:00.123');"
sqlite3 "${DB_PATH}" "CREATE TABLE bad (d TEXT); INSERT INTO bad VALUES ('2024-01-01 12:00:00 junk');"
# A dot with no digit after it is not a fractional-seconds tail and must not be waved through.
sqlite3 "${DB_PATH}" "CREATE TABLE bare_dot (d TEXT); INSERT INTO bare_dot VALUES ('2024-01-01 12:00:00.');"

${CLICKHOUSE_LOCAL} --query "CREATE TABLE s (d DateTime('UTC')) ENGINE = SQLite('${DB_PATH}', 'good'); SELECT * FROM s ORDER BY ALL"
${CLICKHOUSE_LOCAL} --query "CREATE TABLE s (d DateTime('UTC')) ENGINE = SQLite('${DB_PATH}', 'bad'); SELECT * FROM s" 2>&1 | grep -o -m1 "CANNOT_PARSE_INPUT_ASSERTION_FAILED"
${CLICKHOUSE_LOCAL} --query "CREATE TABLE s (d DateTime('UTC')) ENGINE = SQLite('${DB_PATH}', 'bare_dot'); SELECT * FROM s" 2>&1 | grep -o -m1 "CANNOT_PARSE_DATETIME"

rm -f "${DB_PATH}"
