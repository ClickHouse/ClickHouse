#!/usr/bin/env bash

# A table name like `//tmp/data.csv` in a URL database with a `file://` base (the default database
# of clickhouse-local) is a POSIX absolute path with redundant leading slashes, not a scheme-relative
# URL reference: it must read the same file as `/tmp/data.csv`, matching the semantics that the
# `Filesystem` database had for absolute paths.
# https://github.com/ClickHouse/ClickHouse/pull/111512

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.csv"
printf '1,one\n2,two\n' > "${DATA_FILE}"
DATA_FILE_ABS=$(realpath "${DATA_FILE}")

echo '--- absolute path'
${CLICKHOUSE_LOCAL} -q "SELECT * FROM '${DATA_FILE_ABS}'"

echo '--- absolute path with two leading slashes'
${CLICKHOUSE_LOCAL} -q "SELECT * FROM '/${DATA_FILE_ABS}'"

echo '--- absolute path with three leading slashes'
${CLICKHOUSE_LOCAL} -q "SELECT * FROM '//${DATA_FILE_ABS}'"

echo '--- the same via an explicit URL database with a file:// base'
${CLICKHOUSE_LOCAL} -q "
CREATE DATABASE d ENGINE = URL('file://');
SELECT * FROM d.\`/${DATA_FILE_ABS}\`;
"

rm "${DATA_FILE}"
