#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `FORMAT SQLite` declares a `Bool` column as `INTEGER` (the storage of `Bool` is `UInt8`) and stores `0` / `1`,
# so the value keeps the `INTEGER` storage class and schema inference over the written file sees a number.

FILE="${USER_FILES_PATH}/05262_sqlite_bool_${CLICKHOUSE_DATABASE}.sqlite"
rm -f "${FILE}"
trap 'rm -f "${FILE}"' EXIT

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${FILE}', 'SQLite')
    SELECT number % 2 = 1 AS b, CAST(number % 3 = 0, 'Nullable(Bool)') AS nb, number AS n FROM numbers(4)"

sqlite3 "${FILE}" "SELECT sql FROM sqlite_master WHERE type = 'table'"
sqlite3 "${FILE}" "SELECT n, b, typeof(b), nb, typeof(nb) FROM [table] ORDER BY n"

${CLICKHOUSE_CLIENT} --query "DESCRIBE file('${FILE}', 'SQLite')"
${CLICKHOUSE_CLIENT} --query "SELECT n, b, nb FROM file('${FILE}', 'SQLite') ORDER BY n"
${CLICKHOUSE_CLIENT} --query "SELECT n, b, nb FROM file('${FILE}', 'SQLite', 'b Bool, nb Nullable(Bool), n UInt64') ORDER BY n"
