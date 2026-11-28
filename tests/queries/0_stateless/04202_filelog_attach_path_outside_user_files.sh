#!/usr/bin/env bash
# Tags: no-fasttest
# A FileLog table whose stored definition has a path outside `user_files_path` still loads (it stays
# registered, but does not read anything), while a new definition with such a path is rejected,
# also by a full-definition ATTACH.

set -eu

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# An `Atomic` or `Replicated` database attaches a table with a full definition only with its UUID; `Ordinary` has none.
UUID_CLAUSE=""
if [ "$(${CLICKHOUSE_CLIENT} --query "SELECT engine FROM system.databases WHERE name = currentDatabase()")" != "Ordinary" ]; then
    UUID_CLAUSE="UUID '$(${CLICKHOUSE_CLIENT} --query "SELECT generateUUIDv4()")'"
fi

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS filelog_bad_path_attach SYNC;"

${CLICKHOUSE_CLIENT} --query "ATTACH TABLE filelog_bad_path_attach ${UUID_CLAUSE} (k UInt8, v UInt8) ENGINE = FileLog('/tmp/nonexistent_4202_pr62421.csv', 'CSV'); -- { serverError BAD_ARGUMENTS }"

# The table is stored in a clickhouse-local data directory with a valid relative path, its stored
# definition is rewritten to the outside path, and the next start loads it. Error lines on stderr are expected.
cd "${CLICKHOUSE_TMP:?}"
echo '1,1' > "${CLICKHOUSE_TMP}/filelog.csv"
$CLICKHOUSE_LOCAL --path="${CLICKHOUSE_TMP:?}/04202_data" -q "CREATE TABLE filelog_bad_path_attach (k UInt8, v UInt8) ENGINE = FileLog('filelog.csv', 'CSV')"
$CLICKHOUSE_LOCAL -q "
    SET engine_file_truncate_on_insert = 1;
    INSERT INTO FUNCTION file('${CLICKHOUSE_TMP:?}/04202_data.sql', 'RawBLOB')
        SELECT replaceOne(raw_blob, '\'filelog.csv\'', '\'/tmp/nonexistent_4202_pr62421.csv\'') FROM file('${CLICKHOUSE_TMP:?}/04202_data/metadata/default/filelog_bad_path_attach.sql', 'RawBLOB');
    INSERT INTO FUNCTION file('${CLICKHOUSE_TMP:?}/04202_data/metadata/default/filelog_bad_path_attach.sql', 'RawBLOB') SELECT * FROM file('${CLICKHOUSE_TMP:?}/04202_data.sql', 'RawBLOB')"

$CLICKHOUSE_LOCAL --path="${CLICKHOUSE_TMP:?}/04202_data" -q "
    SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'filelog_bad_path_attach';
    SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'filelog_bad_path_attach';" 2>/dev/null
rm -rf "${CLICKHOUSE_TMP:?}/04202_data" "${CLICKHOUSE_TMP:?}/04202_data.sql" "${CLICKHOUSE_TMP:?}/filelog.csv"
