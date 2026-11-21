#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# Operator-side controls over `allow_delta_lake_writes`: a profile constraint that pins it off (the kill
# switch) cannot be undone by SET, by the alias or by a SETTINGS clause, and read-only users cannot write
# even with the setting on. Every rejection leaves the table untouched and readable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_delta_constraints"
TABLE="${ROOT}/t"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"

mkdir -p "${TABLE}/_delta_log"
cat > "${TABLE}/_delta_log/00000000000000000000.json" <<EOF
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}-constraints","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1700000000000}}
EOF

state() {
    echo "versions: $(($(find "${TABLE}/_delta_log" -name '*.json' | wc -l | tr -d ' ') - 1)), data files: $(find "${TABLE}" -name '*.parquet' | wc -l | tr -d ' ')"
}

KILL_SWITCH_USER="${CLICKHOUSE_TEST_UNIQUE_NAME}_kill_switch"
KILL_SWITCH_PROFILE="${CLICKHOUSE_TEST_UNIQUE_NAME}_kill_switch_profile"
READONLY_USER="${CLICKHOUSE_TEST_UNIQUE_NAME}_readonly"
READONLY2_USER="${CLICKHOUSE_TEST_UNIQUE_NAME}_readonly2"
# The plain client binary: the harness client injects settings (log_comment) a readonly = 1 user cannot set.
RAW_CLIENT="${CLICKHOUSE_CLIENT_BINARY} --host=${CLICKHOUSE_HOST} --port=${CLICKHOUSE_PORT_TCP} --database=${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "CREATE TABLE dl (id Int32) ENGINE = DeltaLakeLocal('${TABLE}')"
$CLICKHOUSE_CLIENT --allow_delta_lake_writes=1 --query "INSERT INTO dl VALUES (1)"
$CLICKHOUSE_CLIENT --query "
    DROP USER IF EXISTS ${KILL_SWITCH_USER}, ${READONLY_USER}, ${READONLY2_USER};
    DROP SETTINGS PROFILE IF EXISTS ${KILL_SWITCH_PROFILE};

    -- The Cloud kill switch: the setting is pinned off for the user, whatever the default profile says.
    CREATE SETTINGS PROFILE ${KILL_SWITCH_PROFILE} SETTINGS allow_delta_lake_writes = 0 READONLY;
    CREATE USER ${KILL_SWITCH_USER} SETTINGS PROFILE ${KILL_SWITCH_PROFILE};
    -- Read-only users with the writes setting on.
    CREATE USER ${READONLY_USER} SETTINGS allow_delta_lake_writes = 1, readonly = 1;
    CREATE USER ${READONLY2_USER} SETTINGS allow_delta_lake_writes = 0, readonly = 2;
    GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.dl TO ${KILL_SWITCH_USER}, ${READONLY_USER}, ${READONLY2_USER};
"
state

echo "-- kill switch: SET, the alias and a SETTINGS clause on the INSERT all hit the constraint"
$CLICKHOUSE_CLIENT --user "${KILL_SWITCH_USER}" --query "SET allow_delta_lake_writes = 1" 2>&1 | grep -o "SETTING_CONSTRAINT_VIOLATION" | sort -u
$CLICKHOUSE_CLIENT --user "${KILL_SWITCH_USER}" --query "SET allow_experimental_delta_lake_writes = 1" 2>&1 | grep -o "SETTING_CONSTRAINT_VIOLATION" | sort -u
$CLICKHOUSE_CLIENT --user "${KILL_SWITCH_USER}" --query "INSERT INTO dl SETTINGS allow_delta_lake_writes = 1 VALUES (2)" 2>&1 | grep -o "SETTING_CONSTRAINT_VIOLATION" | sort -u
echo "-- kill switch: a plain INSERT is rejected by the writes gate, SELECT still works"
$CLICKHOUSE_CLIENT --user "${KILL_SWITCH_USER}" --query "INSERT INTO dl VALUES (2)" 2>&1 | grep -o "SUPPORT_IS_DISABLED" | sort -u
$CLICKHOUSE_CLIENT --user "${KILL_SWITCH_USER}" --query "SELECT count() FROM dl"
state

echo "-- readonly = 1 with the writes setting on: INSERT is rejected as read-only"
${RAW_CLIENT} --user="${READONLY_USER}" --query "INSERT INTO dl VALUES (3)" 2>&1 | grep -o "READONLY" | sort -u
${RAW_CLIENT} --user="${READONLY_USER}" --query "SELECT count() FROM dl"
state

echo "-- readonly = 2 may change the setting, INSERT is still rejected as read-only"
${RAW_CLIENT} --user="${READONLY2_USER}" --query "SET allow_delta_lake_writes = 1; SELECT getSetting('allow_delta_lake_writes')"
${RAW_CLIENT} --user="${READONLY2_USER}" --query "SET allow_delta_lake_writes = 1; INSERT INTO dl VALUES (4)" 2>&1 | grep -o "READONLY" | sort -u
state

echo "-- the table is intact: only the initial row"
$CLICKHOUSE_CLIENT --query "SELECT id FROM dl"

$CLICKHOUSE_CLIENT --query "
    DROP USER ${KILL_SWITCH_USER}, ${READONLY_USER}, ${READONLY2_USER};
    DROP SETTINGS PROFILE ${KILL_SWITCH_PROFILE};
    DROP TABLE dl;
"
