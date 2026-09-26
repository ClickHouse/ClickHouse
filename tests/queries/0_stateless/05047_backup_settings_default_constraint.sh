#!/usr/bin/env bash

# A `SETTINGS <name> = DEFAULT` item of a BACKUP/RESTORE query is resolved by setting class: a
# BACKUP/RESTORE-specific name loses its override in the settings layer, while a core name is reset on the
# query context. The reset is a settings change like any other, so it must pass the same constraint check
# that `SET <name> = DEFAULT` and `SELECT ... SETTINGS <name> = DEFAULT` pass, and the specific names must
# stay unaffected by a constraint on a core setting.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
uniq="${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -m --query "
DROP USER IF EXISTS $user;
CREATE TABLE src (a Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO src SELECT * FROM numbers(10);
CREATE USER $user IDENTIFIED WITH no_password SETTINGS max_execution_time = 10 CONST;
GRANT ALL ON *.* TO $user;
"

# The backup the RESTORE cases read, made by the unconstrained test user.
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE src TO Disk('backups', '${uniq}_src') FORMAT Null"

# `rejected` only for a constraint violation: any other failure is reported with its code, so a query that
# breaks for an unrelated reason cannot be mistaken for the constraint doing its job.
run_as_constrained_user() {
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "$user" --query "$1" 2>&1)
    if [ $? -eq 0 ]
    then
        echo "accepted"
    elif echo "$out" | grep -q 'SETTING_CONSTRAINT_VIOLATION'
    then
        echo "rejected"
    else
        echo "unexpected: $(echo "$out" | grep -oE 'Code: [0-9]+' | head -1)"
    fi
}

echo "-- The reference behavior of the same reset outside BACKUP/RESTORE"
run_as_constrained_user "SELECT 1 FORMAT Null"
run_as_constrained_user "SELECT 1 SETTINGS max_execution_time = DEFAULT FORMAT Null"
run_as_constrained_user "SET max_execution_time = DEFAULT"

echo "-- BACKUP/RESTORE resets a core setting on the same context, so it is checked the same way"
run_as_constrained_user "BACKUP TABLE src TO Disk('backups', '${uniq}_b1') SETTINGS max_execution_time = DEFAULT FORMAT Null"
run_as_constrained_user "RESTORE TABLE src AS r1 FROM Disk('backups', '${uniq}_src') SETTINGS max_execution_time = DEFAULT FORMAT Null"

echo "-- Controls: the check rejects the violation only, not every clause and not every reset"
run_as_constrained_user "BACKUP TABLE src TO Disk('backups', '${uniq}_b2') SETTINGS id = '${uniq}_b2' FORMAT Null"
run_as_constrained_user "BACKUP TABLE src TO Disk('backups', '${uniq}_b3') SETTINGS max_threads = DEFAULT FORMAT Null"
run_as_constrained_user "BACKUP TABLE src TO Disk('backups', '${uniq}_b4') SETTINGS max_execution_time = 10 FORMAT Null"

echo "-- A BACKUP/RESTORE-specific reset is resolved in the settings layer and never reaches the context"
run_as_constrained_user "BACKUP TABLE src TO Disk('backups', '${uniq}_b5') SETTINGS compression_method = DEFAULT FORMAT Null"
run_as_constrained_user "RESTORE TABLE src AS r2 FROM Disk('backups', '${uniq}_src') SETTINGS structure_only = DEFAULT FORMAT Null"

echo "-- Neither rejected query left a table behind, and the accepted restore did its work"
${CLICKHOUSE_CLIENT} --query "
SELECT count(), countIf(name = 'r2') FROM system.tables WHERE database = currentDatabase() AND name IN ('r1', 'r2')
"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM r2"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE r2;
DROP TABLE src;
DROP USER $user;
"
