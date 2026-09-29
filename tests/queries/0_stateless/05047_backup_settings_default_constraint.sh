#!/usr/bin/env bash

# A core `name = DEFAULT` in a BACKUP/RESTORE clause is checked as `SET name = DEFAULT` is; a specific
# one is not.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
uniq="${CLICKHOUSE_TEST_UNIQUE_NAME}"
profile="profile_${CLICKHOUSE_TEST_UNIQUE_NAME}"

# A size limit is constrained, not a time limit: a time limit would also bound the RESTORE below and fail it on slow storage.
${CLICKHOUSE_CLIENT} -m --query "
DROP USER IF EXISTS $user;
DROP SETTINGS PROFILE IF EXISTS $profile;
CREATE SETTINGS PROFILE $profile SETTINGS max_query_size = 1000 CONST;
CREATE TABLE src (a Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO src SELECT * FROM numbers(10);
CREATE USER $user IDENTIFIED WITH no_password SETTINGS max_query_size = 1000 CONST;
GRANT ALL ON *.* TO $user;
"

# The backup the RESTORE cases read, made by the unconstrained test user.
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE src TO Disk('backups', '${uniq}_src') FORMAT Null"

# `rejected` only for a constraint violation; any other failure prints its code.
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

# Over HTTP, because the native client applies a `profile` item of the clause itself, and it does not know
# this profile.
run_over_http() {
    local out
    out=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "$1" 2>&1)
    if ! echo "$out" | grep -q 'Code: '
    then
        echo "accepted"
    elif echo "$out" | grep -q 'SETTING_CONSTRAINT_VIOLATION'
    then
        echo "rejected"
    else
        echo "failed: $(echo "$out" | grep -oE 'Code: [0-9]+' | head -1)"
    fi
}

echo "-- The reference behavior of the same reset outside BACKUP/RESTORE"
run_as_constrained_user "SELECT 1 FORMAT Null"
run_as_constrained_user "SELECT 1 SETTINGS max_query_size = DEFAULT FORMAT Null"
run_as_constrained_user "SET max_query_size = DEFAULT"

echo "-- BACKUP/RESTORE resets a core setting on the same context, so it is checked the same way"
run_as_constrained_user "BACKUP TABLE src TO Disk('backups', '${uniq}_b1') SETTINGS max_query_size = DEFAULT FORMAT Null"
run_as_constrained_user "RESTORE TABLE src AS r1 FROM Disk('backups', '${uniq}_src') SETTINGS max_query_size = DEFAULT FORMAT Null"

echo "-- Controls: the check rejects the violation only, not every clause and not every reset"
run_as_constrained_user "BACKUP TABLE src TO Disk('backups', '${uniq}_b2') SETTINGS id = '${uniq}_b2' FORMAT Null"
run_as_constrained_user "BACKUP TABLE src TO Disk('backups', '${uniq}_b3') SETTINGS max_threads = DEFAULT FORMAT Null"
run_as_constrained_user "BACKUP TABLE src TO Disk('backups', '${uniq}_b4') SETTINGS max_query_size = 1000 FORMAT Null"

echo "-- A BACKUP/RESTORE-specific reset is resolved in the settings layer and never reaches the context"
run_as_constrained_user "BACKUP TABLE src TO Disk('backups', '${uniq}_b5') SETTINGS compression_method = DEFAULT FORMAT Null"
run_as_constrained_user "RESTORE TABLE src AS r2 FROM Disk('backups', '${uniq}_src') SETTINGS structure_only = DEFAULT FORMAT Null"

echo "-- A profile set in the clause installs its constraints for the resets after it, as it does in SET"
run_over_http "SELECT 1 SETTINGS profile = '$profile', max_query_size = DEFAULT FORMAT Null"
run_over_http "BACKUP TABLE src TO Disk('backups', '${uniq}_b6') SETTINGS profile = '$profile', max_query_size = DEFAULT FORMAT Null"
run_over_http "RESTORE TABLE src AS r3 FROM Disk('backups', '${uniq}_src') SETTINGS profile = '$profile', max_query_size = DEFAULT FORMAT Null"
run_over_http "BACKUP TABLE src TO Disk('backups', '${uniq}_b7') SETTINGS profile = '$profile' FORMAT Null"

echo "-- The last item for a setting wins, also when it restores the value the setting had before the query"
run_over_http "BACKUP TABLE src TO Disk('backups', '${uniq}_b8') SETTINGS timeout_overflow_mode = 'throw', max_execution_time = 0.000001, max_execution_time = 0 FORMAT Null"
run_over_http "BACKUP TABLE src TO Disk('backups', '${uniq}_b9') SETTINGS timeout_overflow_mode = 'throw', max_execution_time = 0.000001 FORMAT Null"

echo "-- Neither rejected query left a table behind, and the accepted restore did its work"
${CLICKHOUSE_CLIENT} --query "
SELECT count(), countIf(name = 'r2') FROM system.tables WHERE database = currentDatabase() AND name IN ('r1', 'r2', 'r3')
"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM r2"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE r2;
DROP TABLE src;
DROP USER $user;
DROP SETTINGS PROFILE $profile;
"
