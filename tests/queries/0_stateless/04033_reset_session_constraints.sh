#!/usr/bin/env bash
# Tags: no-random-settings
# The test deliberately tightens a `max_threads` constraint mid-flight; the
# random-settings harness's `--max_threads N` would violate it before our SQL
# even runs.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Two scenarios:
#   A. Profile constraint (`MIN`/`MAX`) survives RESET SESSION. After the
#      reset, the out-of-bounds set still fails.
#   B. If a constraint is tightened mid-session, RESET SESSION picks up the
#      new constraint immediately (the re-derive-from-access-control
#      contract).

USER="reset_const_user_${CLICKHOUSE_DATABASE}"
PROF="reset_const_prof_${CLICKHOUSE_DATABASE}"

cleanup() {
    ${CLICKHOUSE_CLIENT} -m -q "
        DROP USER IF EXISTS ${USER};
        DROP SETTINGS PROFILE IF EXISTS ${PROF};
    "
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -m -q "
    CREATE SETTINGS PROFILE ${PROF} SETTINGS max_threads MIN 1 MAX 8;
    CREATE USER ${USER} SETTINGS PROFILE ${PROF};
    GRANT SELECT, CREATE TEMPORARY TABLE ON *.* TO ${USER};
"

# Scenario A: profile constraint survives the reset.
# `SET send_logs_level='fatal'` keeps the expected `SETTING_CONSTRAINT_VIOLATION`
# server log out of the per-test client_logs_file. `RESET SESSION` clears the
# setting, so we re-`SET` it after each reset.
${CLICKHOUSE_CLIENT} --user "${USER}" -m -q "
    SET send_logs_level = 'fatal';
    SET max_threads = 7;
    SELECT 'A: pre-reset changed:', (SELECT changed FROM system.settings WHERE name = 'max_threads');
    SET max_threads = 99; -- { serverError SETTING_CONSTRAINT_VIOLATION }
    RESET SESSION;
    SET send_logs_level = 'fatal';
    SELECT 'A: post-reset changed:', (SELECT changed FROM system.settings WHERE name = 'max_threads');
    -- constraint must still apply after the reset
    SET max_threads = 99; -- { serverError SETTING_CONSTRAINT_VIOLATION }
    SET max_threads = 2;
    SELECT 'A: post-reset constrained value:', getSetting('max_threads');
"

# Scenario B: tighten the profile constraint between two sessions; `RESET SESSION`
# in the second session must re-derive from access control and pick up the new
# (stricter) bound. Demonstrating one user-session per ALTER avoids racing the
# random-settings injection inside a single session.
${CLICKHOUSE_CLIENT} --user "${USER}" -m -q "
    SET max_threads = 8;
    SELECT 'B: pre-tighten value:', getSetting('max_threads');
"

${CLICKHOUSE_CLIENT} -m -q "ALTER SETTINGS PROFILE ${PROF} SETTINGS max_threads MIN 1 MAX 2;"

${CLICKHOUSE_CLIENT} --user "${USER}" -m -q "
    SET send_logs_level = 'fatal';
    SET max_threads = 1;
    RESET SESSION;
    SET send_logs_level = 'fatal';
    -- new (tighter) constraint must be in force after reset
    SET max_threads = 5; -- { serverError SETTING_CONSTRAINT_VIOLATION }
    SET max_threads = 2;
    SELECT 'B: post-tighten max-allowed:', getSetting('max_threads');
"
