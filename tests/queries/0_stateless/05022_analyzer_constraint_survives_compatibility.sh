#!/usr/bin/env bash

# The Stress test and the AST fuzzer keep the analyzer enabled with a profile that both pins
# `enable_analyzer = 1` and constrains it to `MIN 1` (see `tests/docker_scripts/stress_tests.lib`
# and `ci/jobs/scripts/fuzzer/query-fuzzer-tweaks-users.xml`). Both halves are needed and both have
# regressed before, so pin the behaviour here:
#   - the constraint rejects every explicit attempt to disable the analyzer, including through the
#     `allow_experimental_analyzer` alias;
#   - it must still accept a no-op `enable_analyzer = 1` sent together with `compatibility`, which is
#     what a `CONST`/`<readonly/>` constraint would reject;
#   - the pinned value keeps the analyzer enabled under a `compatibility` older than 24.3, which the
#     constraint alone cannot do because such a revert is not an explicit setting change.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER="u_05022_${CLICKHOUSE_DATABASE}"
PROFILE="p_05022_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP PROFILE IF EXISTS ${PROFILE}"
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PROFILE} SETTINGS enable_analyzer = 1 MIN 1"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER} SETTINGS PROFILE '${PROFILE}'"

# Explicitly disabling the analyzer is rejected, whichever way it is spelled.
# The alias is checked server-side only: on the client command line it collapses into whatever
# `enable_analyzer` the harness already passes, so there would be no change left for the server to
# reject (the analyzer stays enabled either way).
${CLICKHOUSE_CLIENT} --user="${USER}" --enable_analyzer=0 -q "SELECT 1" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
${CLICKHOUSE_CLIENT} --user="${USER}" -q "SET allow_experimental_analyzer = 0" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
${CLICKHOUSE_CLIENT} --user="${USER}" -q "SET enable_analyzer = 0" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
${CLICKHOUSE_CLIENT} --user="${USER}" -q "SELECT 1 SETTINGS enable_analyzer = 0" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
${CLICKHOUSE_CLIENT} --user="${USER}" -q "CREATE VIEW ${CLICKHOUSE_DATABASE}.v_05022 AS SELECT 1 SETTINGS enable_analyzer = 0" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1

# A no-op explicit value alongside `compatibility` is accepted: `compatibility` stops the constraint
# check from filtering unchanged settings, so this is the case a CONST constraint would break
${CLICKHOUSE_CLIENT} --user="${USER}" --compatibility='23.8' --enable_analyzer=1 -q "SELECT getSetting('enable_analyzer')"

# A pre-24.3 compatibility does not revert the pinned value
${CLICKHOUSE_CLIENT} --user="${USER}" --compatibility='23.8' -q "SELECT getSetting('enable_analyzer')"
${CLICKHOUSE_CLIENT} --user="${USER}" -q "SET compatibility = '23.8'; SELECT getSetting('enable_analyzer')"
${CLICKHOUSE_CLIENT} --user="${USER}" -q "SELECT getSetting('enable_analyzer') SETTINGS compatibility = '23.8'"

${CLICKHOUSE_CLIENT} -q "DROP USER ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP PROFILE ${PROFILE}"
