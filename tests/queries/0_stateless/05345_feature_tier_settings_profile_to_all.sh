#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Under `allow_feature_tier = 1`, assigning a profile with an EXPERIMENTAL setting `TO ALL` or dropping a `TO ALL`
# profile changes that setting for every user and is refused; a `TO ALL` that reaches no user is allowed.
# The profiles are written before the tier is restricted, so the access storage needs a path.

dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${dir}"

${CLICKHOUSE_LOCAL} --path "${dir}" -m -q "
    CREATE USER u;
    CREATE SETTINGS PROFILE p SETTINGS enable_funnel_functions = 1;
    CREATE SETTINGS PROFILE p_all SETTINGS allow_experimental_prql_dialect = 1 TO ALL;
"

function restricted()
{
    echo -n "$1: "
    ${CLICKHOUSE_LOCAL} --path "${dir}" -q "$1" -- --allow_feature_tier=1 2>&1 \
        | grep -q -F "Changes to EXPERIMENTAL settings are disabled" && echo "refused" || echo "ok"
}

restricted "ALTER SETTINGS PROFILE p TO ALL"
restricted "ALTER SETTINGS PROFILE p TO ALL EXCEPT u, default"
restricted "DROP SETTINGS PROFILE p_all"

${CLICKHOUSE_LOCAL} --path "${dir}" -m -q "
    SHOW CREATE SETTINGS PROFILE p;
    SHOW CREATE SETTINGS PROFILE p_all;
"

rm -rf "${dir}"
