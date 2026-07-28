#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: `Lance` requires the Rust build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

. "${CUR_DIR}/data_lance/run_local_test.sh"

fixture_dir="${CLICKHOUSE_USER_FILES_UNIQUE}/data_lance"
error_file="${CLICKHOUSE_TMP}/lance_${CLICKHOUSE_TEST_UNIQUE_NAME}_prewhere.stderr"
rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE:?}"
mkdir -p "${fixture_dir}"
cp -R "${CUR_DIR}/data_lance/pushdown.lance" "${fixture_dir}/"

cleanup_prewhere()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS lance_local_prewhere_capability" >/dev/null 2>&1 || :
    rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE:?}"
    rm -f "${error_file}"
}
trap cleanup_prewhere EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE lance_local_prewhere_capability
    ENGINE = LanceLocal('${fixture_dir}/pushdown.lance')"

if ${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM lance_local_prewhere_capability
    PREWHERE id >= 2
    FORMAT Null" 2>"${error_file}"
then
    echo "Lance unexpectedly accepted PREWHERE" >&2
    exit 1
fi
grep -q '(ILLEGAL_PREWHERE)' "${error_file}"

cleanup_prewhere
trap - EXIT
run_lance_local_test "04633_lance_local_prewhere_rls"
