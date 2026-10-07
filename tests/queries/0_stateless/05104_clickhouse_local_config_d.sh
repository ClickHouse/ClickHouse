#!/usr/bin/env bash
# `clickhouse-local` merges the `config.d` and `conf.d` directories of the current directory
# into the config embedded in the binary, even when there is no main config file at all.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
EMPTY_DIR="${WORKING_DIR}_empty"
rm -rf "${WORKING_DIR}" "${EMPTY_DIR}"
mkdir -p "${WORKING_DIR}/config.d" "${WORKING_DIR}/conf.d" "${EMPTY_DIR}"

cat > "${WORKING_DIR}/config.d/macros.yaml" <<'EOF'
macros:
  from_config_d: hello
EOF

cat > "${WORKING_DIR}/conf.d/settings.yaml" <<'EOF'
macros:
  from_conf_d: world
mark_cache_size: 12345678
EOF

# There is no config file in the working directory, only the merge directories.
cd "${WORKING_DIR}" || exit 1
$CLICKHOUSE_LOCAL --query "SELECT getMacro('from_config_d'), getMacro('from_conf_d')"
$CLICKHOUSE_LOCAL --query "SELECT value FROM system.server_settings WHERE name = 'mark_cache_size'"

# A directory without merge directories is unaffected.
cd "${EMPTY_DIR}" || exit 1
$CLICKHOUSE_LOCAL --query "SELECT count() FROM system.server_settings WHERE name = 'mark_cache_size' AND value = '12345678'"

cd "${CLICKHOUSE_TMP}" || exit 1
rm -rf "${WORKING_DIR}" "${EMPTY_DIR}"
