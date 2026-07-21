#!/usr/bin/env bash
# Tags: no-fasttest
# Test: exercises the silent-return branch in StorageFileLog constructor when the
#   absolute data path is outside `user_files_path` and the loading mode is
#   >= SECONDARY_CREATE.
# Covers: src/Storages/FileLog/StorageFileLog.cpp:195-198 — the
#   `LOG_ERROR(...); return;` path under `if (LoadingStrictnessLevel::SECONDARY_CREATE <= mode)`.
# The CREATE-mode throw path is already covered by 02125_fix_storage_filelog.sql /
# 02126_fix_filelog.sh. The silent-return path used by ATTACH/SECONDARY_CREATE/
# FORCE_ATTACH/FORCE_RESTORE has zero coverage in 0_stateless or integration.

set -eu

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

UUID=$(${CLICKHOUSE_CLIENT} --query "SELECT generateUUIDv4()")

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS filelog_bad_path_attach SYNC;"

# ATTACH TABLE with full table definition triggers LoadingStrictnessLevel::ATTACH (>= SECONDARY_CREATE),
# so the absolute path /tmp/... (outside `user_files_path`) hits the LOG_ERROR + return
# branch in the constructor instead of throwing BAD_ARGUMENTS. The table is registered
# in `system.tables`. Suppress LOG_ERROR / Warning lines on stderr — they are expected.
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE filelog_bad_path_attach UUID '${UUID}' (k UInt8, v UInt8) ENGINE = FileLog('/tmp/nonexistent_4202_pr62421.csv', 'CSV');" 2>/dev/null

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'filelog_bad_path_attach';"
${CLICKHOUSE_CLIENT} --query "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'filelog_bad_path_attach';"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS filelog_bad_path_attach SYNC;"
