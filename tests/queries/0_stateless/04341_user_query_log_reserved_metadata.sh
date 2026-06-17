#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

test_dir="${CLICKHOUSE_TMP}/04341_user_query_log_reserved_metadata_${CLICKHOUSE_DATABASE}"

rm -rf "${test_dir}"
mkdir -p "${test_dir}/metadata/system"

echo "ATTACH DATABASE system ENGINE=Ordinary" > "${test_dir}/metadata/system.sql"

cat > "${test_dir}/metadata/system/user_query_log.sql" <<'EOF'
ATTACH TABLE system.user_query_log (x UInt8) ENGINE = Memory;
EOF

if ${CLICKHOUSE_LOCAL} --path "${test_dir}" --query "SELECT 1" 2>&1 | grep -q "reserved for the filtered user query log view"
then
    echo "metadata reserved"
else
    echo "UNEXPECTED"
fi
