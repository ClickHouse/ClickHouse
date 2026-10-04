#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A Paimon schema whose second field uses an unsupported nested ROW type.
# Previously the parser threw a bare Exception() with code OK and no message.
# It must now report a descriptive BAD_ARGUMENTS error.
TABLE_DIR="${CLICKHOUSE_USER_FILES_UNIQUE}/paimon_unsupported_type"
rm -rf "${TABLE_DIR}"
mkdir -p "${TABLE_DIR}/schema"

cat > "${TABLE_DIR}/schema/schema-0" <<'EOF'
{ "version": 3, "id": 0,
  "fields": [
    {"id": 0, "name": "c0", "type": "INT"},
    {"id": 1, "name": "c1", "type": {"type": "ROW", "fields": [{"id": 2, "name": "f0", "type": "INT"}]}}
  ],
  "highestFieldId": 2, "partitionKeys": [], "primaryKeys": [], "options": {}, "timeMillis": 1751900000000 }
EOF

${CLICKHOUSE_CLIENT} -q "SELECT * FROM paimonLocal('${TABLE_DIR}')" 2>&1 \
    | grep -q "Unsupported Paimon type: ROW.*(BAD_ARGUMENTS)" && echo "OK: descriptive error"

rm -rf "${TABLE_DIR}"
