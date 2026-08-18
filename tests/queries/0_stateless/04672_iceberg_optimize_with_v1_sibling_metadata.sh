#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# Regression test: OPTIMIZE TABLE on an IcebergLocal table must not fail when
# a sibling metadata file in the metadata/ directory lacks the `table-uuid`
# field. The Iceberg spec makes table-uuid optional at format-version 1, so
# leftover v1 metadata files are a normal occurrence and must be silently
# skipped during UUID-based metadata selection.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (x Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}/')
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1), (2), (3)"

# Inject a sibling metadata file at a non-latest version with format-version 1
# and no table-uuid field. This simulates a leftover from before the table was
# upgraded to v2, or from an external engine that omits the optional field.
echo '{"format-version": 1, "last-updated-ms": 0}' > "${TABLE_PATH}/metadata/1-00000000-0000-0000-0000-000000000000.metadata.json"

# OPTIMIZE must succeed despite the sibling file lacking table-uuid.
${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 \
    --query "OPTIMIZE TABLE ${TABLE}" 2>&1 \
    | grep -cF 'Table UUID is not specified' | sed 's/1/FAIL: threw on missing table-uuid/;s/0/OK/'

# Data must remain intact.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE} SYNC"
