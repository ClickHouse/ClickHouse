#!/usr/bin/env bash
# Tags: no-fasttest

# Two Iceberg table roots can share the same `table-uuid`: copying a table's directory copies its
# metadata verbatim. The latest-metadata-version cache must therefore not be referenced by
# `table-uuid` alone on the ordinary (non-`iceberg_metadata_table_uuid`) path -- otherwise a query
# on the clone caches its own `metadata/` selection under the shared UUID, and a later query on the
# original reuses it and reads the clone's metadata file, which lives under a different table root.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/iceberg_cloned_uuid_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}/original"
trap 'rm -rf "${WORK_DIR}"' EXIT

# The original table, with a single row.
${CLICKHOUSE_LOCAL} \
    --allow_insert_into_iceberg=1 \
    --multiquery -q "
CREATE TABLE original (c0 Int) ENGINE = IcebergLocal('${WORK_DIR}/original/');
INSERT INTO original VALUES (1);
" -- --user_files_path="${WORK_DIR}"

# A byte-for-byte clone at a different path, so both roots carry the same table UUID.
cp -r "${WORK_DIR}/original" "${WORK_DIR}/clone"

# Warm the latest-version cache from the clone, then read the original in the same process, which
# is what makes both share one cache. `IF NOT EXISTS` attaches to the already-populated table root
# instead of refusing it as an existing Iceberg table. The original must see only its own row.
${CLICKHOUSE_LOCAL} \
    --allow_insert_into_iceberg=1 \
    --use_iceberg_metadata_files_cache=1 \
    --iceberg_metadata_staleness_ms=600000 \
    --multiquery -q "
CREATE TABLE IF NOT EXISTS clone (c0 Int) ENGINE = IcebergLocal('${WORK_DIR}/clone/');
INSERT INTO clone VALUES (2), (3);
SELECT 'clone', count() FROM clone;

CREATE TABLE IF NOT EXISTS original (c0 Int) ENGINE = IcebergLocal('${WORK_DIR}/original/');
SELECT 'original', c0 FROM original ORDER BY c0;
SELECT 'original again', c0 FROM original ORDER BY c0;
" -- --user_files_path="${WORK_DIR}"
