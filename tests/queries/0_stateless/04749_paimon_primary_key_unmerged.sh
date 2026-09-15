#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Per-run directory: the flaky check runs this file concurrently with itself, and a fixed
# subdirectory of the shared user_files would have each copy re-running cp -r over the files a
# sibling copy is mid-read on.
DATA_DIR="${CLICKHOUSE_USER_FILES_UNIQUE}/data_minio"
rm -rf "${DATA_DIR}"
mkdir -p "${DATA_DIR}"
cp -r "${CUR_DIR}/data_minio/paimon_primary_key/" "${DATA_DIR}/"
cp -r "${CUR_DIR}/data_minio/paimon_no_partition/" "${DATA_DIR}/"

PK_TABLE="${DATA_DIR}/paimon_primary_key"

# Only the full-scan read mode is reachable here: the targeted and incremental modes are gated on
# the per-table setting paimon_incremental_read, which a table function cannot set. The guard sits
# above all three branches, so one arm covers them.

# A primary-key table needs merge-on-read, which is not implemented: reading it would return the
# raw union of the data files, i.e. row versions superseded by later upserts. It must throw.
out=$(${CLICKHOUSE_CLIENT} -q "SELECT * FROM paimonLocal('${PK_TABLE}') ORDER BY id, val;" 2>&1)
echo "$out" | grep -q 'merge-on-read is not implemented' \
    && echo "$out" | grep -q 'NOT_IMPLEMENTED' \
    && echo "SELECT THROWS"

# Same guard on the aggregate path -- there is no count() shortcut that bypasses it.
out=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM paimonLocal('${PK_TABLE}');" 2>&1)
echo "$out" | grep -q 'merge-on-read is not implemented' \
    && echo "$out" | grep -q 'NOT_IMPLEMENTED' \
    && echo "COUNT THROWS"

# The opt-in returns the documented-incorrect raw union: (1,'old') is superseded by (1,'new')
# yet both come back. Spark returns only "1 new" and "2 two".
${CLICKHOUSE_CLIENT} --paimon_allow_unmerged_primary_key_reads=1 \
    -q "SELECT * FROM paimonLocal('${PK_TABLE}') ORDER BY id, val;"

# compatibility restores the pre-fix behaviour wholesale.
${CLICKHOUSE_CLIENT} --compatibility=26.7 \
    -q "SELECT * FROM paimonLocal('${PK_TABLE}') ORDER BY id, val;"

# The table stays inspectable so a user can see what they have.
${CLICKHOUSE_CLIENT} -q "DESCRIBE paimonLocal('${PK_TABLE}');"

# Append-only tables have no superseded row versions and must keep reading by default.
${CLICKHOUSE_CLIENT} -q "SELECT count(1) FROM paimonLocal('${DATA_DIR}/paimon_no_partition');"

rm -rf "${DATA_DIR}"
