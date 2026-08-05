#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

mkdir -p "${USER_FILES_PATH}/data_minio/"
cp -r "${CUR_DIR}/data_minio/paimon_primary_key/" "${USER_FILES_PATH}/data_minio/"
cp -r "${CUR_DIR}/data_minio/paimon_no_partition/" "${USER_FILES_PATH}/data_minio/"

PK_TABLE="${USER_FILES_PATH}/data_minio/paimon_primary_key"

# A primary-key table needs merge-on-read, which is not implemented: reading it would return the
# raw union of the data files, i.e. row versions superseded by later upserts. It must throw.
${CLICKHOUSE_CLIENT} -q "SELECT * FROM paimonLocal('${PK_TABLE}') ORDER BY id, val;" 2>&1 \
    | grep -q 'merge-on-read is not implemented' && echo "SELECT THROWS"

# Same guard on the aggregate path -- there is no count() shortcut that bypasses it.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM paimonLocal('${PK_TABLE}');" 2>&1 \
    | grep -q 'merge-on-read is not implemented' && echo "COUNT THROWS"

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
${CLICKHOUSE_CLIENT} -q "SELECT count(1) FROM paimonLocal('${USER_FILES_PATH}/data_minio/paimon_no_partition');"
