#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: `IcebergLocal` needs the USE_AVRO build option.
#
# An equality-delete file that names no equality field id is invalid metadata, because the ids are
# what equality is defined by. Reading such a table must be rejected.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap '${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE} SYNC"; rm -rf "${TABLE_PATH}"' EXIT

# The count is the control: the table reads correctly before the manifest is touched.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 -m --query "
    CREATE TABLE ${TABLE} (id UInt64) ENGINE = IcebergLocal('${TABLE_PATH}') ORDER BY id;
    INSERT INTO ${TABLE} SELECT number FROM numbers(10);
    DELETE FROM ${TABLE} WHERE id < 5;
    SELECT count() FROM icebergLocal('${TABLE_PATH}') SETTINGS use_iceberg_metadata_files_cache = 0;
"

# ClickHouse only writes position deletes, so relabel one: `data_file.content` 1 becomes 2. The
# manifest is uncompressed, so the entry's `file_path` appears verbatim, preceded by its Avro length
# prefix and then by `content`. The relabelled entry names no equality field id, because ClickHouse
# writes `equality_ids` as the Avro null branch, which the reader maps to an empty list.
FILE=$(ls "${TABLE_PATH}"data/*-deletes.parquet)
if [ "${#FILE}" -lt 64 ]; then LEN=1; else LEN=2; fi
read -r MANIFEST OFFSET _ <<< "$(grep -aboH -F "${FILE}" "${TABLE_PATH}"metadata/*.avro | tr : ' ')"
AT=$((OFFSET - LEN - 1))
[ "$(od -An -tx1 -j "${AT}" -N1 "${MANIFEST}" | tr -d ' ')" = 02 ] || echo 'not the content byte'
printf '\x04' | dd of="${MANIFEST}" bs=1 seek="${AT}" conv=notrunc status=none

# Both halves of the contract: a typed spec violation, and this check rather than the neighbouring
# presence check, which raises the same error code.
ERR=$(${CLICKHOUSE_CLIENT} --query \
    "SELECT count() FROM icebergLocal('${TABLE_PATH}') SETTINGS use_iceberg_metadata_files_cache = 0" 2>&1)
grep -oF ICEBERG_SPECIFICATION_VIOLATION <<< "${ERR}" | head -n1
grep -oF 'data_file.equality_ids is empty' <<< "${ERR}" | head -n1
