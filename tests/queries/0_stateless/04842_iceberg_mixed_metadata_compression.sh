#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Arm 1: table object opened while the latest metadata file is uncompressed, then another
# writer commits a gzip-compressed successor.
T1="t1_${CLICKHOUSE_DATABASE}"
P1="${USER_FILES_PATH}/${T1}/"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${T1} (c0 Int32) ENGINE = IcebergLocal('${P1}')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${T1} VALUES (1)"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${T1}"

LATEST1=$(ls -1 "${P1}metadata" | grep -E '^v[0-9]+\.metadata\.json$' | sort -V | tail -1)
NEXT1=$(( $(echo "$LATEST1" | sed -E 's/^v([0-9]+)\..*/\1/') + 1 ))
gzip -c "${P1}metadata/${LATEST1}" > "${P1}metadata/v${NEXT1}.gz.metadata.json"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${T1}"

# Arm 2: the same, with the codecs swapped.
T2="t2_${CLICKHOUSE_DATABASE}"
P2="${USER_FILES_PATH}/${T2}/"

${CLICKHOUSE_CLIENT} --iceberg_metadata_compression_method=gzip --query "CREATE TABLE ${T2} (c0 Int32) ENGINE = IcebergLocal('${P2}')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${T2} VALUES (1)"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${T2}"

LATEST2=$(ls -1 "${P2}metadata" | grep -E '^v[0-9]+\.gz\.metadata\.json$' | sort -V | tail -1)
NEXT2=$(( $(echo "$LATEST2" | sed -E 's/^v([0-9]+)\..*/\1/') + 1 ))
gzip -cd "${P2}metadata/${LATEST2}" > "${P2}metadata/v${NEXT2}.metadata.json"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${T2}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${T1}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${T2}"
rm -rf "${P1}" "${P2}"
