#!/usr/bin/env bash
# Test: exercises `JSONColumnsWithMetadataReader::readChunkStart` with
# `input_format_json_validate_types_from_metadata=0`, hitting the else-branch
# that calls `JSONUtils::readMetadata` instead of `readMetadataAndValidateHeader`.
# Covers: src/Processors/Formats/Impl/JSONColumnsWithMetadataBlockInputFormat.cpp:27-28

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists test_jcm_novalidate"
$CLICKHOUSE_CLIENT -q "create table test_jcm_novalidate(x UInt32) engine=Memory"

# Default (validate=1): metadata says UInt64 but column is UInt32 → INCORRECT_DATA.
echo -ne '{"meta":[{"name":"x","type":"UInt64"}],"data":{"x":[1,2,3]}}\n' \
  | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT%20INTO%20test_jcm_novalidate%20FORMAT%20JSONColumnsWithMetadata" --data-binary @- 2>&1 \
  | grep -o "INCORRECT_DATA" | head -1

$CLICKHOUSE_CLIENT -q "select count() from test_jcm_novalidate"

# validate=0: same mismatched metadata is ignored; rows parsed using table's UInt32 column type.
echo -ne '{"meta":[{"name":"x","type":"UInt64"}],"data":{"x":[1,2,3]}}\n' \
  | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&input_format_json_validate_types_from_metadata=0&query=INSERT%20INTO%20test_jcm_novalidate%20FORMAT%20JSONColumnsWithMetadata" --data-binary @-

$CLICKHOUSE_CLIENT -q "select * from test_jcm_novalidate order by x"

$CLICKHOUSE_CLIENT -q "drop table test_jcm_novalidate"
