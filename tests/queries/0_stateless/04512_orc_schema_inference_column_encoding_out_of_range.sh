#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test: an ORC file whose type tree declares more columns than the stripe footer has
# ColumnEncoding entries used to crash the server. Schema inference calls getColumnEncoding for a
# STRING column (input_format_orc_dictionary_as_low_cardinality defaults to 1), which indexed the
# stripe footer out of range and aborted via an absl CHECK. It must now be rejected with a parse
# error instead.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.orc"
trap 'rm -f "$TMP_FILE"' EXIT

# Minimal hand-crafted ORC file: root STRUCT (column 0) with one STRING child (column 1), but the
# stripe footer carries only a single ColumnEncoding entry. Reading column 1's encoding is therefore
# out of range. Emitting the exact bytes keeps the test self-contained and parallel-safe.
printf '\x4f\x52\x43\x12\x02\x08\x00\x08\x03\x10\x07\x1a\x0a\x08\x03\x10\x00\x18\x00\x20\x04\x28\x00\x22\x09\x08\x0c\x12\x01\x01\x1a\x02\x73\x30\x22\x02\x08\x07\x30\x00\x40\x90\x4e\x08\x24\x10\x00\x18\x80\x80\x04\x22\x02\x00\x0c\x28\x00\x82\xf4\x03\x03\x4f\x52\x43\x15' > "$TMP_FILE"

# Pin input_format_orc_dictionary_as_low_cardinality=1 so the test always exercises the
# getColumnEncoding path regardless of randomized or future default settings.
$CLICKHOUSE_LOCAL --query "DESC TABLE file('${TMP_FILE}', ORC) SETTINGS input_format_orc_dictionary_as_low_cardinality = 1" 2>&1 \
    | grep -F -q 'CANNOT_EXTRACT_TABLE_STRUCTURE' && echo 'OK' || echo 'FAIL'
