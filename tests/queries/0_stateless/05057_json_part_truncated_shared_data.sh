#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The ADVANCED shared data serialization keeps the paths indexes of a JSON column in their own file, and
# the number of indexes to read comes from the offsets. A file that ends earlier used to be read silently,
# leaving the paths column shorter than the offsets say, and reading the column then went out of bounds.

DATA_DIR=$CLICKHOUSE_TMP/part_shared_data_$CLICKHOUSE_DATABASE
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

# ADVANCED is used for merged parts, so two inserts and a merge are needed to get it. The serialization
# versions are pinned because the test harness randomizes them.
$CLICKHOUSE_LOCAL --path "$DATA_DIR" -q "
    CREATE TABLE t (json JSON(max_dynamic_paths=0)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, object_serialization_version = 'v3', object_shared_data_serialization_version = 'advanced';
    INSERT INTO t SELECT ('{\"p' || toString(number % 8) || '\":\"' || repeat('Q', 40) || '\"}')::JSON(max_dynamic_paths=0) FROM numbers(200);
    INSERT INTO t SELECT ('{\"z' || toString(number % 8) || '\":\"' || repeat('W', 40) || '\"}')::JSON(max_dynamic_paths=0) FROM numbers(200);
    OPTIMIZE TABLE t FINAL;
"

PART=$(ls -d "$DATA_DIR"/store/*/*/all_1_2_1)
INDEXES=$PART/json.object_shared_data.copy.paths_indexes.bin
# Without this, a wrong file name would be created by the truncate below and the read would fail for
# another reason.
test -s "$INDEXES" && echo "paths indexes stream is there"
truncate -s 0 "$INDEXES"
rm -f "$PART"/checksums.txt

$CLICKHOUSE_LOCAL --path "$DATA_DIR" -q "SELECT count(), sum(length(toString(json))) FROM t" 2>&1 \
    | grep -m1 -oE "ATTEMPT_TO_READ_AFTER_EOF|CANNOT_READ_ALL_DATA"

rm -rf "$DATA_DIR"
