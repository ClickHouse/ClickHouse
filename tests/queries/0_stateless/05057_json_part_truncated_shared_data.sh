#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The ADVANCED shared data serialization of a JSON column keeps the paths indexes and the granule
# structure in their own files, and how much to read from them comes from the sizes stream. A file that
# ends earlier used to be read silently, leaving the paths column shorter than the offsets say, and
# reading the column then went out of bounds.

DATA_DIR=$CLICKHOUSE_TMP/part_shared_data_$CLICKHOUSE_DATABASE

# ADVANCED is used for merged parts, so two inserts and a merge are needed to get it. The serialization
# versions are pinned because the test harness randomizes them.
function create_part()
{
    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR"
    $CLICKHOUSE_LOCAL --path "$DATA_DIR" -q "
        CREATE TABLE t (json JSON(max_dynamic_paths=0)) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS min_bytes_for_wide_part = 0, object_serialization_version = 'v3', object_shared_data_serialization_version = 'advanced';
        INSERT INTO t SELECT ('{\"p' || toString(number % 8) || '\":\"' || repeat('Q', 40) || '\"}')::JSON(max_dynamic_paths=0) FROM numbers(200);
        INSERT INTO t SELECT ('{\"z' || toString(number % 8) || '\":\"' || repeat('W', 40) || '\"}')::JSON(max_dynamic_paths=0) FROM numbers(200);
        OPTIMIZE TABLE t FINAL;
    "

    PART=$(ls -d "$DATA_DIR"/store/*/*/all_1_2_1)
    # The merged part is read only if the parts it was merged from are gone.
    rm -rf "$DATA_DIR"/store/*/*/all_1_1_0 "$DATA_DIR"/store/*/*/all_2_2_0
    rm -f "$PART"/checksums.txt
}

function read_part()
{
    $CLICKHOUSE_LOCAL --path "$DATA_DIR" -q "SELECT count(), sum(length(toString(json))) FROM t" 2>&1 \
        | grep -m1 -oE "ATTEMPT_TO_READ_AFTER_EOF|CANNOT_READ_ALL_DATA"
}

create_part
# Without this check a wrong file name would be created by the truncate and the read would fail for
# another reason.
test -s "$PART"/json.object_shared_data.copy.paths_indexes.bin && echo "paths indexes stream is there"
truncate -s 0 "$PART"/json.object_shared_data.copy.paths_indexes.bin
read_part

create_part
test -s "$PART"/json.object_shared_data.0.structure.bin && echo "structure stream is there"
truncate -s 0 "$PART"/json.object_shared_data.*.structure.bin
read_part

rm -rf "$DATA_DIR"
