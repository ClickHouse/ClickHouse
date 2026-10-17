#!/usr/bin/env bash
# Tags: no-darwin
# no-darwin: the names of the streams of a part are hashed there, so the test cannot find the two it truncates.

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
    PART=$($CLICKHOUSE_LOCAL --path "$DATA_DIR" -q "
        CREATE TABLE t (json JSON(max_dynamic_paths=0)) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS min_bytes_for_wide_part = 0, object_serialization_version = 'v3', object_shared_data_serialization_version = 'advanced';
        INSERT INTO t SELECT ('{\"p' || toString(number % 8) || '\":\"' || repeat('Q', 40) || '\"}')::JSON(max_dynamic_paths=0) FROM numbers(200);
        INSERT INTO t SELECT ('{\"z' || toString(number % 8) || '\":\"' || repeat('W', 40) || '\"}')::JSON(max_dynamic_paths=0) FROM numbers(200);
        OPTIMIZE TABLE t FINAL;
        SELECT trim(TRAILING '/' FROM path) FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active
    ")

    if [ -z "$PART" ]
    then
        echo "the table has no active part"
        return
    fi

    # The parts the merge consumed are still on disk and are read instead if the merged one is broken.
    local part
    for part in "$(dirname "$PART")"/all_*; do
        [ "$part" = "$PART" ] || rm -rf "$part"
    done
    rm -f "$PART"/checksums.txt
}

# The stream names are matched, not spelled out, so the test does not depend on the part layout.
function truncate_streams()
{
    local label=$1
    shift

    if [ -s "$1" ]
    then
        echo "$label is there"
        truncate -s 0 "$@"
    else
        echo "$label is missing, the part contains:"
        ls "$PART" 2>&1 | head -10
    fi
}

function read_part()
{
    $CLICKHOUSE_LOCAL --path "$DATA_DIR" -q "SELECT count(), sum(length(toString(json))) FROM t" 2>&1 \
        | grep -m1 -oE "ATTEMPT_TO_READ_AFTER_EOF|CANNOT_READ_ALL_DATA"
}

create_part
truncate_streams "paths indexes stream" "$PART"/*object_shared_data*paths_indexes.bin
read_part

create_part
truncate_streams "structure stream" "$PART"/*object_shared_data*structure.bin
read_part

rm -rf "$DATA_DIR"
