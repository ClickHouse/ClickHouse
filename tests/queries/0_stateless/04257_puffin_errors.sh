#!/usr/bin/env bash
# Tags: no-fasttest
#
# Deletion-vector / Puffin payload error cases. Split from the former monolithic
# 04257 suite so slow CI builds stay under the 300s timeout. Cases still run in
# a small parallel pool because clickhouse-local startup dominates runtime.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./data_puffin/puffin_errors_common.sh
. "$CURDIR"/data_puffin/puffin_errors_common.sh

for f in overflow_offset_length negative_offset length_exceeds_file blob_overlaps_footer
do
    launch "$id" puffin "$DATA/$f.puffin" 'Puffin blob 0: offset/length out of bounds'
    id=$((id + 1))
done

launch "$id" puffin "$DATA/invalid_roaring_bitmap.puffin" 'Failed to deserialize deletion vector roaring bitmap' 'BAD_ARGUMENTS'
id=$((id + 1))
launch "$id" puffin "$DATA/invalid_bitmap_key.puffin" 'Invalid deletion vector bitmap key'
id=$((id + 1))
launch "$id" puffin "$DATA/cardinality_mismatch_large_bitmap.puffin" 'exceeds declared cardinality'
id=$((id + 1))
launch "$id" puffin "$DATA/cardinality_exceeds_materialization_limit.puffin" 'exceeds materialization limit' 'BAD_ARGUMENTS'
id=$((id + 1))

for f in invalid_cardinality_non_numeric invalid_cardinality_negative
do
    launch "$id" puffin "$DATA/$f.puffin" "property 'cardinality' must be an unsigned integer"
    id=$((id + 1))
done

launch "$id" raw_puffin 'puffin_wrong_type' 'Unexpected type' \
    "SELECT deleted_rows FROM file('$PUFFIN', Puffin, 'deleted_rows Array(String)')"
id=$((id + 1))
launch "$id" raw_puffin 'puffin_unknown_column' 'Unexpected column' \
    "SELECT foo FROM file('$PUFFIN', Puffin, 'foo String')"
id=$((id + 1))

finish_puffin_errors
