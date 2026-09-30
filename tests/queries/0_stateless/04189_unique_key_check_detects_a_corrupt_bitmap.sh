#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
# UNIQUE KEY: CHECK TABLE re-hashes a delete bitmap through its `checksums.txt` entry: the bitmap
# is listed, CHECK passes, then fails once a byte of the bitmap is flipped.
# no-object-storage, no-s3-storage: the test rewrites a byte of a part file in place.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --enable_unique_key 1"

# Red if a bitmap is not listed in `checksums.txt` (`check_before` goes to 0: an unlisted file).
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_bitmap_check"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE uk_bitmap_check (k UInt64, v UInt64)
    ENGINE = MergeTree ORDER BY k UNIQUE KEY (k)
    SETTINGS min_bytes_for_wide_part = 0
"

$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES uk_bitmap_check"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_bitmap_check SELECT number, 0 FROM numbers(8)"
# The second INSERT overwrites four keys, so all_2_2_0 holds a bitmap for all_1_1_0.
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_bitmap_check SELECT number, 1 FROM numbers(4)"

HOLDER=$($CLICKHOUSE_CLIENT --query "
    SELECT path FROM system.parts
    WHERE database = currentDatabase() AND table = 'uk_bitmap_check' AND active AND name = 'all_2_2_0'
")
BITMAP=$(find "$HOLDER" -maxdepth 1 -name 'delete_bitmap_*.rbm' | head -1)

if [[ -z "$BITMAP" ]]; then
    echo "bitmap_written 0"
    exit 1
fi
echo "bitmap_written 1"

echo "check_before $($CLICKHOUSE_CLIENT --check_query_single_value_result 1 --query "CHECK TABLE uk_bitmap_check")"

# Flip the last byte, covered by the trailing CRC and by the checksum entry.
SIZE=$(stat -c %s "$BITMAP")
printf '\xff' | dd of="$BITMAP" bs=1 seek=$((SIZE - 1)) count=1 conv=notrunc status=none

$CLICKHOUSE_CLIENT --query "DETACH TABLE uk_bitmap_check"
$CLICKHOUSE_CLIENT --query "ATTACH TABLE uk_bitmap_check"

RESULT=$($CLICKHOUSE_CLIENT --send_logs_level none --check_query_single_value_result 1 \
    --query "CHECK TABLE uk_bitmap_check" 2>&1 || true)
case "$RESULT" in
    0|*CHECKSUM_DOESNT_MATCH*|*BAD_SIZE_OF_FILE*) echo "corrupt_detected 1" ;;
    *)                                            echo "corrupt_detected 0 ($RESULT)" ;;
esac

$CLICKHOUSE_CLIENT --query "DROP TABLE uk_bitmap_check"
