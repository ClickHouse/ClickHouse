#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# File names listed in a part's checksums.txt are used verbatim to build paths inside the part
# directory, so ATTACH must reject names that can escape it. Legacy checksums files (format
# version 2) with plain names must keep working.

data_path="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$data_path"

# min_bytes_for_full_part_storage = 0 keeps the part in full storage, so checksums.txt is a file
# on disk instead of an entry inside data.packed.
$CLICKHOUSE_LOCAL --path "$data_path" -m -q "
CREATE TABLE tab (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS min_bytes_for_full_part_storage = 0;
INSERT INTO tab VALUES (1);
ALTER TABLE tab DETACH PARTITION tuple();
"

part_dir=$(find "$data_path" -type d -path '*/detached/all_1_1_0' | head -n 1)

attach() {
    $CLICKHOUSE_LOCAL --path "$data_path" -q "ALTER TABLE tab ATTACH PART 'all_1_1_0'" 2>&1 | grep -o -a -F -m 1 "$1"
}

# The file name is passed through %b so that the NUL byte case can be written as an escape.
reject_v2() {
    chmod u+w "$part_dir/checksums.txt"
    printf 'checksums format version: 2\n1 files:\n%b\n\tsize: 0\n\thash: 0 0\n\tcompressed: 0\n' "$1" > "$part_dir/checksums.txt"
    attach "$2"
}

reject_v2 '../../../evil' "File name '../../../evil' in checksums of data part contains '..' path component"
reject_v2 './data.bin' "File name './data.bin' in checksums of data part contains '.' path component"
reject_v2 '/etc/passwd' "Absolute file name '/etc/passwd' in checksums of data part"
reject_v2 '' "Empty file name in checksums of data part"
reject_v2 'data\000.bin' "in checksums of data part contains a NUL byte"

# Format version 3 is binary: varint count, binary string name, varint size, 16 byte hash, is_compressed.
chmod u+w "$part_dir/checksums.txt"
printf 'checksums format version: 3\n\x01\x0a../../evil\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' > "$part_dir/checksums.txt"
attach "File name '../../evil' in checksums of data part contains '..' path component"

# A legacy format version 2 checksums file that lists the real files of the part still attaches.
chmod u+w "$part_dir/checksums.txt"
files=$(cd "$part_dir" && ls -1 | grep -v -x 'checksums.txt')
{
    printf 'checksums format version: 2\n'
    printf '%s files:\n' "$(echo "$files" | wc -l)"
    for f in $files; do
        printf '%s\n\tsize: %s\n\thash: 0 0\n\tcompressed: 0\n' "$f" "$(stat -c%s "$part_dir/$f")"
    done
} > "$part_dir/checksums.txt"
$CLICKHOUSE_LOCAL --path "$data_path" -m -q "ALTER TABLE tab ATTACH PART 'all_1_1_0'; SELECT count(), sum(a) FROM tab"

rm -rf "$data_path"
