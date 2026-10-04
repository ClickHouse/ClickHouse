#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is experimental while its wire layout is still evolving.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"

# `ColumnBinaryInputFormat` decodes a frame in place when the read buffer already holds it
# whole, and otherwise copies it into its own storage. Which branch runs depends only on how
# the data happens to be buffered, so both must decode a frame identically. Force each branch
# with `max_read_buffer_size` and check the results match: a buffer far larger than the frame
# takes the in-place branch, one far smaller makes the frame span several refills and takes
# the copying branch.
#
# The table mixes fixed-width, variable-width, nullable and nested columns so that the
# descriptor offsets a frame-spanning read has to reassemble are not trivial.
${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE IF EXISTS t_05024_src;
CREATE TABLE t_05024_src (n UInt64, s String, m Nullable(Int32), a Array(UInt16)) ENGINE = Memory;
INSERT INTO t_05024_src
SELECT number,
       repeat('x', 1 + number % 97),
       if(number % 7 = 0, NULL, toInt32(number) - 5000),
       range(number % 5)
FROM numbers(20000);
"

mkdir -p "${USER_FILES_PATH}"
frame="${USER_FILES_PATH}/05024_frame_${CLICKHOUSE_DATABASE}.bin"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM t_05024_src ORDER BY n FORMAT ColumnBinary" > "${frame}"

# The frame must be much larger than the small buffer below, otherwise the copying branch
# would never be exercised and the test would silently check the same path twice.
echo "frame larger than 64 KiB: $(( $(wc -c < "${frame}") > 65536 ))"

read_back() {
    ${CLICKHOUSE_CLIENT} --max_read_buffer_size "$1" --query "
    SELECT sum(n), sum(cityHash64(s)), sum(assumeNotNull(m)), countIf(m IS NULL), sum(length(a)), count()
    FROM file('${frame}', ColumnBinary, 'n UInt64, s String, m Nullable(Int32), a Array(UInt16)')"
}

in_place=$(read_back 4194304)
spanning=$(read_back 8192)

echo "in-place matches source: $([ "${in_place}" = "$(${CLICKHOUSE_CLIENT} --query "SELECT sum(n), sum(cityHash64(s)), sum(assumeNotNull(m)), countIf(m IS NULL), sum(length(a)), count() FROM t_05024_src")" ] && echo 1 || echo 0)"
echo "frame-spanning matches in-place: $([ "${spanning}" = "${in_place}" ] && echo 1 || echo 0)"

# A truncated frame must be rejected the same way on both branches, rather than one of them
# decoding whatever bytes happen to follow. The in-place branch cannot claim the frame (the
# buffer never holds all of it), so both fall through to the copying branch and fail on the
# short read of the data section.
head -c 1024 "${frame}" > "${frame}.trunc"
for buf in 4194304 8192; do
    if ${CLICKHOUSE_CLIENT} --max_read_buffer_size "${buf}" --query "
    SELECT count() FROM file('${frame}.trunc', ColumnBinary, 'n UInt64, s String, m Nullable(Int32), a Array(UInt16)')" 2>&1 | grep -q 'CANNOT_READ_ALL_DATA'
    then
        echo "truncated frame rejected with max_read_buffer_size=${buf}: 1"
    else
        echo "truncated frame rejected with max_read_buffer_size=${buf}: 0"
    fi
done

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05024_src"
rm -f "${frame}" "${frame}.trunc"
