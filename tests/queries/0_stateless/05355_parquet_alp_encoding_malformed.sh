#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet is not built in the fast test.

# Malformed `ALP` pages must be rejected with `INCORRECT_DATA` rather than read out of bounds.
# A file with one uncompressed `ALP` page is written, then single fields of the page are overwritten.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

P="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_alp_malformed"
trap 'rm -f "${P}"_*.parquet' EXIT

# 100 values, the one at index 5 is NaN, which is stored as an exception.
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('${P}_good.parquet') SELECT if(number = 5, nan, round(number * 0.25 - 3, 2)) AS d FROM numbers(100)
SETTINGS output_format_parquet_use_alp_encoding = 1, output_format_parquet_compression_method = 'none', output_format_parquet_write_checksums = 0;
SELECT tupleElement(columns[1], 'encodings') FROM file('${P}_good.parquet', ParquetMetadata);
SELECT count(), countIf(isNaN(d)), sum(d) FROM file('${P}_good.parquet');
"

GOOD=good

# The ALP page header (mode 0, FOR + bit-packing, 2^10 values per vector, 100 values), followed by the offset of the only vector.
HEADER="00000A6400000004000000"
# The exception value: the bits of NaN.
NAN_BITS="000000000000F87F"

# Overwrites the bytes at offset `$2` from the first occurrence of the bytes `$1` with the bytes `$3`, reads
# the file, and prints the result or the error.
function check()
{
    $CLICKHOUSE_LOCAL -q "
    INSERT INTO FUNCTION file('${P}_bad.parquet', RawBLOB)
    SELECT concat(substring(raw, 1, p - 1 + $2), unhex('$3'), substring(raw, p + $2 + length(unhex('$3'))))
    FROM (SELECT raw, position(raw, unhex('$1')) AS p FROM file('${P}_${GOOD}.parquet', RawBLOB, 'raw String'))
    SETTINGS engine_file_truncate_on_insert = 1;
    SELECT count(), sum(d) FROM file('${P}_bad.parquet', Parquet, 'd Float64');
    " 2>&1 | grep -o -m1 -E "Code: [0-9]+\. DB::Exception: [^:(]*|^[0-9].*" | sed -E 's/^Code: ([0-9]+)\. DB::Exception: /error \1: /'
}

echo "--- page header"
check "$HEADER" 0 "01"         # compression mode
check "$HEADER" 1 "01"         # integer encoding
check "$HEADER" 2 "02"         # log_vector_size below the minimum
check "$HEADER" 2 "10"         # log_vector_size above the maximum
check "$HEADER" 3 "FFFFFFFF"   # negative number of values
check "$HEADER" 3 "FFFFFF7F"   # too many vectors for the page
check "$HEADER" 3 "32000000"   # fewer values than the page has
echo "--- offsets"
check "$HEADER" 7 "00000000"   # into the offset array
check "$HEADER" 7 "FFFFFF00"   # past the end of the page
check "$HEADER" 7 "05000000"   # a gap before the first vector
echo "--- vector header"
check "$HEADER" 11 "13"        # exponent above the maximum
check "$HEADER" 12 "FF"        # factor above the exponent
check "$HEADER" 13 "FFFF"      # more exceptions than values
check "$HEADER" 13 "0200"      # exceptions past the end of the page
check "$HEADER" 13 "0000"      # fewer exceptions, the page has bytes after the last vector
check "$HEADER" 23 "41"        # bit width above the maximum
check "$HEADER" 23 "40"        # packed values past the end of the page
echo "--- exceptions"
check "$NAN_BITS" -2 "6400"    # position past the end of the vector
check "$NAN_BITS" -2 "6300"    # the last position is fine

# 2000 values in two vectors: the second vector must begin right where the first one ends.
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('${P}_two.parquet') SELECT round(number * 0.01, 2) AS d FROM numbers(2000)
SETTINGS output_format_parquet_use_alp_encoding = 1, output_format_parquet_compression_method = 'none', output_format_parquet_write_checksums = 0;
SELECT tupleElement(columns[1], 'encodings') FROM file('${P}_two.parquet', ParquetMetadata);
SELECT count(), round(sum(d), 2) FROM file('${P}_two.parquet');
"
GOOD=two
HEADER="00000AD007000008000000"
echo "--- vector boundaries"
check "$HEADER" 11 "FFFF0000"  # a gap or an overlap between the vectors
check "$HEADER" 11 "08000000"  # the second vector at the same place as the first one
