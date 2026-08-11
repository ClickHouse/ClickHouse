#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: uses pyarrow.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A gzip-compressed Parquet page whose DEFLATE payload expands to exactly the declared
# uncompressed size but whose gzip trailer (CRC32) is corrupted must be rejected:
# zlib reports the trailer error only on the read after the one that fills the output exactly.

FILE="${CLICKHOUSE_TMP}/04848_gzip_trailer.parquet"

${CLICKHOUSE_LOCAL} -q "
    SET output_format_parquet_compression_method = 'gzip';
    SELECT number, toString(number) AS s FROM numbers(100000) INTO OUTFILE '${FILE}' TRUNCATE FORMAT Parquet
"

python3 -c "
import pyarrow.parquet as pq

path = '${FILE}'
col = pq.ParquetFile(path).metadata.row_group(0).column(0)
start = col.dictionary_page_offset or col.data_page_offset
end = start + col.total_compressed_size
with open(path, 'r+b') as f:
    # The last 8 bytes of the chunk's final gzip member are CRC32 + ISIZE; flip the CRC.
    f.seek(end - 8)
    crc = f.read(4)
    f.seek(end - 8)
    f.write(bytes(b ^ 0xFF for b in crc))
"

echo "valid columns still readable:"
${CLICKHOUSE_LOCAL} -q "SELECT sum(cityHash64(s) % 1000) FROM file('${FILE}')"

echo "with page checksum verification:"
${CLICKHOUSE_LOCAL} -q "SELECT sum(number) FROM file('${FILE}')" 2>&1 | grep -c 'INCORRECT_DATA'

echo "without page checksum verification:"
${CLICKHOUSE_LOCAL} -q "
    SET input_format_parquet_verify_checksums = 0;
    SELECT sum(number) FROM file('${FILE}')
" 2>&1 | grep -c 'ZLIB_INFLATE_FAILED'

rm -f "${FILE}"
