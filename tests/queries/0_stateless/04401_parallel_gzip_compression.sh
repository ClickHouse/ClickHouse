#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

if ! command -v gzip &> /dev/null; then echo "gzip not found" 1>&2; exit 1; fi

# max_generic_compression_threads > 1 makes 'gzip' compression run in parallel. The output must stay a
# standard gzip stream: decompressible by the system gzip and identical to single-threaded output.
PAR="${CLICKHOUSE_TMP}/04401_parallel.tsv.gz"
SEQ="${CLICKHOUSE_TMP}/04401_serial.tsv.gz"
rm -f "$PAR" "$SEQ"

# 100k rows is ~0.6 MiB of TSV, larger than several 256 KiB blocks, so the parallel path produces multiple
# independently-flushed blocks; kept small so the repeated flaky-check runs stay fast.
${CLICKHOUSE_CLIENT} --max_generic_compression_threads 8 --query "SELECT number FROM numbers(100000) INTO OUTFILE '${PAR}' COMPRESSION 'gzip' FORMAT TSV"
${CLICKHOUSE_CLIENT} --max_generic_compression_threads 1 --query "SELECT number FROM numbers(100000) INTO OUTFILE '${SEQ}' COMPRESSION 'gzip' FORMAT TSV"

gzip -t "$PAR" && echo "parallel output is valid gzip"
diff <(gzip -dc "$PAR") <(gzip -dc "$SEQ") > /dev/null && echo "parallel and serial gzip decompress identically"
# The parallel path flushes independent blocks, so its framing differs from the single-stream serial output:
# this confirms the parallel deflater actually ran rather than silently falling back to serial.
cmp -s "$PAR" "$SEQ" && echo "UNEXPECTED: parallel and serial output are byte-identical" || echo "parallel and serial gzip framing differ"

rm -f "$PAR" "$SEQ"
