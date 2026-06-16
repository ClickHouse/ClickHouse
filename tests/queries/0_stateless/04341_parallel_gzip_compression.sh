#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

if ! command -v gzip &> /dev/null; then echo "gzip not found" 1>&2; exit 1; fi

# max_generic_compression_threads > 1 makes 'gzip' compression run in parallel. The output must stay a
# standard gzip stream: byte-for-byte decompressible by the system gzip and identical to single-threaded output.
PAR="${CLICKHOUSE_TMP}/04341_parallel.tsv.gz"
SEQ="${CLICKHOUSE_TMP}/04341_serial.tsv.gz"
rm -f "$PAR" "$SEQ"

${CLICKHOUSE_CLIENT} --max_generic_compression_threads 8 --query "SELECT number FROM numbers(2000000) INTO OUTFILE '${PAR}' COMPRESSION 'gzip' FORMAT TSV"
${CLICKHOUSE_CLIENT} --max_generic_compression_threads 1 --query "SELECT number FROM numbers(2000000) INTO OUTFILE '${SEQ}' COMPRESSION 'gzip' FORMAT TSV"

gzip -t "$PAR" && echo "parallel output is valid gzip"
diff <(gzip -dc "$PAR") <(gzip -dc "$SEQ") > /dev/null && echo "parallel and serial gzip decompress identically"

# The parallel output round-trips through the regular gzip reader.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_04341"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_04341 (x UInt64) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_04341 (x) FROM INFILE '${PAR}' COMPRESSION 'gzip' FORMAT TSV"
${CLICKHOUSE_CLIENT} --query "SELECT count(x), count(DISTINCT x) FROM test_04341"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test_04341"
rm -f "$PAR" "$SEQ"
