#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# INSERT ... FROM INFILE is parsed on the client through a StorageFile pipeline — a different code
# path from inline/stdin data. A parse error caused by a structure mismatch must get the same
# explanation there, including when the file is compressed.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

DATA_FILE_MISMATCH="${CLICKHOUSE_TMP}/04608_infile_mismatch_${CLICKHOUSE_DATABASE}.tsv"
DATA_FILE_COMPATIBLE="${CLICKHOUSE_TMP}/04608_infile_compatible_${CLICKHOUSE_DATABASE}.tsv"
DATA_FILE_GZ="${CLICKHOUSE_TMP}/04608_infile_mismatch_${CLICKHOUSE_DATABASE}.tsv.gz"

printf '1\tpage_view\t/users/profile\n' > "$DATA_FILE_MISMATCH"
printf '1\t1.5\t2\n' > "$DATA_FILE_COMPATIBLE"
printf '1\tpage_view\t/users/profile\n' | gzip > "$DATA_FILE_GZ"

$CLICKHOUSE_CLIENT -q "CREATE TABLE test_mismatch_infile (c1 UInt8, c2 UInt8, c3 UInt8) ENGINE = Memory"

echo "-- type mismatch in the file"
$CLICKHOUSE_CLIENT --query "INSERT INTO test_mismatch_infile FROM INFILE '${DATA_FILE_MISMATCH}' FORMAT TSV" < /dev/null 2>&1 | check

echo "-- compatible structure, the parse fails only on a fractional value (no false positive)"
$CLICKHOUSE_CLIENT --query "INSERT INTO test_mismatch_infile FROM INFILE '${DATA_FILE_COMPATIBLE}' FORMAT TSV" < /dev/null 2>&1 | check

echo "-- type mismatch in a compressed file"
$CLICKHOUSE_CLIENT --query "INSERT INTO test_mismatch_infile FROM INFILE '${DATA_FILE_GZ}' FORMAT TSV" < /dev/null 2>&1 | check

$CLICKHOUSE_CLIENT -q "DROP TABLE test_mismatch_infile"
rm -f "$DATA_FILE_MISMATCH" "$DATA_FILE_COMPATIBLE" "$DATA_FILE_GZ"
