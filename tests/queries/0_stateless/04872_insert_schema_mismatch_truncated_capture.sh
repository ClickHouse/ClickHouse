#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A streamed insert (stdin / HTTP body) samples the data for the parse-error diagnostic through a
# capped `PrefixCapturingReadBuffer`. A row cut off by the cap must not masquerade as a structure
# mismatch: schema inference treats the end of the sample as the end of a row, so a `TSV` row whose
# first field is longer than the cap would look like a single-column row and a value-level error in
# a later column of that (well-shaped) row would get a bogus structure-mismatch explanation.

DATA_FILE="$CLICKHOUSE_TMP/04872_data_$CLICKHOUSE_DATABASE.tsv"

# Case 1: the first field alone exceeds the 1 MiB capture cap and the second column has a
# value-level error (`1.5` into `UInt8`). The row is well-shaped, so no structure-mismatch
# explanation must be added, but the parse error itself must still be reported.
{ head -c 2097152 /dev/zero | tr '\0' 'a'; printf '\t1.5\n'; } > "$DATA_FILE"
$CLICKHOUSE_LOCAL --query "
    CREATE TABLE t_04872 (s String, n UInt8) ENGINE = Memory;
    INSERT INTO t_04872 FORMAT TSV
" < "$DATA_FILE" 2>&1 | grep -c 'The structure of the data being inserted'
$CLICKHOUSE_LOCAL --query "
    CREATE TABLE t_04872 (s String, n UInt8) ENGINE = Memory;
    INSERT INTO t_04872 FORMAT TSV
" < "$DATA_FILE" 2>&1 | grep -c 'Cannot parse input'

# Case 2: valid data of the same shape still inserts fine through the capturing wrapper.
{ head -c 2097152 /dev/zero | tr '\0' 'a'; printf '\t1\n'; } > "$DATA_FILE"
$CLICKHOUSE_LOCAL --query "
    CREATE TABLE t_04872 (s String, n UInt8) ENGINE = Memory;
    INSERT INTO t_04872 FORMAT TSV
" < "$DATA_FILE" 2>&1 | grep -c 'Exception'

# Case 3: an untruncated sample keeps the diagnostic: a small genuine mismatch is still explained.
printf 'text\thello\n' > "$DATA_FILE"
$CLICKHOUSE_LOCAL --query "
    CREATE TABLE t_04872 (a UInt8, b UInt8) ENGINE = Memory;
    INSERT INTO t_04872 FORMAT TSV
" < "$DATA_FILE" 2>&1 | grep -c 'The structure of the data being inserted'

rm -f "$DATA_FILE"
