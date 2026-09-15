#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# On the `INSERT ... FROM INFILE` path the row the parser had reached is recovered from the error
# message, and the excerpt of the data quoted there may contain a lookalike of the
# ": (in file/uri " suffix that `IInputFormat::generate` appends after the genuine "(at row N)"
# marker. Such a lookalike must not disable the row bound: with the bound lost, rows the parser
# never reached would be sampled and could turn a value-level error into a bogus structure
# mismatch (or suppress a genuine one).

DATA_FILE="$CLICKHOUSE_TMP/04871_data_$CLICKHOUSE_DATABASE.tsv"

# Case 1: row 1 fails on a value-level error (`1.5` into `UInt8`) and its excerpt contains the
# spoofing substring; row 2 would widen the first column to `String`. The row bound must survive
# the spoof, so only row 1 is sampled and no structure-mismatch explanation is added.
printf '1.5\thello: (in file/uri x\ntext\tworld\n' > "$DATA_FILE"
$CLICKHOUSE_LOCAL --input_format_tsv_detect_header 0 --query "
    CREATE TABLE t_04871 (a UInt8, b String) ENGINE = Memory;
    INSERT INTO t_04871 FROM INFILE '$DATA_FILE' FORMAT TSV;
" < /dev/null 2>&1 | grep -c 'The structure of the data being inserted'

# Case 2: a genuine structure mismatch in row 1 (text where a number is expected), with the same
# spoofing substring in the data. The explanation must still be present.
printf 'abc\thello: (in file/uri x\t9\n1\tworld\t8\n' > "$DATA_FILE"
$CLICKHOUSE_LOCAL --input_format_tsv_detect_header 0 --query "
    CREATE TABLE t_04871 (a UInt8, b String, c UInt8) ENGINE = Memory;
    INSERT INTO t_04871 FROM INFILE '$DATA_FILE' FORMAT TSV;
" < /dev/null 2>&1 | grep -c 'The structure of the data being inserted'

rm -f "$DATA_FILE"
