#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An Arrow `date32` field read into a numeric ClickHouse target must return the raw day number without the
# `Date32` range check, matching the Apache Arrow library reader's numeric type-hint behavior — even for day
# numbers outside ClickHouse's representable `Date32` range. The native reader previously threw on those days
# before the value could be cast to the requested numeric type, regressing valid explicit-structure reads.
# A `Date32` target, by contrast, must still apply the range check. The file is built with pyarrow because
# ClickHouse itself cannot write a `date32` value outside the `Date32` range.

DATA_FILE="${CLICKHOUSE_TMP}/04343_date32.arrow"

python3 - "$DATA_FILE" <<'PY'
import sys
import pyarrow as pa

# Day numbers since the epoch: the epoch, a normal date, the lowest in-range day (1900-01-01), and two days
# far outside ClickHouse's `Date32` range, which a numeric target must return verbatim.
days = [0, 19000, -25567, 3000000, -3000000]
batch = pa.record_batch([pa.array(days, type=pa.date32())], names=["d"])
with pa.OSFile(sys.argv[1], "wb") as sink:
    with pa.ipc.new_file(sink, batch.schema) as writer:
        writer.write_batch(batch)
PY

echo "--- date32 -> Int32 numeric target: raw day numbers, native == library ---"
for READER in 1 0; do
    ${CLICKHOUSE_LOCAL} --query "
        SELECT d FROM file('${DATA_FILE}', 'Arrow', 'd Int32')
        SETTINGS input_format_arrow_use_native_reader = ${READER}
    "
done

echo "--- date32 -> Date32 target: out-of-range day is still rejected (native) ---"
${CLICKHOUSE_LOCAL} --query "
    SELECT d FROM file('${DATA_FILE}', 'Arrow', 'd Date32')
    SETTINGS input_format_arrow_use_native_reader = 1
" 2>&1 | grep -o "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE" | head -1

rm -f "${DATA_FILE}"
