#!/usr/bin/env bash
# Tags: no-fasttest

# Reading Parquet columns whose dictionary indices and definition levels are bit-packed (RLE_DICTIONARY plus
# nullable columns). This is the most common decode path in the native reader (BitPackedRLEDecoder). The file
# is generated with pyarrow at test time and mixes several dictionary bit-widths (large/small cardinality) and
# nullable columns so both bit-packed and RLE runs, and definition levels, are exercised. Checks are
# order-independent, and the file is prefixed with the (unique) database name so the test is parallel-safe.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

F="${CLICKHOUSE_DATABASE}_dict_def_levels.parquet"
trap 'rm -f "${USER_FILES_PATH:?}/${F}"' EXIT

python3 - "$USER_FILES_PATH/${F}" <<'PYEOF'
import sys, random
import pyarrow as pa
import pyarrow.parquet as pq

path = sys.argv[1]
random.seed(123)
n = 20000
words = [f"item_{i:04d}_{'x' * (i % 7)}" for i in range(500)]
di  = pa.array([random.randint(0, 299) * 1000 for _ in range(n)], type=pa.int32())          # ~9-bit indices
ds  = pa.array([words[random.randrange(500)] for _ in range(n)], type=pa.string())          # ~9-bit indices
dsm = pa.array([random.choice([10, 20, 30]) for _ in range(n)], type=pa.int32())            # tiny bit width / RLE runs
ni  = pa.array([None if random.random() < 0.3 else random.randint(-5, 5) for _ in range(n)], type=pa.int64())
ns  = pa.array([None if random.random() < 0.4 else words[random.randrange(500)] for _ in range(n)], type=pa.string())
k   = pa.array(list(range(n)), type=pa.int32())
pq.write_table(pa.table({"di": di, "ds": ds, "dsm": dsm, "ni": ni, "ns": ns, "k": k}),
               path, use_dictionary=True, data_page_version="2.0", compression="none")
PYEOF

# Structure is specified explicitly so the result is independent of schema-inference settings.
STRUCT="di Int32, ds String, dsm Int32, ni Nullable(Int64), ns Nullable(String), k Int32"

echo "--- counts and per-column checksums ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT
        count(),
        count(ni), count(ns),
        sum(cityHash64(di)), sum(cityHash64(ds)), sum(cityHash64(dsm)),
        sum(cityHash64(ni)), sum(cityHash64(ns))
    FROM file('${F}', 'Parquet', '$STRUCT')"

echo "--- distinct dictionary cardinalities ---"
${CLICKHOUSE_CLIENT} --query "SELECT uniqExact(di), uniqExact(ds), uniqExact(dsm) FROM file('${F}', 'Parquet', '$STRUCT')"

echo "--- sample rows ---"
${CLICKHOUSE_CLIENT} --query "SELECT k, di, ds, dsm, ni, ns FROM file('${F}', 'Parquet', '$STRUCT') ORDER BY k LIMIT 8"

echo "--- filtered read (exercises partial decode) ---"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(cityHash64(ds)) FROM file('${F}', 'Parquet', '$STRUCT') WHERE k % 7 = 0"
