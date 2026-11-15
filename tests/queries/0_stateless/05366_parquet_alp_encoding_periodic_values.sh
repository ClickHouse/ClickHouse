#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet is not built in the fast test.

# The `ALP` encoder chooses its parameters on samples of the values. The samples must not alias
# with periodic data: for values with a step of 0.25 a sample of every 16th value contains only
# the integers, the parameters chosen on it make 3/4 of the values exceptions, and the pages are
# written as `PLAIN` instead.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

P="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_alp_periodic"
trap 'rm -f "${P}"_*.parquet' EXIT

$CLICKHOUSE_LOCAL -q "
CREATE TABLE src ENGINE = Memory AS SELECT
    round(number * 0.25 - 3, 2) AS d_quarter,
    toFloat32(round(number * 0.25 - 3, 2)) AS f_quarter,
    round(number * 0.125, 3) AS d_eighth,
    round(intDiv(number, 7) + (number % 4) * 0.01, 2) AS d_cents
FROM numbers(100000);

INSERT INTO FUNCTION file('${P}_alp.parquet') SELECT * FROM src SETTINGS
    output_format_parquet_compression_method = 'none', output_format_parquet_use_alp_encoding = 1;

SELECT arrayMap(c -> (tupleElement(c, 'path'), tupleElement(c, 'encodings')), columns) FROM file('${P}_alp.parquet', ParquetMetadata);

-- Less than 0.6 of PLAIN: no vector is mostly exceptions. The values need 15 bits per vector, and
-- the rest is the overhead, which is bigger with the smaller pages for the small randomized blocks.
SELECT arrayMap(c -> (tupleElement(c, 'path'), tupleElement(c, 'total_compressed_size') * 5 < 100000 * 3 * if(tupleElement(c, 'path') LIKE 'f%', 4, 8)), columns) FROM file('${P}_alp.parquet', ParquetMetadata);

SELECT
    (SELECT sum(cityHash64(reinterpretAsUInt64(d_quarter), reinterpretAsUInt32(f_quarter), reinterpretAsUInt64(d_eighth), reinterpretAsUInt64(d_cents))) FROM src)
    = (SELECT sum(cityHash64(reinterpretAsUInt64(d_quarter), reinterpretAsUInt32(f_quarter), reinterpretAsUInt64(d_eighth), reinterpretAsUInt64(d_cents))) FROM file('${P}_alp.parquet'));
"
