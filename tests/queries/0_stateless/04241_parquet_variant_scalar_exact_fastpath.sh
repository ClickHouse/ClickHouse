#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: `Parquet` format is not supported in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TMP}/04241_parquet_variant_scalar_exact_fastpath.parquet"

rm -f "$FILE"

"${CLICKHOUSE_LOCAL}" --multiquery --query "
SET allow_experimental_dynamic_type = 1;
SET enable_time_time64_type = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

CREATE TABLE src
(
    id UInt8,
    d Dynamic
)
ENGINE = Memory;

INSERT INTO src VALUES
    (1, CAST(toDate32('2024-01-02'), 'Dynamic')),
    (2, CAST(toDateTime64('2024-01-02 03:04:05.123456', 6, 'UTC'), 'Dynamic')),
    (3, CAST(CAST('12:34:56.123456', 'Time64(6)'), 'Dynamic')),
    (4, CAST(toDecimal32('123.45', 2), 'Dynamic')),
    (5, CAST(toDecimal64('-12345.6789', 4), 'Dynamic')),
    (6, CAST(toUUID('01234567-89ab-cdef-0123-456789abcdef'), 'Dynamic'));

SELECT *
FROM src
INTO OUTFILE '${FILE}'
FORMAT Parquet;

SELECT
    id,
    dynamicType(d),
    toString(d)
FROM file(
    '${FILE}',
    Parquet,
    'id UInt8, d Dynamic')
ORDER BY id
FORMAT TSVRaw;

SELECT
    id,
    d.Date32,
    d.\`DateTime64(6, 'UTC')\`,
    d.\`Time64(6)\`,
    d.\`Decimal(9, 2)\`,
    d.\`Decimal(18, 4)\`,
    d.UUID
FROM file(
    '${FILE}',
    Parquet,
    'id UInt8, d Dynamic')
ORDER BY id
FORMAT TSVRaw;
"
