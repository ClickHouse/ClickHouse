#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Iceberg table function is not in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An Iceberg table whose decimal column is `Decimal(38, 30)`, so the column bounds in its manifest
# hold an unscaled value 14 bytes wide. Decoding a bound used to run in `int64_t`, where the
# big-endian accumulation, the sign extension and the 10^scale scaler all overflow; that is undefined
# behaviour and it aborts a build with the undefined behaviour sanitizer. A bound that wide never
# becomes a `Field`, so the results below are the same before and after the fix: what they assert is
# that reading the manifest with a filter present is free of undefined behaviour.

TABLE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_iceberg_decimal"

cleanup()
{
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

mkdir -p "${TABLE_PATH}"

# ClickHouse cannot write a decimal column to Iceberg, so the table cannot be built by the test.
# This one was produced by Spark and attached to
# https://github.com/ClickHouse/ClickHouse/issues/114929:
#
#   CREATE TABLE dec_min (id int, d decimal(38, 30)) USING iceberg
#     TBLPROPERTIES ('format-version' = '2', 'write.format.default' = 'Parquet');
#   INSERT INTO dec_min SELECT 1, decimal(42.42);
#
# repacked here without the Spark checksum files.
python3 "$CUR_DIR/04907_iceberg_decimal_bounds_overflow.py" "${TABLE_PATH}"

# Any filter makes the manifest bounds be parsed; it does not have to touch the decimal column.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM icebergLocal('${TABLE_PATH}') WHERE id > 0"
${CLICKHOUSE_CLIENT} --query "SELECT id, toString(d) FROM icebergLocal('${TABLE_PATH}') WHERE d > 0"
