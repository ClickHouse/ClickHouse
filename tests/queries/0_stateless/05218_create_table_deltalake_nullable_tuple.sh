#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan
#
# Issue #120247: a Delta `struct` field is read back as a bare `Tuple` even when it is marked nullable,
# so a declared `Nullable(Tuple)` does not round-trip and no INSERT can ever write a NULL into it.
# `CREATE TABLE` must reject it up front (Code: 48 = NOT_IMPLEMENTED) at every nesting position instead
# of leaving an unusable Delta table on storage. A non-Nullable Tuple with Nullable leaves stays supported.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}_nullable_tuple"

echo "rejections:"
for spec in "Nullable(Tuple(a Int32))" "Array(Nullable(Tuple(a Int32)))" "Map(String, Nullable(Tuple(a Int32)))" "Tuple(a Nullable(Tuple(b Int32)))"; do
    reject_path="${TABLE_PATH}_reject"
    rm -rf "$reject_path"
    if $CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
SET allow_delta_lake_create_table = 1;
SET enable_nullable_tuple_type = 1;
CREATE TABLE t_dl_nullable_tuple (c ${spec}) ENGINE = DeltaLakeLocal('${reject_path}', Parquet);
" 2>&1 | grep -q "Code: 48"; then
        echo "${spec}: rejected"
    else
        echo "${spec}: NOT rejected"
    fi
    # A rejected CREATE must not leave an orphan Delta table behind at the target path.
    if [ -d "${reject_path}/_delta_log" ]; then
        echo "${spec}: fail: orphan _delta_log left behind"
    fi
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_dl_nullable_tuple" >/dev/null 2>&1
    rm -rf "$reject_path"
done

# The nullability of a Delta `struct` lives on its leaf fields, so a non-Nullable named Tuple with
# Nullable elements round-trips and must still be accepted.
echo "nullable-leaves:"
rm -rf "$TABLE_PATH"
$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
SET allow_delta_lake_create_table = 1;
DROP TABLE IF EXISTS t_dl_nullable_leaves;
CREATE TABLE t_dl_nullable_leaves (c Tuple(a Nullable(Int32), b Nullable(String))) ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet);
"
echo "accepted"

SCHEMA_QUERY="SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_dl_nullable_leaves'"
DECLARED=$($CLICKHOUSE_CLIENT --query "SET allow_experimental_delta_kernel_rs = 1; ${SCHEMA_QUERY}")

$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
DROP TABLE t_dl_nullable_leaves;
CREATE TABLE t_dl_nullable_leaves ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet);
"
REATTACHED=$($CLICKHOUSE_CLIENT --query "SET allow_experimental_delta_kernel_rs = 1; ${SCHEMA_QUERY}")

if [ "$DECLARED" = "$REATTACHED" ]; then
    echo "schema preserved"
else
    echo "schema MISMATCH"
    echo "declared:";   echo "$DECLARED"
    echo "reattached:"; echo "$REATTACHED"
fi

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_dl_nullable_leaves"

rm -rf "$TABLE_PATH"
