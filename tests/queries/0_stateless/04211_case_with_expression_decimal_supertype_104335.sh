#!/usr/bin/env bash

# Regression for https://github.com/ClickHouse/ClickHouse/issues/104335
#
# `caseWithExpression` used a `dst_supertype` computed only over the THEN values,
# but `transform`'s own return type is `getLeastSupertype(dst_supertype, default_type)`.
# When the ELSE branch widens the supertype to a different `ColumnDecimal` storage
# (e.g. THENs `(UInt16, Int8)` -> `Int32`, then `getLeastSupertype(Int32, Decimal(9, 2))`
# -> `Decimal(12, 2)` -> `Decimal64`), the cache row column ends up `Decimal64` while
# `caseWithExpression`'s declared result type is `Decimal(9, 2)` -> `Decimal32`.
# The downstream `assert_cast` in `executeNumToNum` then fired a `Bad cast` `LOGICAL_ERROR`
# and aborted the server / `clickhouse-local`.
#
# This file uses `clickhouse-local` because the server-side query analyzer constant-folds
# the all-constant expression before reaching `transform`, hiding the bug. `clickhouse-local`
# preserves the same code path that `Unit tests (function_prop_fuzzer)` originally hit.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Minimised case: search `UInt64`, WHENs widen to `UInt64`, THENs `(UInt16, Int8)`
# have a narrower THEN-only supertype than the (THENs + ELSE) supertype.
$CLICKHOUSE_LOCAL --query "SELECT caseWithExpression(toUInt64(123),
    CAST(1 AS UInt32), CAST(31712 AS UInt16),
    CAST(2 AS UInt32), CAST(-8 AS Int8),
    CAST(-0.09 AS Decimal(9, 2)));"

# Match on first WHEN.
$CLICKHOUSE_LOCAL --query "SELECT caseWithExpression(toUInt64(1),
    CAST(1 AS UInt32), CAST(31712 AS UInt16),
    CAST(2 AS UInt32), CAST(-8 AS Int8),
    CAST(-0.09 AS Decimal(9, 2)));"

# Match on second WHEN.
$CLICKHOUSE_LOCAL --query "SELECT caseWithExpression(toUInt64(2),
    CAST(1 AS UInt32), CAST(31712 AS UInt16),
    CAST(2 AS UInt32), CAST(-8 AS Int8),
    CAST(-0.09 AS Decimal(9, 2)));"

# Original fuzzer-found query from the issue, against `system.one ARRAY JOIN`.
# Search type `UInt64`, WHEN values `(UInt128, UInt32)`, THEN values `(UInt16, Int8(-8))`,
# ELSE `Decimal(9, 2)`.
$CLICKHOUSE_LOCAL --query "SELECT count(), groupArray(r) FROM (
    SELECT caseWithExpression(a,
        CAST(338830549581202628070853306952619594975 AS UInt128), CAST(31712 AS UInt16),
        CAST(3252362303 AS UInt32), CAST(-8 AS Int8),
        CAST(-0.09 AS Decimal(9, 2))) AS r
    FROM system.one
    ARRAY JOIN [toUInt64(1), toUInt64(2), toUInt64(3252362303), toUInt64(123)] AS a);"

# Same shape but pushed into a wider Decimal128 ELSE -- exercises the second path where
# THEN-only supertype `Int32` does not match `Decimal(38, 4)` either.
$CLICKHOUSE_LOCAL --query "SELECT caseWithExpression(toUInt64(2),
    CAST(1 AS UInt32), CAST(31712 AS UInt16),
    CAST(2 AS UInt32), CAST(-8 AS Int8),
    CAST(0.99 AS Decimal(38, 4)));"

# THEN values that fit into `Int8`/`UInt16` with a `Float64` ELSE branch:
# the THEN-only supertype is `Int32` while `getLeastSupertype(Int32, Float64)` is `Float64`.
$CLICKHOUSE_LOCAL --query "SELECT caseWithExpression(toUInt64(2),
    CAST(1 AS UInt32), CAST(31712 AS UInt16),
    CAST(2 AS UInt32), CAST(-8 AS Int8),
    CAST(-0.09 AS Float64));"
