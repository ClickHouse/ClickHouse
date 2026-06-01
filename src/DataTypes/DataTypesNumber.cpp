#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFactory.h>


#include <Parsers/IAST.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <typename T>
static DataTypePtr createNumericDataType(const ASTPtr & arguments)
{
    if (arguments)
    {
        if (std::is_integral_v<T>)
        {
            if (arguments->children.size() > 1)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                                "{} data type family must not have more than one argument - display width", TypeName<T>);
        }
        else
        {
            if (arguments->children.size() > 2)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                                "{} data type family must not have more than two arguments - total number "
                                "of digits and number of digits following the decimal point", TypeName<T>);
        }
    }
    return std::make_shared<DataTypeNumber<T>>();
}

bool isUInt64ThatCanBeInt64(const DataTypePtr & type)
{
    const DataTypeUInt64 * uint64_type = typeid_cast<const DataTypeUInt64 *>(type.get());
    return uint64_type && uint64_type->canUnsignedBeSigned();
}


void registerDataTypeNumbers(DataTypeFactory & factory)
{
    factory.registerDataType("UInt8", createNumericDataType<UInt8>, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = "A 8-bit unsigned integer. See the `Int32` entry for the full documentation of the integer types.",
            .syntax = "UInt8",
            .related = {"Int32"},
        });
    factory.registerDataType("UInt16", createNumericDataType<UInt16>, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = "A 16-bit unsigned integer. See the `Int32` entry for the full documentation of the integer types.",
            .syntax = "UInt16",
            .related = {"Int32"},
        });
    factory.registerDataType("UInt32", createNumericDataType<UInt32>, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = "A 32-bit unsigned integer. See the `Int32` entry for the full documentation of the integer types.",
            .syntax = "UInt32",
            .related = {"Int32"},
        });
    factory.registerDataType("UInt64", createNumericDataType<UInt64>, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = "A 64-bit unsigned integer. See the `Int32` entry for the full documentation of the integer types.",
            .syntax = "UInt64",
            .related = {"Int32"},
        });

    factory.registerDataType("Int8", createNumericDataType<Int8>, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = "A 8-bit signed integer. See the `Int32` entry for the full documentation of the integer types.",
            .syntax = "Int8",
            .related = {"Int32"},
        });
    factory.registerDataType("Int16", createNumericDataType<Int16>, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = "A 16-bit signed integer. See the `Int32` entry for the full documentation of the integer types.",
            .syntax = "Int16",
            .related = {"Int32"},
        });
    factory.registerDataType("Int32", createNumericDataType<Int32>, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = R"DOCS_MD(
ClickHouse offers a number of fixed-length integers,
with a sign (`Int`) or without a sign (unsigned `UInt`) ranging from one byte to 32 bytes.

When creating tables, numeric parameters for integer numbers can be set (e.g. `TINYINT(8)`, `SMALLINT(16)`, `INT(32)`, `BIGINT(64)`), but ClickHouse ignores them.

## Integer Ranges {#integer-ranges}

Integer types have the following ranges:

| Type     | Range                                                                                                                                                              |
|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Int8`   | \[-128 : 127\]                                                                                                                                                     |
| `Int16`  | \[-32768 : 32767\]                                                                                                                                                 |
| `Int32`  | \[-2147483648 : 2147483647\]                                                                                                                                       |
| `Int64`  | \[-9223372036854775808 : 9223372036854775807\]                                                                                                                     |
| `Int128` | \[-170141183460469231731687303715884105728 : 170141183460469231731687303715884105727\]                                                                             |
| `Int256` | \[-57896044618658097711785492504343953926634992332820282019728792003956564819968 : 57896044618658097711785492504343953926634992332820282019728792003956564819967\] |

Unsigned integer types have the following ranges:

| Type      | Range                                                                                  |
|-----------|----------------------------------------------------------------------------------------|
| `UInt8`   | \[0 : 255\]                                                                            |
| `UInt16`  | \[0 : 65535\]                                                                          |
| `UInt32`  | \[0 : 4294967295\]                                                                     |
| `UInt64`  | \[0 : 18446744073709551615\]                                                           |
| `UInt128` | \[0 : 340282366920938463463374607431768211455\]                                        |
| `UInt256` | \[0 : 115792089237316195423570985008687907853269984665640564039457584007913129639935\] |

## Integer Aliases {#integer-aliases}

Integer types have the following aliases:

| Type    | Alias                                                                             |
|---------|-----------------------------------------------------------------------------------|
| `Int8`  | `TINYINT`, `INT1`, `BYTE`, `TINYINT SIGNED`, `INT1 SIGNED`                        |
| `Int16` | `SMALLINT`, `SMALLINT SIGNED`                                                     |
| `Int32` | `INT`, `INTEGER`, `MEDIUMINT`, `MEDIUMINT SIGNED`, `INT SIGNED`, `INTEGER SIGNED` |
| `Int64` | `BIGINT`, `SIGNED`, `BIGINT SIGNED`, `TIME`                                       |

Unsigned integer types have the following aliases:

| Type     | Alias                                                    |
|----------|----------------------------------------------------------|
| `UInt8`  | `TINYINT UNSIGNED`, `INT1 UNSIGNED`                      |
| `UInt16` | `SMALLINT UNSIGNED`                                      |
| `UInt32` | `MEDIUMINT UNSIGNED`, `INT UNSIGNED`, `INTEGER UNSIGNED` |
| `UInt64` | `UNSIGNED`, `BIGINT UNSIGNED`, `BIT`, `SET`              |
)DOCS_MD",
            .syntax = "Int32",
            .related = {"Int64", "UInt32"},
        });
    factory.registerDataType("Int64", createNumericDataType<Int64>, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = "A 64-bit signed integer. See the `Int32` entry for the full documentation of the integer types.",
            .syntax = "Int64",
            .related = {"Int32"},
        });

    factory.registerDataType("BFloat16", createNumericDataType<BFloat16>, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = "A 16-bit floating-point number. See the `Float64` entry for the full documentation of the floating-point types.",
            .syntax = "BFloat16",
            .related = {"Float64"},
        });
    factory.registerDataType("Float32", createNumericDataType<Float32>, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = "A 32-bit floating-point number. See the `Float64` entry for the full documentation of the floating-point types.",
            .syntax = "Float32",
            .related = {"Float64"},
        });
    factory.registerDataType("Float64", createNumericDataType<Float64>, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = R"DOCS_MD(
:::note
If you need accurate calculations, in particular if you work with financial or business data requiring a high precision, you should consider using [Decimal](../data-types/decimal.md) instead.

[Floating Point Numbers](https://en.wikipedia.org/wiki/IEEE_754) might lead to inaccurate results as illustrated below:

```sql
CREATE TABLE IF NOT EXISTS float_vs_decimal
(
   my_float Float64,
   my_decimal Decimal64(3)
)
ENGINE=MergeTree
ORDER BY tuple();

# Generate 1 000 000 random numbers with 2 decimal places and store them as a float and as a decimal
INSERT INTO float_vs_decimal SELECT round(randCanonical(), 3) AS res, res FROM system.numbers LIMIT 1000000;
```
```sql
SELECT sum(my_float), sum(my_decimal) FROM float_vs_decimal;

┌──────sum(my_float)─┬─sum(my_decimal)─┐
│ 499693.60500000004 │      499693.605 │
└────────────────────┴─────────────────┘

SELECT sumKahan(my_float), sumKahan(my_decimal) FROM float_vs_decimal;

┌─sumKahan(my_float)─┬─sumKahan(my_decimal)─┐
│         499693.605 │           499693.605 │
└────────────────────┴──────────────────────┘
```
:::

The equivalent types in ClickHouse and in C are given below:

- `Float32` — `float`.
- `Float64` — `double`.

Float types in ClickHouse have the following aliases:

- `Float32` — `FLOAT`, `REAL`, `SINGLE`.
- `Float64` — `DOUBLE`, `DOUBLE PRECISION`.

When creating tables, numeric parameters for floating point numbers can be set (e.g. `FLOAT(12)`, `FLOAT(15, 22)`, `DOUBLE(12)`, `DOUBLE(4, 18)`), but ClickHouse ignores them.

## Using floating-point numbers {#using-floating-point-numbers}

- Computations with floating-point numbers might produce a rounding error.

<!-- -->

```sql
SELECT 1 - 0.9

┌───────minus(1, 0.9)─┐
│ 0.09999999999999998 │
└─────────────────────┘
```

- The result of the calculation depends on the calculation method (the processor type and architecture of the computer system).
- Floating-point calculations might result in numbers such as infinity (`Inf`) and "not-a-number" (`NaN`). This should be taken into account when processing the results of calculations.
- When parsing floating-point numbers from text, the result might not be the nearest machine-representable number.

## NaN and Inf {#nan-and-inf}

In contrast to standard SQL, ClickHouse supports the following categories of floating-point numbers:

- `Inf` – Infinity.

<!-- -->

```sql
SELECT 0.5 / 0

┌─divide(0.5, 0)─┐
│            inf │
└────────────────┘
```

- `-Inf` — Negative infinity.

<!-- -->

```sql
SELECT -0.5 / 0

┌─divide(-0.5, 0)─┐
│            -inf │
└─────────────────┘
```

- `NaN` — Not a number.

<!-- -->

```sql
SELECT 0 / 0

┌─divide(0, 0)─┐
│          nan │
└──────────────┘
```

See the rules for `NaN` sorting in the section [ORDER BY clause](../../sql-reference/statements/select/order-by.md).

## NaN values in set semantics {#nan-values-in-set-semantics}

The IEEE 754 standard defines `NaN` such that the scalar comparison `NaN = NaN` returns `false`.
ClickHouse follows that rule for the `=` operator.

However, `NaN` is not a single value; it is any bit pattern whose exponent is all ones and whose
mantissa is non-zero. Different operations and different CPU architectures can produce `NaN`
values with different sign bits or different mantissa payloads. For example:

- `0./0.` produces a `NaN` whose sign bit is 1 on most x86 platforms.
- The literal `nan` produces a `NaN` whose sign bit is 0.
- After [PR #98230](https://github.com/ClickHouse/ClickHouse/pull/98230), the AArch64 NEON path of
  `log` returns a `NaN` whose sign bit differs from glibc's scalar `log` on negative inputs.

Hash tables in ClickHouse compare keys byte-wise, so different `NaN` bit patterns hash to
different buckets and are treated as distinct values by set-semantics operations including
`DISTINCT`, `GROUP BY`, `uniqExact`, `countDistinct`, and equi-`JOIN` on a `Float` key:

```sql
SELECT countDistinct(arrayJoin([0./0., nan, log(-1.)]));
-- May return 2 or 3 depending on architecture and build, even though all three inputs are NaN.
```

This is consistent with IEEE 754 (every `NaN` is unequal to every other value, including itself)
but can be surprising. If you need set-semantics operations to treat all `NaN` values as equal,
canonicalize them in the query:

```sql
-- Replace every NaN with a single canonical NaN value
SELECT countDistinct(if(isNaN(x), CAST('nan' AS Float64), x))
FROM (SELECT arrayJoin([0./0., nan, log(-1.)]) AS x);
-- Returns 1.

-- Or exclude NaN values from the set entirely
SELECT countDistinct(if(isNaN(x), NULL, x))
FROM (SELECT arrayJoin([0./0., nan, log(-1.)]) AS x);
-- Returns 0.
```

The same approach works for `DISTINCT`, `GROUP BY`, and `JOIN` keys.

## BFloat16 {#bfloat16}

`BFloat16` is a 16-bit floating point data type with 8-bit exponent, sign, and 7-bit mantissa.
It is useful for machine learning and AI applications.

ClickHouse supports conversions between `Float32` and `BFloat16` which
can be done using the [`toFloat32()`](../functions/type-conversion-functions.md/#toFloat32) or [`toBFloat16`](../functions/type-conversion-functions.md/#toBFloat16) functions.

:::note
Most other operations are not supported.
:::
)DOCS_MD",
            .syntax = "Float64",
            .related = {"Float32", "Decimal"},
        });

    factory.registerSimpleDataType("UInt128", [] { return DataTypePtr(std::make_shared<DataTypeUInt128>()); }, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = "A 128-bit unsigned integer. See the `Int32` entry for the full documentation of the integer types.",
            .syntax = "UInt128",
            .related = {"Int32"},
        });
    factory.registerSimpleDataType("UInt256", [] { return DataTypePtr(std::make_shared<DataTypeUInt256>()); }, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = "A 256-bit unsigned integer. See the `Int32` entry for the full documentation of the integer types.",
            .syntax = "UInt256",
            .related = {"Int32"},
        });

    factory.registerSimpleDataType("Int128", [] { return DataTypePtr(std::make_shared<DataTypeInt128>()); }, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = "A 128-bit signed integer. See the `Int32` entry for the full documentation of the integer types.",
            .syntax = "Int128",
            .related = {"Int32"},
        });
    factory.registerSimpleDataType("Int256", [] { return DataTypePtr(std::make_shared<DataTypeInt256>()); }, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = "A 256-bit signed integer. See the `Int32` entry for the full documentation of the integer types.",
            .syntax = "Int256",
            .related = {"Int32"},
        });

    /// These synonyms are added for compatibility.

    factory.registerAlias("TINYINT", "Int8", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("INT1", "Int8", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("BYTE", "Int8", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("TINYINT SIGNED", "Int8", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("INT1 SIGNED", "Int8", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("SMALLINT", "Int16", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("SMALLINT SIGNED", "Int16", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("INT", "Int32", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("INTEGER", "Int32", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("MEDIUMINT", "Int32", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("MEDIUMINT SIGNED", "Int32", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("INT SIGNED", "Int32", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("INTEGER SIGNED", "Int32", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("BIGINT", "Int64", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("SIGNED", "Int64", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("BIGINT SIGNED", "Int64", DataTypeFactory::Case::Insensitive);

    factory.registerAlias("TINYINT UNSIGNED", "UInt8", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("INT1 UNSIGNED", "UInt8", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("SMALLINT UNSIGNED", "UInt16", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("YEAR", "UInt16", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("MEDIUMINT UNSIGNED", "UInt32", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("INT UNSIGNED", "UInt32", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("INTEGER UNSIGNED", "UInt32", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("UNSIGNED", "UInt64", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("BIGINT UNSIGNED", "UInt64", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("BIT", "UInt64", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("SET", "UInt64", DataTypeFactory::Case::Insensitive);

    factory.registerAlias("FLOAT", "Float32", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("REAL", "Float32", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("SINGLE", "Float32", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("DOUBLE", "Float64", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("DOUBLE PRECISION", "Float64", DataTypeFactory::Case::Insensitive);
}

/// Explicit template instantiations.
template class DataTypeNumber<UInt8>;
template class DataTypeNumber<UInt16>;
template class DataTypeNumber<UInt32>;
template class DataTypeNumber<UInt64>;
template class DataTypeNumber<Int8>;
template class DataTypeNumber<Int16>;
template class DataTypeNumber<Int32>;
template class DataTypeNumber<Int64>;
template class DataTypeNumber<BFloat16>;
template class DataTypeNumber<Float32>;
template class DataTypeNumber<Float64>;

template class DataTypeNumber<UInt128>;
template class DataTypeNumber<Int128>;
template class DataTypeNumber<UInt256>;
template class DataTypeNumber<Int256>;


DataTypePtr getSmallestIndexesType(size_t num_indexes)
{
    if (num_indexes <= std::numeric_limits<UInt8>::max() + 1ull)
        return std::make_shared<DataTypeUInt8>();
    if (num_indexes <= std::numeric_limits<UInt16>::max() + 1ull)
        return std::make_shared<DataTypeUInt16>();
    if (num_indexes <= std::numeric_limits<UInt32>::max() + 1ull)
        return std::make_shared<DataTypeUInt32>();
    return std::make_shared<DataTypeUInt64>();
}

}
