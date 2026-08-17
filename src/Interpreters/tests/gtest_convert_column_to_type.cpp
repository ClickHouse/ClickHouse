#include <limits>
#include <string>
#include <vector>

#include <Core/Field.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/convertColumnToType.h>
#include <Interpreters/convertFieldToType.h>

#include <gtest/gtest.h>
#include <base/types.h>
#include <base/Decimal.h>

using namespace DB;

namespace
{

/// `convertColumnToTypeOrNull` must give exactly the same answer as `convertFieldToType`: for every
/// (from_type, value, to_type, strict, convert_inexact_floats), the column twin returns null iff the
/// Field version returns a Null Field, and otherwise its single value equals the Field result. This
/// pins the contract so column-native fast paths can replace the delegation without changing results.
struct Case
{
    const char * from_type;
    Field from_value;
    const char * to_type;
    bool strict = false;
    bool inexact = false;
};

void checkEquivalent(const Case & c)
{
    const auto & type_factory = DataTypeFactory::instance();
    const auto from = type_factory.get(c.from_type);
    const auto to = type_factory.get(c.to_type);

    /// Oracle. `try` variant so throwing cases (bad string, tuple size mismatch, ...) map to Null on
    /// both sides uniformly instead of crashing the test.
    const Field expected = tryConvertFieldToType(c.from_value, *to, from.get(), {}, c.strict, c.inexact);

    auto column = from->createColumn();
    column->insert(c.from_value);
    const ColumnPtr actual = tryConvertColumnToTypeOrNull(*column, from, to, {}, c.strict, c.inexact);

    /// Real callers (e.g. evaluateConstantExpressionAsColumn) pass a ColumnConst; it must work too.
    const auto const_column = ColumnConst::create(std::move(column), 1);
    const ColumnPtr actual_const = tryConvertColumnToTypeOrNull(*const_column, from, to, {}, c.strict, c.inexact);

    SCOPED_TRACE(std::string(c.from_type) + " -> " + c.to_type + (c.strict ? " strict" : "")
        + (c.inexact ? " inexact" : ""));

    /// The const and non-const inputs must agree.
    ASSERT_EQ(actual == nullptr, actual_const == nullptr);
    if (actual && actual_const)
        EXPECT_EQ(actual->compareAt(0, 0, *actual_const->convertToFullColumnIfConst(), 1), 0);

    if (expected.isNull())
    {
        /// `convertFieldToType` returns a Null `Field` both for a legitimate NULL result (NULL input
        /// into a nullable-capable type) and for "not representable". The column twin distinguishes
        /// them: a valid NULL is a size-1 column holding NULL; "not representable" is `ColumnPtr{}`.
        if (c.from_value.isNull() && canContainNull(*to))
        {
            ASSERT_NE(actual, nullptr);
            ASSERT_EQ(actual->size(), 1u);
            EXPECT_TRUE(actual->isNullAt(0));
        }
        else
        {
            EXPECT_EQ(actual, nullptr);
        }
    }
    else
    {
        ASSERT_NE(actual, nullptr);
        ASSERT_EQ(actual->size(), 1u);
        /// Compare the value as stored in a column of `to` (what callers get), rather than comparing
        /// raw `Field`s: reading a value back out of a column canonicalizes its `Field` tag
        /// (e.g. `Bool` -> `UInt64`), so a raw `Field` `==` would spuriously differ from
        /// `convertFieldToType`'s result even when the values are identical.
        auto expected_column = to->createColumn();
        expected_column->insert(expected);
        EXPECT_EQ(actual->compareAt(0, 0, *expected_column, 1), 0);
    }
}

}

TEST(ConvertColumnToType, MatchesConvertFieldToType)
{
    const double nan = std::numeric_limits<double>::quiet_NaN();
    const double inf = std::numeric_limits<double>::infinity();

    const std::vector<Case> cases = {
        /// integers: widen / narrow / overflow / sign
        {"Int32", Field(Int64(5)), "Int64"},
        {"UInt64", Field(UInt64(5)), "UInt8"},
        {"UInt64", Field(UInt64(256)), "UInt8"},          // overflow -> null
        {"Int64", Field(Int64(-1)), "UInt8"},             // negative -> null
        {"UInt64", Field(UInt64(255)), "UInt8"},
        {"Int64", Field(Int64(-128)), "Int8"},
        {"Int64", Field(Int64(-129)), "Int8"},            // overflow -> null

        /// wide integers (Int128/256, UInt128/256): `convertFieldToType` routes them through the same
        /// `accurate::convertNumeric` as native numbers, so the column-native fast path serves them too.
        {"Int64", Field(Int64(5)), "Int128"},                           // widen native -> wide
        {"UInt64", Field(UInt64(5)), "UInt256"},                        // widen native -> wide
        {"Int128", Field(Int128(5)), "Int64"},                          // wide -> native, in range
        {"Int128", Field(Int128(5)), "Int256"},                         // widen wide -> wide
        {"Int256", Field(Int256(200)), "UInt8"},                        // wide -> native, in range
        {"Int256", Field(Int256(300)), "UInt8"},                        // overflow -> null
        {"Int128", Field(Int128(-1)), "UInt64"},                        // negative -> null
        {"Int128", Field(Int128(1) << 70), "Int64"},                    // wide overflows native -> null
        {"Int128", Field(Int128(1) << 70), "Int256"},                   // wide -> wider, exact
        {"UInt256", Field(UInt256(255)), "UInt8"},                      // wide -> native, in range
        {"Float64", Field(Float64(5.0)), "Int128"},                     // float -> wide, exact
        {"Int128", Field(Int128(5)), "Float64"},                        // wide -> float
        /// wide integers, strict
        {"Int128", Field(Int128(5)), "Int64", true},                    // in range -> 5
        {"Int128", Field(Int128(1) << 70), "Int64", true},              // overflow -> null
        {"Int256", Field(Int256(300)), "UInt8", true},                  // overflow -> null
        {"Float64", Field(Float64(3.0)), "Int128", true},               // exact -> 3
        {"Float64", Field(Float64(3.5)), "Int128", true},               // non-integer -> null
        {"UInt64", Field(UInt64(5)), "Int256", true},                   // widen across sign -> 5
        /// wide int -> float precision loss: strict must reject (this is what IN/set building relies on)
        {"Int128", Field(Int128(9007199254740993ll)), "Float64"},        // 2^53+1: default -> nearest float
        {"Int128", Field(Int128(9007199254740993ll)), "Float64", true},  // 2^53+1: strict -> null
        {"UInt256", Field(UInt256(9007199254740993ull)), "Float64", true}, // strict -> null
        {"Int128", Field(Int128(16777217)), "Float32", true},            // 2^24+1: strict -> null (Float32)

        /// floats: exact / inexact in all three modes / overflow / NaN / inf
        {"Float64", Field(Float64(0.5)), "Float32"},
        {"Float64", Field(Float64(0.1)), "Float32"},                      // default: exact -> null
        {"Float64", Field(Float64(0.1)), "Float32", false, true},        // inexact -> 0.1f
        {"Float64", Field(Float64(0.1)), "Float32", true, false},        // strict -> null
        {"Float64", Field(Float64(1e300)), "Float32", false, true},      // overflow -> null
        {"Float64", Field(nan), "Float32"},
        {"Float64", Field(nan), "Float64"},
        {"Float64", Field(inf), "Float32"},
        {"Float64", Field(Float64(3.25)), "Int32"},
        {"Float64", Field(Float64(3.9)), "Int32"},

        /// bool clamp vs strict {0,1}
        {"UInt64", Field(UInt64(255)), "Bool"},
        {"UInt64", Field(UInt64(255)), "Bool", true, false},             // strict -> null
        {"UInt64", Field(UInt64(1)), "Bool", true, false},

        /// decimals
        {"Decimal64(2)", Field(DecimalField<Decimal64>(Decimal64(3333), 2)), "Decimal64(1)"},              // 33.33 -> 33.3 (round)
        {"Decimal64(2)", Field(DecimalField<Decimal64>(Decimal64(3333), 2)), "Decimal64(1)", true, false}, // strict -> null
        {"Int64", Field(Int64(5)), "Decimal64(2)"},

        /// more decimals (column-native path): int/wide/float/decimal sources into every width, default + strict
        {"UInt64", Field(UInt64(42)), "Decimal64(2)"},                                                     // 42.00
        {"Int64", Field(Int64(-5)), "Decimal64(2)"},                                                       // -5.00
        {"UInt64", Field(UInt64(1000000000000000000ull)), "Decimal32(0)"},                                 // too big -> throws -> null
        {"Int128", Field(Int128(5)), "Decimal128(3)"},                                                     // wide int -> 5.000
        {"UInt256", Field(UInt256(7)), "Decimal256(2)"},                                                   // wide int -> 7.00
        {"Float64", Field(Float64(0.5)), "Decimal64(2)"},                                                  // 0.50
        {"Float64", Field(Float64(0.5)), "Decimal64(2)", true},                                            // exact -> 0.50
        {"Float64", Field(Float64(0.125)), "Decimal64(2)", true},                                          // inexact -> null
        {"Decimal32(2)", Field(DecimalField<Decimal32>(Decimal32(333), 2)), "Decimal64(4)"},               // widen 3.33 -> 3.3300
        {"Decimal128(4)", Field(DecimalField<Decimal128>(Decimal128(12345), 4)), "Decimal64(2)"},          // narrow 1.2345 -> 1.23 (round)
        {"Decimal128(4)", Field(DecimalField<Decimal128>(Decimal128(12345), 4)), "Decimal64(2)", true},    // narrow lossy -> null
        {"Decimal64(1)", Field(DecimalField<Decimal64>(Decimal64(333), 1)), "Decimal128(3)", true},        // widen exact -> 33.300

        /// string <-> number
        {"String", Field(String("42")), "Int32"},
        {"String", Field(String("256")), "UInt8"},                       // out of range -> null
        {"String", Field(String("not a number")), "Int32"},              // throws -> null (via try)
        {"UInt64", Field(UInt64(42)), "String"},

        /// date / datetime
        {"UInt16", Field(UInt64(19000)), "Date"},
        {"Date", Field(UInt64(19000)), "DateTime('UTC')"},

        /// numeric -> date family (column-native path)
        {"UInt64", Field(UInt64(19000)), "Date"},                         // 19000
        {"Int64", Field(Int64(19000)), "Date"},                          // 19000
        {"UInt64", Field(UInt64(70000)), "Date"},                        // out of UInt16 range -> null
        {"Int64", Field(Int64(-1)), "Date"},                             // negative -> null
        {"Int64", Field(Int64(19000)), "Date32"},                        // in window
        {"UInt64", Field(UInt64(19000)), "Date32"},                      // in window
        {"Int64", Field(Int64(3000000)), "Date32"},                      // > max extended day -> null
        {"Int64", Field(Int64(-800000)), "Date32"},                      // < min extended day -> null
        {"UInt64", Field(UInt64(1700000000)), "DateTime('UTC')"},        // fits UInt32
        {"UInt64", Field(UInt64(5000000000)), "DateTime('UTC')"},        // > UInt32 max -> truncates (raw, no range check)

        /// cross-calendar Date/Date32 <-> DateTime (timezone-aware): day 19000 == 2022-01-08 == 1641600000 UTC
        {"DateTime('UTC')", Field(UInt64(1641600000)), "Date"},          // -> day 19000
        {"Date32", Field(Int64(19000)), "DateTime('UTC')"},              // -> 1641600000
        {"DateTime('UTC')", Field(UInt64(1641600000)), "Date32"},        // -> day 19000
        /// non-UTC: proves the timezone object of the DateTime type is actually consulted
        {"Date32", Field(Int64(19000)), "DateTime('Europe/Berlin')"},    // fromDayNum in Berlin tz
        {"DateTime('Europe/Berlin')", Field(UInt64(1641600000)), "Date"}, // toDayNum in Berlin tz
        {"DateTime('Europe/Berlin')", Field(UInt64(1641600000)), "Date32"},

        /// nullable / lowcardinality wrappers
        {"Nullable(Int32)", Field(Int64(7)), "Int64"},
        {"Nullable(Int32)", Field(Null()), "Int64"},                     // null in, non-null to -> not representable
        {"Nullable(Int32)", Field(Int64(7)), "Nullable(Int64)"},
        {"Nullable(Int32)", Field(Null()), "Nullable(Int64)"},           // null in, nullable to -> valid NULL
        {"LowCardinality(String)", Field(String("x")), "String"},

        /// composites: element conversion + "unconvertible element -> whole null"
        {"Array(UInt8)", Field(Array{UInt64(1), UInt64(2)}), "Array(Int64)"},
        {"Array(UInt64)", Field(Array{UInt64(256)}), "Array(UInt8)"},     // element overflow -> whole null
        {"Tuple(UInt8, String)", Field(Tuple{UInt64(1), String("a")}), "Tuple(Int64, String)"},

        /// `Bool`-source. `IColumn::get` on a `DataTypeBool` column (backed by `ColumnUInt8`) yields a
        /// `UInt64` `Field`, but `convertColumnToTypeOrNull` re-tags it back to `Bool` before delegating
        /// (see `retagBoolInField`), so it matches `convertFieldToType` for tag-sensitive targets too:
        /// `Bool -> String` gives 'true'/'false', not '1'/'0'. Numeric targets are value-preserving
        /// either way. Nested `Bool` (Array/Tuple/Map, and under Nullable) is re-tagged recursively.
        {"Bool", Field(true), "Int32"},
        {"Bool", Field(false), "UInt8"},
        {"Bool", Field(true), "String"},
        {"Bool", Field(false), "String"},
        {"Nullable(Bool)", Field(true), "Nullable(String)"},
        {"Array(Bool)", Field(Array{true, false}), "Array(String)"},
        {"Tuple(Bool, UInt8)", Field(Tuple{true, UInt64(7)}), "Tuple(String, String)"},
        {"Map(String, Bool)", Field(Map{Tuple{String("k"), true}}), "Map(String, String)"},

        /// Controls: other custom/dedicated-column types already round-trip their `Field` tag through
        /// `IColumn::get`, so their textual conversions match `convertFieldToType` without re-tagging.
        {"Enum8('a' = 1, 'b' = 2)", Field(Int64(1)), "String"},
        {"Date", Field(UInt64(19000)), "String"},
        {"Decimal64(2)", Field(DecimalField<Decimal64>(Decimal64(3333), 2)), "String"},
        {"IPv4", Field(IPv4(0x7f000001)), "String"},

        /// `strict` native-number matrix. `convertColumnToType` now serves `strict` for native numbers
        /// via the column-native fast path (`castColumnAccurateOrNull`), which must match
        /// `convertFieldToType` with `strict=true`. This is what the IN/set set builder relies on
        /// (it converts with `strict=true` to exclude values not exactly representable in the LHS type).
        {"UInt64", Field(UInt64(5)), "UInt8", true},                       // in range -> 5
        {"UInt64", Field(UInt64(256)), "UInt8", true},                     // overflow -> null
        {"Int64", Field(Int64(-1)), "UInt8", true},                        // negative -> null
        {"Int64", Field(Int64(-128)), "Int8", true},                       // in range -> -128
        {"Int64", Field(Int64(-129)), "Int8", true},                       // overflow -> null
        {"Float64", Field(Float64(3.0)), "Int32", true},                   // exact -> 3
        {"Float64", Field(Float64(3.5)), "Int32", true},                   // non-integer -> null
        {"Float64", Field(Float64(0.5)), "Float32", true},                 // exact -> 0.5
        {"Float64", Field(Float64(1e300)), "Float32", true},               // overflow -> null
        {"Int64", Field(Int64(9007199254740993ll)), "Float64", true},      // int -> float precision loss
        {"Int64", Field(Int64(5)), "Float32", true},                       // exact int -> float
        {"UInt64", Field(UInt64(5)), "Int8", true},                        // in range across sign -> 5

        /// `strict` non-native controls: these keep going through the `Field` fallback (Decimal must NOT
        /// use `castColumnAccurateOrNull`, which would round `33.33` to `33.3` instead of rejecting it).
        {"Decimal64(2)", Field(DecimalField<Decimal64>(Decimal64(3333), 2)), "Decimal64(1)", true}, // scale loss -> null
        {"Decimal64(1)", Field(DecimalField<Decimal64>(Decimal64(333), 1)), "Decimal64(2)", true},  // widen -> 33.30
    };

    for (const auto & c : cases)
        checkEquivalent(c);
}

TEST(ConvertColumnToType, OrThrow)
{
    const auto & type_factory = DataTypeFactory::instance();
    const auto u64 = type_factory.get("UInt64");
    const auto u8 = type_factory.get("UInt8");

    auto in_range = u64->createColumn();
    in_range->insert(Field(UInt64(200)));
    const ColumnPtr ok = convertColumnToTypeOrThrow(*in_range, u64, u8);
    ASSERT_NE(ok, nullptr);
    EXPECT_EQ((*ok)[0], Field(UInt64(200)));

    auto out_of_range = u64->createColumn();
    out_of_range->insert(Field(UInt64(256)));
    EXPECT_ANY_THROW(convertColumnToTypeOrThrow(*out_of_range, u64, u8));

    /// NULL handling mirrors convertFieldToTypeOrThrow: NULL into a non-nullable target throws;
    /// NULL into a nullable target is a valid NULL result (a size-1 column holding NULL).
    const auto nullable_i32 = type_factory.get("Nullable(Int32)");
    const auto nullable_i64 = type_factory.get("Nullable(Int64)");
    const auto i64 = type_factory.get("Int64");

    auto null_value = nullable_i32->createColumn();
    null_value->insert(Field());
    EXPECT_ANY_THROW(convertColumnToTypeOrThrow(*null_value, nullable_i32, i64));

    const ColumnPtr null_ok = convertColumnToTypeOrThrow(*null_value, nullable_i32, nullable_i64);
    ASSERT_NE(null_ok, nullptr);
    ASSERT_EQ(null_ok->size(), 1u);
    EXPECT_TRUE(null_ok->isNullAt(0));
}
