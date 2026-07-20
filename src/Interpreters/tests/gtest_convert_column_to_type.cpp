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

        /// string <-> number
        {"String", Field(String("42")), "Int32"},
        {"String", Field(String("256")), "UInt8"},                       // out of range -> null
        {"String", Field(String("not a number")), "Int32"},              // throws -> null (via try)
        {"UInt64", Field(UInt64(42)), "String"},

        /// date / datetime
        {"UInt16", Field(UInt64(19000)), "Date"},
        {"Date", Field(UInt64(19000)), "DateTime('UTC')"},

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
