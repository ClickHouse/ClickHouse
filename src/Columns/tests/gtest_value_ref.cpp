#include <gtest/gtest.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ValueRef.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <Common/SipHash.h>

#include <vector>

using namespace DB;

namespace
{

/// Build a column of `type` and append every value in `values` (given as `Field`s).
ColumnPtr makeColumn(const DataTypePtr & type, const std::vector<Field> & values)
{
    auto column = type->createColumn();
    for (const auto & value : values)
        column->insert(value);
    return column;
}

/// For every row: a `ValueRef` must reproduce exactly what `operator[]` / `get` return, and match
/// the column's own null/default/hash answers. This is the core "the wrapper adds nothing but
/// convenience" guarantee.
void checkRoundTrip(const IColumn & column)
{
    for (size_t row = 0; row < column.size(); ++row)
    {
        ValueRef ref(column, row);
        ASSERT_TRUE(ref.isValid());

        /// toField() parity with operator[].
        EXPECT_EQ(ref.toField(), column[row]) << "row " << row;

        /// toField(Field &) parity with get(row, Field &).
        Field via_out;
        ref.toField(via_out);
        Field via_get;
        column.get(row, via_get);
        EXPECT_EQ(via_out, via_get) << "row " << row;

        /// null / default parity.
        EXPECT_EQ(ref.isNull(), column.isNullAt(row)) << "row " << row;
        EXPECT_EQ(ref.isDefault(), column.isDefaultAt(row)) << "row " << row;

        /// hash parity.
        SipHash ref_hash;
        ref.updateHashWithValue(ref_hash);
        SipHash col_hash;
        column.updateHashWithValue(row, col_hash);
        EXPECT_EQ(ref_hash.get64(), col_hash.get64()) << "row " << row;
    }
}

/// Within one column, ValueRef::compareAt must agree with IColumn::compareAt for every pair.
void checkComparisonParity(const IColumn & column, int nan_direction_hint)
{
    for (size_t i = 0; i < column.size(); ++i)
    {
        for (size_t j = 0; j < column.size(); ++j)
        {
            ValueRef a(column, i);
            ValueRef b(column, j);
            EXPECT_EQ(
                a.compareAt(b, nan_direction_hint),
                column.compareAt(i, j, column, nan_direction_hint))
                << "pair (" << i << ", " << j << ")";
        }
    }
}

}

TEST(ValueRef, DefaultConstructedIsInvalid)
{
    ValueRef ref;
    EXPECT_FALSE(ref.isValid());
    EXPECT_EQ(ref.column, nullptr);
}

TEST(ValueRef, Numbers)
{
    /// UInt8 exercises the type-fidelity point: operator[] on ValueRef reflects what the column
    /// stores; unlike a manual NearestFieldType round trip there is no surprise here beyond what
    /// IColumn itself does.
    auto column = makeColumn(std::make_shared<DataTypeUInt8>(), {UInt64(0), UInt64(1), UInt64(255), UInt64(7)});
    checkRoundTrip(*column);
    checkComparisonParity(*column, 1);
    checkComparisonParity(*column, -1);
}

TEST(ValueRef, Floats)
{
    auto column = makeColumn(
        std::make_shared<DataTypeFloat64>(),
        {Float64(-1.5), Float64(0.0), Float64(3.25), std::numeric_limits<Float64>::quiet_NaN()});
    checkRoundTrip(*column);
    /// NaN ordering depends on nan_direction_hint; both must stay consistent with the column.
    checkComparisonParity(*column, 1);
    checkComparisonParity(*column, -1);
}

TEST(ValueRef, Strings)
{
    auto column = makeColumn(std::make_shared<DataTypeString>(), {"", "a", "abc", "abd", "zzz"});
    checkRoundTrip(*column);
    checkComparisonParity(*column, 1);
}

TEST(ValueRef, Nullable)
{
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto column = makeColumn(type, {Int64(-5), Null(), Int64(0), Int64(42), Null()});
    checkRoundTrip(*column);
    checkComparisonParity(*column, 1);
    checkComparisonParity(*column, -1);
}

TEST(ValueRef, Array)
{
    auto type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    auto column = makeColumn(
        type,
        {Array{}, Array{UInt64(1)}, Array{UInt64(1), UInt64(2)}, Array{UInt64(1), UInt64(3)}});
    checkRoundTrip(*column);
    checkComparisonParity(*column, 1);
}

TEST(ValueRef, LowCardinality)
{
    auto type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    auto column = makeColumn(type, {"red", "green", "red", "blue", "green"});
    checkRoundTrip(*column);
    checkComparisonParity(*column, 1);
}

TEST(ValueRef, Const)
{
    auto inner = makeColumn(std::make_shared<DataTypeString>(), {"same"});
    auto column = ColumnConst::create(std::move(inner), 4);
    checkRoundTrip(*column);
    checkComparisonParity(*column, 1);
}

TEST(ValueRef, CompareAcrossColumnsSameType)
{
    auto lhs = makeColumn(std::make_shared<DataTypeInt64>(), {Int64(10), Int64(20)});
    auto rhs = makeColumn(std::make_shared<DataTypeInt64>(), {Int64(15), Int64(20)});

    ValueRef a(*lhs, 0);   // 10
    ValueRef b(*rhs, 0);   // 15
    ValueRef c(*rhs, 1);   // 20
    ValueRef d(*lhs, 1);   // 20

    EXPECT_LT(a.compareAt(b, 1), 0);   // 10 < 15
    EXPECT_GT(b.compareAt(a, 1), 0);   // 15 > 10
    EXPECT_EQ(c.compareAt(d, 1), 0);   // 20 == 20 across columns
}

TEST(ValueRef, InsertIntoRoundTrips)
{
    auto source = makeColumn(std::make_shared<DataTypeString>(), {"x", "yy", "zzz"});
    auto dest = std::make_shared<DataTypeString>()->createColumn();

    for (size_t row = 0; row < source->size(); ++row)
        ValueRef(*source, row).insertInto(*dest);

    ASSERT_EQ(dest->size(), source->size());
    for (size_t row = 0; row < source->size(); ++row)
        EXPECT_EQ((*dest)[row], (*source)[row]) << "row " << row;
}
