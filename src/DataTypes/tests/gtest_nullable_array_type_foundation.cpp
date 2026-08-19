#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

DataTypePtr arrayOfUInt8()
{
    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt8>());
}

MutableColumnPtr makeArrayUInt8Column()
{
    auto values = ColumnUInt8::create();
    values->insertValue(1);
    values->insertValue(2);

    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->getData().push_back(2);

    return ColumnArray::create(std::move(values), std::move(offsets));
}

}

/// `Array` must keep reporting that it cannot be inside `Nullable`. The whole design rests on this:
/// because the predicate never changes meaning, no pre-existing code path can start producing
/// `Nullable(Array(...))` by itself, so only the explicit opt-in sites below can.
TEST(NullableArrayFoundation, ArrayStillCannotBeInsideNullable)
{
    EXPECT_FALSE(arrayOfUInt8()->canBeInsideNullable());
    EXPECT_FALSE(makeArrayUInt8Column()->canBeInsideNullable());
}

TEST(NullableArrayFoundation, MakeNullableRejectsArray)
{
    EXPECT_THROW(makeNullable(arrayOfUInt8()), Exception);
}

/// `makeNullableSafe` returns the type unchanged rather than throwing, which is how a
/// `Nullable(Array)` can be silently degraded to `Array`. Pinned so the behaviour is deliberate.
TEST(NullableArrayFoundation, MakeNullableSafeLeavesArrayUnwrapped)
{
    auto type = arrayOfUInt8();
    EXPECT_TRUE(makeNullableSafe(type)->equals(*type));
    EXPECT_FALSE(makeNullableSafe(type)->isNullable());
}

TEST(NullableArrayFoundation, MakeNullableAllowingArrayWrapsArray)
{
    auto type = makeNullableAllowingArray(arrayOfUInt8());

    ASSERT_TRUE(type->isNullable());
    EXPECT_EQ(type->getName(), "Nullable(Array(UInt8))");
    EXPECT_TRUE(removeNullable(type)->equals(*arrayOfUInt8()));
}

TEST(NullableArrayFoundation, MakeNullableAllowingArrayIsIdempotent)
{
    auto once = makeNullableAllowingArray(arrayOfUInt8());
    auto twice = makeNullableAllowingArray(once);

    EXPECT_TRUE(twice->equals(*once));
}

TEST(NullableArrayFoundation, MakeNullableAllowingArrayStillHonoursOtherRestrictions)
{
    /// The escape hatch is for `Array` only; it must not become a way to wrap anything at all.
    auto map_type = DataTypeFactory::instance().get("Map(String, UInt8)");
    EXPECT_THROW(makeNullableAllowingArray(map_type), Exception);
}

/// Stage boundary: the type is constructible from C++ but must remain unreachable by name, so that
/// no query, DDL statement or type-string argument can produce it before the gate is introduced.
TEST(NullableArrayFoundation, TypeStringIsStillRejected)
{
    EXPECT_THROW(DataTypeFactory::instance().get("Nullable(Array(UInt8))"), Exception);
    EXPECT_THROW(DataTypeFactory::instance().get("Tuple(Nullable(Array(UInt8)))"), Exception);
}

TEST(NullableArrayFoundation, ColumnNullableAcceptsArrayWithNonConstNullMap)
{
    auto nested = makeArrayUInt8Column();
    auto null_map = ColumnUInt8::create();
    null_map->insertValue(0);

    ColumnPtr column;
    ASSERT_NO_THROW(column = ColumnNullable::create(std::move(nested), std::move(null_map)));
    EXPECT_EQ(column->size(), 1u);
}

TEST(NullableArrayFoundation, ColumnNullableRejectsConstantNullMapForArray)
{
    auto nested = makeArrayUInt8Column();
    auto null_map_data = ColumnUInt8::create();
    null_map_data->insertValue(0);
    auto const_null_map = ColumnConst::create(std::move(null_map_data), 1);

    EXPECT_THROW(
        ColumnNullable::create(nested->assumeMutable(), const_null_map->assumeMutable()),
        Exception);
}

/// A `Nullable(Array)` column and its type must agree, since the two are built through
/// independent opt-ins and a mismatch would surface far away from here.
TEST(NullableArrayFoundation, TypeCreatesMatchingColumn)
{
    auto type = makeNullableAllowingArray(arrayOfUInt8());
    auto column = type->createColumn();

    ASSERT_EQ(column->getDataType(), TypeIndex::Nullable);
    const auto & nullable = assert_cast<const ColumnNullable &>(*column);
    EXPECT_EQ(nullable.getNestedColumn().getDataType(), TypeIndex::Array);
    EXPECT_TRUE(column->empty());
}

TEST(NullableArrayFoundation, DefaultValueIsNull)
{
    auto type = makeNullableAllowingArray(arrayOfUInt8());
    auto column = type->createColumn();
    type->insertDefaultInto(*column);

    ASSERT_EQ(column->size(), 1u);
    EXPECT_TRUE(column->isNullAt(0));
}
