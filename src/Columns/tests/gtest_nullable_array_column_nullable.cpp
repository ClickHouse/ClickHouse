#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <gtest/gtest.h>

namespace DB
{

namespace
{

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

TEST(ColumnNullable, NullableArrayRejectsConstantNullMap)
{
    auto nested = makeArrayUInt8Column();
    auto null_map_data = ColumnUInt8::create();
    null_map_data->insertValue(0);
    auto const_null_map = ColumnConst::create(std::move(null_map_data), 1);

    EXPECT_THROW(
        ColumnNullable::create(nested->assumeMutable(), const_null_map->assumeMutable()),
        Exception);
}

TEST(ColumnNullable, NullableArrayAcceptsNonConstNullMap)
{
    auto nested = makeArrayUInt8Column();
    auto null_map = ColumnUInt8::create();
    null_map->insertValue(0);

    EXPECT_NO_THROW(ColumnNullable::create(nested->assumeMutable(), std::move(null_map)));
}

}
