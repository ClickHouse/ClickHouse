#include <Columns/ColumnMap.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

template <typename T>
ColumnMap::MutablePtr createEmptyMap()
{
    auto keys = T::create();
    auto values = T::create();
    auto offsets = ColumnArray::ColumnOffsets::create();
    auto nested_data = ColumnTuple::create(Columns{std::move(keys), std::move(values)});
    auto nested = ColumnArray::create(std::move(nested_data), std::move(offsets));
    return ColumnMap::create(std::move(nested));
}

ColumnMap::MutablePtr createMap()
{
    auto keys = ColumnUInt64::create();
    keys->insert(10);
    keys->insert(20);

    auto values = ColumnUInt64::create();
    values->insert(100);
    values->insert(200);

    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->getData().push_back(1);
    offsets->getData().push_back(1);
    offsets->getData().push_back(2);

    auto nested_data = ColumnTuple::create(Columns{std::move(keys), std::move(values)});
    auto nested = ColumnArray::create(std::move(nested_data), std::move(offsets));
    return ColumnMap::create(std::move(nested));
}

}

TEST(ColumnMap, InsertManyDefaultsForEmptyMaps)
{
    auto map = createEmptyMap<ColumnUInt64>();

    const auto key_capacity = map->getNestedData().getColumn(0).capacity();
    const auto value_capacity = map->getNestedData().getColumn(1).capacity();

    map->insertManyDefaults(64);

    ASSERT_EQ(map->size(), 64);
    ASSERT_EQ(map->getNestedColumn().getData().size(), 0);
    ASSERT_EQ(map->getNestedData().getColumn(0).capacity(), key_capacity);
    ASSERT_EQ(map->getNestedData().getColumn(1).capacity(), value_capacity);
    ASSERT_EQ(map->getNumberOfDefaultRows(), 64);

    for (auto offset : map->getNestedColumn().getOffsets())
        EXPECT_EQ(offset, 0);
}

TEST(ColumnMap, InsertManyDefaultsDoesNotOverallocateNarrowMaps)
{
    constexpr size_t rows = 65521;
    auto map = createEmptyMap<ColumnUInt8>();

    map->insertManyDefaults(rows);

    ASSERT_EQ(map->size(), rows);
    ASSERT_EQ(map->getNestedColumn().getOffsets().capacity(), rows);
    ASSERT_EQ(map->getNestedData().getColumn(0).capacity(), 0);
    ASSERT_EQ(map->getNestedData().getColumn(1).capacity(), 0);
}

TEST(ColumnMap, InsertManyDefaultsPreservesExistingRows)
{
    auto map = createMap();

    map->insertManyDefaults(3);

    ASSERT_EQ(map->size(), 6);
    ASSERT_EQ(map->getNestedColumn().getData().size(), 2);

    const auto & offsets = map->getNestedColumn().getOffsets();
    ASSERT_EQ(offsets.size(), 6);
    EXPECT_EQ(offsets[0], 1);
    EXPECT_EQ(offsets[1], 1);
    EXPECT_EQ(offsets[2], 2);
    EXPECT_EQ(offsets[3], 2);
    EXPECT_EQ(offsets[4], 2);
    EXPECT_EQ(offsets[5], 2);
}

TEST(ColumnMap, InsertManyDefaultsWithZeroLengthDoesNothing)
{
    auto map = createMap();
    const auto allocated_bytes = map->allocatedBytes();

    map->insertManyDefaults(0);

    EXPECT_EQ(map->size(), 3);
    EXPECT_EQ(map->getNestedColumn().getData().size(), 2);
    EXPECT_EQ(map->allocatedBytes(), allocated_bytes);
}
