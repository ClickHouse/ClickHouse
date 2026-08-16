#include <gtest/gtest.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>

#include <limits>
#include <string>
#include <utility>

using namespace DB;

TEST(ColumnNullable, InsertManyFromNotNullableRepeatsValue)
{
    auto src = ColumnUInt64::create();
    src->insert(10);
    src->insert(20);
    src->insert(30);

    auto nested = ColumnUInt64::create();
    nested->insert(7);
    auto null_map = ColumnUInt8::create();
    null_map->insert(1);
    auto dst = ColumnNullable::create(std::move(nested), std::move(null_map));

    dst->insertManyFromNotNullable(*src, 1, 3);

    ASSERT_EQ(dst->size(), 4);
    EXPECT_EQ(dst->getNestedColumn().getUInt(0), 7);
    EXPECT_EQ(dst->getNestedColumn().getUInt(1), 20);
    EXPECT_EQ(dst->getNestedColumn().getUInt(2), 20);
    EXPECT_EQ(dst->getNestedColumn().getUInt(3), 20);
    EXPECT_EQ(dst->getNullMapData()[0], 1);
    EXPECT_EQ(dst->getNullMapData()[1], 0);
    EXPECT_EQ(dst->getNullMapData()[2], 0);
    EXPECT_EQ(dst->getNullMapData()[3], 0);
    dst->checkConsistency();
}

TEST(ColumnNullable, InsertManyFromNotNullableSupportsStringColumns)
{
    auto src = ColumnString::create();
    src->insertData("one", 3);
    src->insertData("two", 3);

    auto nested = ColumnString::create();
    nested->insertData("prefix", 6);
    auto null_map = ColumnUInt8::create();
    null_map->insert(0);
    auto dst = ColumnNullable::create(std::move(nested), std::move(null_map));

    dst->insertManyFromNotNullable(*src, 1, 2);

    const auto & result = assert_cast<const ColumnString &>(dst->getNestedColumn());
    ASSERT_EQ(dst->size(), 3);
    EXPECT_EQ(std::string(result.getDataAt(0)), "prefix");
    EXPECT_EQ(std::string(result.getDataAt(1)), "two");
    EXPECT_EQ(std::string(result.getDataAt(2)), "two");
    EXPECT_EQ(dst->getNullMapData()[1], 0);
    EXPECT_EQ(dst->getNullMapData()[2], 0);
    dst->checkConsistency();
}

TEST(ColumnNullable, InsertManyFromNotNullableWithZeroLengthIsNoOp)
{
    auto src = ColumnUInt64::create();
    auto nested = ColumnUInt64::create();
    auto null_map = ColumnUInt8::create();
    auto dst = ColumnNullable::create(std::move(nested), std::move(null_map));

    dst->insertManyFromNotNullable(*src, std::numeric_limits<size_t>::max(), 0);

    EXPECT_EQ(dst->size(), 0);
    dst->checkConsistency();
}
