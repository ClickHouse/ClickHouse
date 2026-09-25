#include <gtest/gtest.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>

#include <limits>
#include <string>
#include <utility>

using namespace DB;

namespace
{

MutableColumnPtr makeTupleColumn(size_t fixed_string_size, UInt64 number, char character)
{
    auto numbers = ColumnUInt64::create();
    numbers->insertValue(number);

    auto strings = ColumnFixedString::create(fixed_string_size);
    strings->insertData(&character, 1);

    MutableColumns elements;
    elements.push_back(std::move(numbers));
    elements.push_back(std::move(strings));
    return ColumnTuple::create(std::move(elements));
}

void expectTupleColumnUnchanged(const ColumnNullable & column)
{
    ASSERT_EQ(column.size(), 1);
    ASSERT_EQ(column.getNullMapData().size(), 1);
    EXPECT_EQ(column.getNullMapData()[0], 0);

    const auto & tuple = assert_cast<const ColumnTuple &>(column.getNestedColumn());
    ASSERT_EQ(tuple.size(), 1);
    ASSERT_EQ(tuple.getColumn(0).size(), 1);
    ASSERT_EQ(tuple.getColumn(1).size(), 1);
    EXPECT_EQ(tuple.getColumn(0).getUInt(0), 7);

    const auto & string = assert_cast<const ColumnFixedString &>(tuple.getColumn(1));
    EXPECT_EQ(string.getDataAt(0)[0], 'd');
    column.checkConsistency();
}

}

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
    dst->insertManyFromNotNullable(*src, 2, 1);

    ASSERT_EQ(dst->size(), 5);
    EXPECT_EQ(dst->getNestedColumn().getUInt(0), 7);
    EXPECT_EQ(dst->getNestedColumn().getUInt(1), 20);
    EXPECT_EQ(dst->getNestedColumn().getUInt(2), 20);
    EXPECT_EQ(dst->getNestedColumn().getUInt(3), 20);
    EXPECT_EQ(dst->getNestedColumn().getUInt(4), 30);
    EXPECT_EQ(dst->getNullMapData()[0], 1);
    EXPECT_EQ(dst->getNullMapData()[1], 0);
    EXPECT_EQ(dst->getNullMapData()[2], 0);
    EXPECT_EQ(dst->getNullMapData()[3], 0);
    EXPECT_EQ(dst->getNullMapData()[4], 0);
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
    src->insert(10);
    auto nested = ColumnUInt64::create();
    nested->insert(7);
    auto null_map = ColumnUInt8::create();
    null_map->insert(1);
    auto dst = ColumnNullable::create(std::move(nested), std::move(null_map));

    dst->insertManyFromNotNullable(*src, std::numeric_limits<size_t>::max(), 0);

    ASSERT_EQ(dst->size(), 1);
    EXPECT_EQ(dst->getNestedColumn().getUInt(0), 7);
    EXPECT_EQ(dst->getNullMapData()[0], 1);
    dst->checkConsistency();
}

TEST(ColumnNullable, InsertManyFromNullableSourceRepeatsRawNullMarker)
{
    auto src_nested = ColumnUInt64::create();
    src_nested->insert(10);
    src_nested->insert(20);

    auto src_null_map = ColumnUInt8::create();
    src_null_map->insert(0);
    /// Non-zero bytes all represent NULL. Keep a non-canonical marker to verify exact repetition.
    src_null_map->insert(2);

    auto src = ColumnNullable::create(std::move(src_nested), std::move(src_null_map));
    auto dst = ColumnNullable::create(ColumnUInt64::create(), ColumnUInt8::create());

    dst->insertManyFrom(*src, 0, 1);
    dst->insertManyFrom(*src, 0, 3);
    dst->insertManyFrom(*src, 1, 2);
    dst->insertManyFrom(*src, std::numeric_limits<size_t>::max(), 0);

    ASSERT_EQ(dst->size(), 6);
    EXPECT_EQ(dst->getNestedColumn().getUInt(0), 10);
    EXPECT_EQ(dst->getNestedColumn().getUInt(1), 10);
    EXPECT_EQ(dst->getNestedColumn().getUInt(2), 10);
    EXPECT_EQ(dst->getNestedColumn().getUInt(3), 10);
    EXPECT_EQ(dst->getNestedColumn().getUInt(4), 20);
    EXPECT_EQ(dst->getNestedColumn().getUInt(5), 20);
    EXPECT_EQ(dst->getNullMapData()[0], 0);
    EXPECT_EQ(dst->getNullMapData()[1], 0);
    EXPECT_EQ(dst->getNullMapData()[2], 0);
    EXPECT_EQ(dst->getNullMapData()[3], 0);
    EXPECT_EQ(dst->getNullMapData()[4], 2);
    EXPECT_EQ(dst->getNullMapData()[5], 2);
    dst->checkConsistency();
}

TEST(ColumnNullable, InsertManyFromNullableSourceRollsBackCompositeFailure)
{
    auto src_nested = makeTupleColumn(8, 42, 's');
    auto src_null_map = ColumnUInt8::create();
    src_null_map->insertValue(0);
    auto src = ColumnNullable::create(std::move(src_nested), std::move(src_null_map));

    auto dst_nested = makeTupleColumn(4, 7, 'd');
    auto dst_null_map = ColumnUInt8::create();
    dst_null_map->insertValue(0);
    auto dst = ColumnNullable::create(std::move(dst_nested), std::move(dst_null_map));

    EXPECT_THROW(dst->insertManyFrom(*src, 0, 2), Exception);
    expectTupleColumnUnchanged(*dst);
}

TEST(ColumnNullable, InsertManyFromNotNullableRollsBackCompositeFailure)
{
    auto src = makeTupleColumn(8, 42, 's');

    auto dst_nested = makeTupleColumn(4, 7, 'd');
    auto dst_null_map = ColumnUInt8::create();
    dst_null_map->insertValue(0);
    auto dst = ColumnNullable::create(std::move(dst_nested), std::move(dst_null_map));

    EXPECT_THROW(dst->insertManyFromNotNullable(*src, 0, 2), Exception);
    expectTupleColumnUnchanged(*dst);
}
