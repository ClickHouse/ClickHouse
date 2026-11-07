#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <vector>

#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Common/assert_cast.h>

using namespace DB;

namespace
{

using Backpressure = ManyAggregatedData::DictionaryAggregationBackpressure;

ColumnPtr makePrivateArgument()
{
    auto column = DataTypeLowCardinality(std::make_shared<DataTypeString>()).createColumn();
    for (size_t row = 0; row < 16; ++row)
        column->insert(String(1024, static_cast<char>('a' + row % 4)));
    return column;
}

std::vector<Columns> makeShards(const IColumn & column, size_t num_shards)
{
    std::vector<Columns> shards(num_shards);
    for (size_t shard = 0; shard < num_shards; ++shard)
    {
        auto indexes = ColumnUInt64::create();
        for (size_t row = shard; row < column.size(); row += num_shards)
            indexes->insertValue(row);
        shards[shard].push_back(column.index(*indexes, 0));
    }
    return shards;
}

size_t payloadBytes(const Columns & columns)
{
    return Backpressure::getBlockBytes(columns)
        - sizeof(ManyAggregatedData::DictionaryAggregationBlock) - columns.capacity() * sizeof(ColumnPtr);
}

}

TEST(DictionaryAggregationBackpressure, DictionaryBytesDoNotMultiplyWithShards)
{
    auto private_argument = makePrivateArgument();
    const auto & source = assert_cast<const ColumnLowCardinality &>(*private_argument);
    const auto dictionary = source.getDictionaryPtr();

    for (bool shared : {false, true})
    {
        for (bool nested : {false, true})
        {
            for (size_t num_shards : {1, 2, 4, 8})
            {
                SCOPED_TRACE(::testing::Message() << "shared=" << shared << ", nested=" << nested << ", shards=" << num_shards);
                ColumnPtr argument = ColumnLowCardinality::create(dictionary, source.getIndexes().getPtr(), shared);
                if (nested)
                    argument = ColumnTuple::create(Columns{argument, argument});
                auto shards = makeShards(*argument, num_shards);

                size_t fragment_bytes = 0;
                for (const auto & columns : shards)
                {
                    const IColumn & column = nested
                        ? assert_cast<const ColumnTuple &>(*columns[0]).getColumn(0) : *columns[0];
                    const auto & low_cardinality = assert_cast<const ColumnLowCardinality &>(column);
                    ASSERT_EQ(low_cardinality.getDictionaryPtr().get(), dictionary.get());
                    ASSERT_EQ(low_cardinality.isSharedDictionary(), shared);
                    fragment_bytes += payloadBytes(columns);
                }
                EXPECT_EQ(fragment_bytes, source.getIndexes().byteSize() * (nested ? 2 : 1));

                auto backpressure = std::make_shared<Backpressure>();
                auto retained = backpressure->retainDictionaries(shards, nullptr);
                ASSERT_TRUE(retained);
                ASSERT_EQ(retained->dictionaries.size(), 1u);
                EXPECT_EQ(retained->dictionaries[0].get(), dictionary.get());
            }
        }
    }
}

TEST(DictionaryAggregationBackpressure, RetainsRebuiltNestedDictionaries)
{
    auto values = DataTypeLowCardinality(std::make_shared<DataTypeString>()).createColumn();
    values->insertDefault();
    auto offsets = ColumnUInt64::create();
    for (size_t row = 0; row < 4; ++row)
    {
        values->insert(String(1024, static_cast<char>('a' + row)));
        offsets->insertValue(row);
    }
    const auto source_dictionary = assert_cast<const ColumnLowCardinality &>(*values).getDictionaryPtr();
    auto sparse = ColumnSparse::create(std::move(values), std::move(offsets), 4);
    auto shards = makeShards(*sparse, 2);
    auto backpressure = std::make_shared<Backpressure>();
    auto retained = backpressure->retainDictionaries(shards, nullptr);
    ASSERT_TRUE(retained);
    ASSERT_EQ(retained->dictionaries.size(), 2u);

    for (const auto & columns : shards)
    {
        const auto & fragment = assert_cast<const ColumnSparse &>(*columns[0]);
        const auto & low_cardinality = assert_cast<const ColumnLowCardinality &>(fragment.getValuesColumn());
        const auto dictionary = low_cardinality.getDictionaryPtr();
        ASSERT_NE(dictionary.get(), source_dictionary.get());
        EXPECT_FALSE(low_cardinality.isSharedDictionary());
        EXPECT_NE(std::ranges::find(retained->dictionaries, dictionary), retained->dictionaries.end());
        EXPECT_EQ(
            payloadBytes(columns), low_cardinality.getIndexes().byteSize() + fragment.getOffsetsColumn().byteSize() + sizeof(size_t));
    }
}

TEST(DictionaryAggregationBackpressure, ExcludesOnlyTheGroupingDictionary)
{
    auto grouping_column = makePrivateArgument();
    auto argument = makePrivateArgument();
    const auto grouping_dictionary = assert_cast<const ColumnLowCardinality &>(*grouping_column).getDictionaryPtr();
    const auto argument_dictionary = assert_cast<const ColumnLowCardinality &>(*argument).getDictionaryPtr();
    Columns columns{grouping_column, argument, argument};
    auto backpressure = std::make_shared<Backpressure>();

    /// Equal values do not make different dictionaries the same allocation.
    auto all = backpressure->retainDictionaries({&columns, 1}, nullptr);
    ASSERT_TRUE(all);
    EXPECT_EQ(all->dictionaries.size(), 2u);

    auto arguments_only = backpressure->retainDictionaries({&columns, 1}, grouping_dictionary.get());
    ASSERT_TRUE(arguments_only);
    ASSERT_EQ(arguments_only->dictionaries.size(), 1u);
    EXPECT_EQ(arguments_only->dictionaries[0].get(), argument_dictionary.get());
}
