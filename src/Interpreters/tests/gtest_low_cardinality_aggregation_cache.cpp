#include <gtest/gtest.h>

#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/AggregatedData.h>
#include <Interpreters/AggregationMethod.h>
#include <Interpreters/Aggregator.h>

#include <initializer_list>
#include <utility>

using namespace DB;

namespace
{

class CountingData : public HashMap<UInt64, UInt64>
{
public:
    using Base = HashMap<UInt64, UInt64>;
    using LookupResult = typename Base::LookupResult;

    template <typename KeyHolder>
    void emplace(KeyHolder && key_holder, LookupResult & result, bool & inserted)
    {
        ++emplace_calls;
        Base::emplace(std::forward<KeyHolder>(key_holder), result, inserted);
    }

    template <typename KeyHolder>
    void emplace(KeyHolder && key_holder, LookupResult & result, bool & inserted, size_t hash)
    {
        ++emplace_calls;
        Base::emplace(std::forward<KeyHolder>(key_holder), result, inserted, hash);
    }

    LookupResult find(const UInt64 & key)
    {
        ++find_calls;
        return Base::find(key);
    }

    LookupResult find(const UInt64 & key, size_t hash)
    {
        ++find_calls;
        return Base::find(key, hash);
    }

    bool & hasNullKeyData() { return has_null_key_data; }
    UInt64 & getNullKeyData() { return null_key_data; }

    size_t emplace_calls = 0;
    size_t find_calls = 0;

private:
    bool has_null_key_data = false;
    UInt64 null_key_data = 0;
};

using BaseMethod = AggregationMethodOneNumber<UInt64, CountingData>;
using Method = AggregationMethodSingleLowCardinalityColumn<BaseMethod>;
using InlineCountBaseMethod = AggregationMethodOneNumber<UInt64, AggregatedDataWithNullableUInt64Key>;
using InlineCountMethod = AggregationMethodSingleLowCardinalityColumn<InlineCountBaseMethod>;
using UInt8BaseMethod = AggregationMethodOneNumber<UInt8, AggregatedDataWithNullableUInt8Key, false>;
using UInt8Method = AggregationMethodSingleLowCardinalityColumn<UInt8BaseMethod>;

ColumnPtr makeDictionary(std::initializer_list<UInt64> values)
{
    auto keys = ColumnUInt64::create();
    for (UInt64 value : values)
        keys->insertValue(value);

    return DataTypeLowCardinality::createColumnUnique(DataTypeUInt64(), std::move(keys));
}

ColumnPtr makeDictionary(size_t size)
{
    auto keys = ColumnUInt64::create();
    for (size_t value = 0; value < size; ++value)
        keys->insertValue(value);

    return DataTypeLowCardinality::createColumnUnique(DataTypeUInt64(), std::move(keys));
}

ColumnPtr makeUInt8Dictionary(std::initializer_list<UInt8> values)
{
    auto keys = ColumnUInt8::create();
    for (UInt8 value : values)
        keys->insertValue(value);

    return DataTypeLowCardinality::createColumnUnique(DataTypeUInt8(), std::move(keys));
}

ColumnPtr makeLowCardinalityColumn(const ColumnPtr & dictionary, std::initializer_list<UInt8> indexes, bool is_shared)
{
    auto index_column = ColumnUInt8::create();
    for (UInt8 index : indexes)
        index_column->insertValue(index);

    return ColumnLowCardinality::create(dictionary, std::move(index_column), is_shared);
}

template <typename AggregationMethod = Method>
ColumnsHashing::HashMethodContextPtr makeContext()
{
    ColumnsHashing::HashMethodContextSettings settings;
    settings.max_threads = 1;
    return AggregationMethod::StateNoCache::createContext(settings);
}

void aggregateBlock(
    Method & method,
    CountingData & data,
    const ColumnPtr & column,
    const ColumnsHashing::HashMethodContextPtr & context,
    Method::LowCardinalityCache * low_cardinality_cache_override = nullptr)
{
    ColumnRawPtrs key_columns{column.get()};
    Sizes key_sizes{sizeof(UInt64)};
    auto state = createAggregationMethodState<typename Method::StateNoCache>(
        method, key_columns, key_sizes, context, low_cardinality_cache_override);
    Arena pool;
    state.resetCache();

    for (size_t row = 0; row < column->size(); ++row)
    {
        auto result = state.emplaceKey(data, row, pool);
        const UInt64 expected_mapped = column->getUInt(row) + 1000;
        if (result.isInserted())
            result.setMapped(expected_mapped);
        EXPECT_EQ(result.getMapped(), expected_mapped);
    }
}

void aggregateBlock(Method & method, const ColumnPtr & column, const ColumnsHashing::HashMethodContextPtr & context)
{
    aggregateBlock(method, method.data, column, context);
}

TEST(LowCardinalityAggregationCache, ReusesMappingsForSharedDictionaryAcrossBlocks)
{
    Method method;
    auto context = makeContext();
    auto dictionary = makeDictionary({0, 11, 22});

    aggregateBlock(method, makeLowCardinalityColumn(dictionary, {1, 2, 1}, true), context);
    ASSERT_EQ(method.data.emplace_calls, 2);

    aggregateBlock(method, makeLowCardinalityColumn(dictionary, {2, 1, 2}, true), context);
    EXPECT_EQ(method.data.emplace_calls, 2);
}

TEST(LowCardinalityAggregationCache, InvalidatesMappingsAfterHashTableChange)
{
    Method method;
    auto context = makeContext();
    auto dictionary = makeDictionary({0, 11});
    auto column = makeLowCardinalityColumn(dictionary, {1, 1, 1}, true);
    ColumnRawPtrs key_columns{column.get()};
    Sizes key_sizes{sizeof(UInt64)};
    auto state = createAggregationMethodState<typename Method::StateNoCache>(
        method, key_columns, key_sizes, context);
    Arena pool;

    auto first = state.emplaceKey(method.data, 0, pool);
    ASSERT_TRUE(first.isInserted());
    first.setMapped(1011);
    ASSERT_EQ(method.data.emplace_calls, 1);

    state.resetCache();
    auto second = state.emplaceKey(method.data, 1, pool);
    EXPECT_FALSE(second.isInserted());
    EXPECT_EQ(second.getMapped(), 1011);
    EXPECT_EQ(method.data.emplace_calls, 1);

    state.resetCacheAfterHashTableChange();
    auto third = state.emplaceKey(method.data, 2, pool);
    EXPECT_FALSE(third.isInserted());
    EXPECT_EQ(third.getMapped(), 1011);
    EXPECT_EQ(method.data.emplace_calls, 2);
}

TEST(LowCardinalityAggregationCache, ReplacesMappingsForDifferentSharedDictionary)
{
    Method method;
    auto context = makeContext();

    aggregateBlock(method, makeLowCardinalityColumn(makeDictionary({0, 11}), {1}, true), context);
    ASSERT_EQ(method.data.emplace_calls, 1);

    aggregateBlock(method, makeLowCardinalityColumn(makeDictionary({0, 22}), {1}, true), context);
    EXPECT_EQ(method.data.emplace_calls, 2);
}

TEST(LowCardinalityAggregationCache, ReleasesBuffersForDifferentSharedDictionary)
{
    Method method;
    auto context = makeContext();

    aggregateBlock(method, makeLowCardinalityColumn(makeDictionary(200), {199}, true), context);
    const size_t large_dictionary_bytes = method.low_cardinality_cache.allocatedBytes();

    aggregateBlock(method, makeLowCardinalityColumn(makeDictionary(2), {1}, true), context);
    EXPECT_LT(method.low_cardinality_cache.allocatedBytes(), large_dictionary_bytes);
}

TEST(LowCardinalityAggregationCache, DoesNotRetainMappingsForNonSharedDictionary)
{
    Method method;
    auto context = makeContext();
    auto dictionary = makeDictionary({0, 11});

    aggregateBlock(method, makeLowCardinalityColumn(dictionary, {1, 1}, false), context);
    ASSERT_EQ(method.data.emplace_calls, 1);
    EXPECT_FALSE(method.low_cardinality_cache.dictionary_key.has_value());
    EXPECT_TRUE(method.low_cardinality_cache.visit_cache.empty());

    aggregateBlock(method, makeLowCardinalityColumn(dictionary, {1}, false), context);
    EXPECT_EQ(method.data.emplace_calls, 2);
}

TEST(LowCardinalityAggregationCache, KeepsMappingsIsolatedBetweenCaches)
{
    Method method;
    CountingData first_data;
    CountingData second_data;
    Method::LowCardinalityCache first_cache;
    Method::LowCardinalityCache second_cache;
    auto context = makeContext();
    auto dictionary = makeDictionary({0, 11});
    auto column = makeLowCardinalityColumn(dictionary, {1}, true);

    aggregateBlock(method, first_data, column, context, &first_cache);
    aggregateBlock(method, second_data, column, context, &second_cache);
    ASSERT_EQ(first_data.emplace_calls, 1);
    ASSERT_EQ(second_data.emplace_calls, 1);

    aggregateBlock(method, first_data, column, context, &first_cache);
    aggregateBlock(method, second_data, column, context, &second_cache);
    EXPECT_EQ(first_data.emplace_calls, 1);
    EXPECT_EQ(second_data.emplace_calls, 1);
}

TEST(LowCardinalityAggregationCache, CachesMissingMappingsOnlyWithinBlock)
{
    Method method;
    auto context = makeContext();
    auto dictionary = makeDictionary({0, 11});
    auto column = makeLowCardinalityColumn(dictionary, {1, 1, 1}, true);
    ColumnRawPtrs key_columns{column.get()};
    Sizes key_sizes{sizeof(UInt64)};
    Arena pool;

    {
        auto state = createAggregationMethodState<typename Method::StateNoCache>(
            method, key_columns, key_sizes, context);
        for (size_t row = 0; row < column->size(); ++row)
            EXPECT_FALSE(state.findKey(method.data, row, pool).isFound());
    }
    ASSERT_EQ(method.data.find_calls, 1);

    method.data[11] = 1011;

    {
        auto state = createAggregationMethodState<typename Method::StateNoCache>(
            method, key_columns, key_sizes, context);
        auto result = state.findKey(method.data, 0, pool);
        ASSERT_TRUE(result.isFound());
        EXPECT_EQ(result.getMapped(), 1011);
    }
    EXPECT_EQ(method.data.find_calls, 2);
}

TEST(LowCardinalityAggregationCache, DoesNotCacheInlineCountMappedValuesAcrossBlocks)
{
    InlineCountMethod method;
    auto context = makeContext();
    auto dictionary = makeDictionary({0, 11, 22});

    const auto aggregate_block = [&](const ColumnPtr & column)
    {
        ColumnRawPtrs key_columns{column.get()};
        Sizes key_sizes{sizeof(UInt64)};
        auto state = createAggregationMethodState<typename InlineCountMethod::StateNoCacheWithoutMappedCache>(
            method, key_columns, key_sizes, context);
        Arena pool;

        for (size_t row = 0; row < column->size(); ++row)
        {
            auto result = state.emplaceKey(method.data, row, pool);
            if (result.isInserted())
                getInlineCountState(result.getMapped()) = 1;
            else
                ++getInlineCountState(result.getMapped());
        }
    };

    aggregate_block(makeLowCardinalityColumn(dictionary, {1, 2}, true));
    aggregate_block(makeLowCardinalityColumn(dictionary, {1, 2}, true));

    ASSERT_TRUE(method.data.find(11));
    EXPECT_EQ(getInlineCountState(method.data.find(11)->getMapped()), 2);
    ASSERT_TRUE(method.data.find(22));
    EXPECT_EQ(getInlineCountState(method.data.find(22)->getMapped()), 2);
}

TEST(LowCardinalityAggregationCache, UsesBlockLocalMappingsForUInt8Keys)
{
    UInt8Method method;
    UInt8Method::LowCardinalityCache cache_override;
    auto context = makeContext<UInt8Method>();
    auto dictionary = makeUInt8Dictionary({0, 11});
    auto column = makeLowCardinalityColumn(dictionary, {1, 1}, true);
    ColumnRawPtrs key_columns{column.get()};
    Sizes key_sizes{sizeof(UInt8)};
    Arena pool;
    char aggregate_state = 0;

    {
        auto state = createAggregationMethodState<typename UInt8Method::StateNoCache>(
            method, key_columns, key_sizes, context, &cache_override);
        {
            auto result = state.emplaceKey(method.data, 0, pool);
            ASSERT_TRUE(result.isInserted());
            result.setMapped(&aggregate_state);
        }
        {
            auto result = state.emplaceKey(method.data, 1, pool);
            EXPECT_FALSE(result.isInserted());
            EXPECT_EQ(result.getMapped(), &aggregate_state);
        }
    }

    EXPECT_FALSE(method.low_cardinality_cache.dictionary_key.has_value());
    EXPECT_TRUE(method.low_cardinality_cache.visit_cache.empty());
    EXPECT_FALSE(cache_override.dictionary_key.has_value());
    EXPECT_TRUE(cache_override.visit_cache.empty());
}

}
