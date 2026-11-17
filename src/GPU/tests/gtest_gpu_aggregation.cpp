#include "config.h"

#if USE_GPU

#include <GPU/GPUAccumulator.h>
#include <GPU/GPUDevice.h>
#include <GPU/GPUTypeMapping.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ServerSettings.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_context.h>

#include <cuda_runtime_api.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <map>
#include <mutex>
#include <random>
#include <string>
#include <vector>

using namespace DB;
using namespace DB::GPU;

namespace
{

bool hasDevice()
{
    int count = 0;
    const bool has = cudaGetDeviceCount(&count) == cudaSuccess && count > 0;
    cudaGetLastError();
    return has;
}

class GPUAggregation : public ::testing::Test
{
protected:
    void SetUp() override
    {
        if (!hasDevice())
            GTEST_SKIP() << "No CUDA device";

        static std::once_flag once;
        std::call_once(once, [] { getMutableContext().context->setGPU(ServerSettings{}); });
    }
};

/// Small batches, so that a test sends many and every slot of the pipeline is used again and again.
constexpr size_t small_batch_bytes = 64 * 1024;

template <typename T>
ColumnPtr columnOf(const std::vector<T> & values)
{
    auto column = ColumnVector<T>::create();
    column->getData().assign(values.begin(), values.end());
    return column;
}

ColumnPtr stringColumnOf(const std::vector<std::string> & values)
{
    auto column = ColumnString::create();
    for (const auto & value : values)
        column->insertData(value.data(), value.size());
    return column;
}

/// Adds `rows` in blocks of `block_rows`, as a pipeline of `MergeTree` reads hands them over.
template <typename Add>
void inBlocks(size_t rows, size_t block_rows, Add && add)
{
    for (size_t begin = 0; begin < rows; begin += block_rows)
        add(begin, std::min(rows, begin + block_rows));
}

template <typename T>
std::vector<T> slice(const std::vector<T> & values, size_t begin, size_t end)
{
    return std::vector<T>(values.begin() + begin, values.begin() + end);
}

DataTypePtr resultTypeOf(GPUAggregationKind aggregation, const DataTypePtr & argument_type)
{
    switch (aggregation)
    {
        case GPUAggregationKind::Sum:
        {
            const auto element_type = elementTypeOf(*argument_type);
            switch (sumResultTypeFor(*element_type))
            {
                case GPUElementType::UInt64: return std::make_shared<DataTypeUInt64>();
                case GPUElementType::Int64: return std::make_shared<DataTypeInt64>();
                default: return std::make_shared<DataTypeFloat64>();
            }
        }
        case GPUAggregationKind::Min:
        case GPUAggregationKind::Max:
            return argument_type;
        case GPUAggregationKind::Count:
            return std::make_shared<DataTypeUInt64>();
        case GPUAggregationKind::Mean:
            return std::make_shared<DataTypeFloat64>();
    }
    return nullptr;
}

/// The groups of a `GroupByGPUAccumulator`, by the row of each, sorted by `key_of`.
struct Groups
{
    MutableColumns keys;
    MutableColumns aggregates;
    size_t rows = 0;
};

Groups groupsOf(
    GroupByGPUAccumulator & accumulator, const DataTypes & key_types, const std::vector<DataTypePtr> & aggregate_types)
{
    Groups groups;
    groups.rows = accumulator.finalize();
    for (const auto & type : key_types)
        groups.keys.push_back(type->createColumn());
    for (const auto & type : aggregate_types)
        groups.aggregates.push_back(type->createColumn());
    accumulator.copyGroupsTo(groups.keys, groups.aggregates);
    return groups;
}

}

TEST_F(GPUAggregation, ReduceWithoutKeys)
{
    const size_t rows = 1'000'003;
    std::mt19937_64 random(42);

    std::vector<UInt32> u32(rows);
    std::vector<Int64> i64(rows);
    std::vector<Float64> f64(rows);
    for (size_t i = 0; i < rows; ++i)
    {
        u32[i] = static_cast<UInt32>(random());
        i64[i] = static_cast<Int64>(random() % 2'000'001) - 1'000'000;
        f64[i] = static_cast<Float64>(random() % 1'000'000) / 1000.0;
    }

    const DataTypePtr u32_type = std::make_shared<DataTypeUInt32>();
    const DataTypePtr i64_type = std::make_shared<DataTypeInt64>();
    const DataTypePtr f64_type = std::make_shared<DataTypeFloat64>();

    const std::vector<GPUAggregationKind> aggregations
        = {GPUAggregationKind::Sum, GPUAggregationKind::Min, GPUAggregationKind::Max, GPUAggregationKind::Sum, GPUAggregationKind::Max};
    const DataTypes argument_types = {u32_type, u32_type, i64_type, i64_type, f64_type};
    DataTypes result_types;
    for (size_t i = 0; i < aggregations.size(); ++i)
        result_types.push_back(resultTypeOf(aggregations[i], argument_types[i]));

    GPUAccumulator accumulator(argument_types, result_types, aggregations, small_batch_bytes);
    inBlocks(rows, 65'409, [&](size_t begin, size_t end)
    {
        const ColumnPtr u = columnOf(slice(u32, begin, end));
        const ColumnPtr i = columnOf(slice(i64, begin, end));
        const ColumnPtr f = columnOf(slice(f64, begin, end));
        accumulator.add({u, u, i, i, f});
    });

    const std::vector<Field> results = accumulator.finalize();
    ASSERT_EQ(results.size(), aggregations.size());

    UInt64 sum_u32 = 0;
    Int64 sum_i64 = 0;
    for (size_t i = 0; i < rows; ++i)
    {
        sum_u32 += u32[i];
        sum_i64 += i64[i];
    }

    EXPECT_EQ(results[0].safeGet<UInt64>(), sum_u32);
    EXPECT_EQ(results[1].safeGet<UInt64>(), *std::min_element(u32.begin(), u32.end()));
    EXPECT_EQ(results[2].safeGet<Int64>(), *std::max_element(i64.begin(), i64.end()));
    EXPECT_EQ(results[3].safeGet<Int64>(), sum_i64);
    EXPECT_EQ(results[4].safeGet<Float64>(), *std::max_element(f64.begin(), f64.end()));
}

TEST_F(GPUAggregation, ReduceWithoutRows)
{
    const DataTypePtr type = std::make_shared<DataTypeInt32>();
    const DataTypePtr sum_type = std::make_shared<DataTypeInt64>();

    GPUAccumulator accumulator({type, type}, {sum_type, type}, {GPUAggregationKind::Sum, GPUAggregationKind::Min}, small_batch_bytes);
    const std::vector<Field> results = accumulator.finalize();

    ASSERT_EQ(results.size(), 2);
    EXPECT_EQ(results[0].safeGet<Int64>(), 0);
    EXPECT_EQ(results[1].safeGet<Int64>(), 0);
}

TEST_F(GPUAggregation, ReduceMinMaxOfStrings)
{
    const size_t rows = 300'000;
    std::mt19937_64 random(7);
    std::vector<std::string> values(rows);
    for (auto & value : values)
        value = "s" + std::to_string(random() % 1'000'000'007);

    const DataTypePtr type = std::make_shared<DataTypeString>();
    GPUAccumulator accumulator({type, type}, {type, type}, {GPUAggregationKind::Min, GPUAggregationKind::Max}, small_batch_bytes);
    inBlocks(rows, 10'000, [&](size_t begin, size_t end)
    {
        const ColumnPtr column = stringColumnOf(slice(values, begin, end));
        accumulator.add({column, column});
    });

    const std::vector<Field> results = accumulator.finalize();
    ASSERT_EQ(results.size(), 2);
    EXPECT_EQ(results[0].safeGet<String>(), *std::min_element(values.begin(), values.end()));
    EXPECT_EQ(results[1].safeGet<String>(), *std::max_element(values.begin(), values.end()));
}

namespace
{

struct Expected
{
    Int64 sum = 0;
    Int32 min = std::numeric_limits<Int32>::max();
    Int32 max = std::numeric_limits<Int32>::min();
    UInt64 count = 0;
};

/// Groups `values` by `keys` on the device by `sum`, `min`, `max`, `count` and, with `with_avg`, `avg`, and checks every
/// group. Without `avg`, the partial results of runs of keys combine, which the device reduces sorted keys by.
void checkGroupBy(const std::vector<UInt64> & keys, const std::vector<Int32> & values, size_t batch_bytes, bool with_avg = true)
{
    std::map<UInt64, Expected> expected;
    for (size_t i = 0; i < keys.size(); ++i)
    {
        Expected & group = expected[keys[i]];
        group.sum += values[i];
        group.min = std::min(group.min, values[i]);
        group.max = std::max(group.max, values[i]);
        ++group.count;
    }

    const DataTypePtr key_type = std::make_shared<DataTypeUInt64>();
    const DataTypePtr value_type = std::make_shared<DataTypeInt32>();
    std::vector<DataTypePtr> aggregate_types = {std::make_shared<DataTypeInt64>(), value_type, value_type, std::make_shared<DataTypeUInt64>()};

    std::vector<GPUGroupByAggregate> aggregates = {
        {.aggregation = GPUAggregationKind::Sum, .column = 0, .result_type = GPUElementType::Int64},
        {.aggregation = GPUAggregationKind::Min, .column = 0, .result_type = GPUElementType::Int32},
        {.aggregation = GPUAggregationKind::Max, .column = 0, .result_type = GPUElementType::Int32},
        {.aggregation = GPUAggregationKind::Count, .column = GPUGroupByAggregate::no_column, .result_type = GPUElementType::UInt64},
    };
    if (with_avg)
    {
        aggregates.push_back({.aggregation = GPUAggregationKind::Mean, .column = 0, .result_type = GPUElementType::Float64});
        aggregate_types.push_back(std::make_shared<DataTypeFloat64>());
    }

    GroupByGPUAccumulator accumulator({key_type}, {value_type}, std::move(aggregates), batch_bytes);
    inBlocks(keys.size(), 65'536, [&](size_t begin, size_t end)
    {
        accumulator.add({columnOf(slice(keys, begin, end))}, {columnOf(slice(values, begin, end))});
    });

    const Groups groups = groupsOf(accumulator, {key_type}, aggregate_types);
    ASSERT_EQ(groups.rows, expected.size());

    const auto & got_keys = assert_cast<const ColumnUInt64 &>(*groups.keys[0]).getData();
    const auto & sums = assert_cast<const ColumnInt64 &>(*groups.aggregates[0]).getData();
    const auto & mins = assert_cast<const ColumnInt32 &>(*groups.aggregates[1]).getData();
    const auto & maxs = assert_cast<const ColumnInt32 &>(*groups.aggregates[2]).getData();
    const auto & counts = assert_cast<const ColumnUInt64 &>(*groups.aggregates[3]).getData();

    for (size_t row = 0; row < groups.rows; ++row)
    {
        const auto it = expected.find(got_keys[row]);
        ASSERT_NE(it, expected.end()) << "an unexpected key " << got_keys[row];
        const Expected & group = it->second;
        EXPECT_EQ(sums[row], group.sum) << "key " << got_keys[row];
        EXPECT_EQ(mins[row], group.min) << "key " << got_keys[row];
        EXPECT_EQ(maxs[row], group.max) << "key " << got_keys[row];
        EXPECT_EQ(counts[row], group.count) << "key " << got_keys[row];
        if (with_avg)
        {
            const Float64 mean = assert_cast<const ColumnFloat64 &>(*groups.aggregates[4]).getData()[row];
            EXPECT_NEAR(mean, static_cast<double>(group.sum) / static_cast<double>(group.count), 1e-9 * std::max<double>(1, std::abs(mean)))
                << "key " << got_keys[row];
        }
        expected.erase(it);
    }
}

}

TEST_F(GPUAggregation, GroupByFewKeys)
{
    const size_t rows = 1'000'000;
    std::mt19937_64 random(1);
    std::vector<UInt64> keys(rows);
    std::vector<Int32> values(rows);
    for (size_t i = 0; i < rows; ++i)
    {
        keys[i] = random() % 100;
        values[i] = static_cast<Int32>(random() % 2'000'001) - 1'000'000;
    }
    checkGroupBy(keys, values, small_batch_bytes);
}

/// More groups than the first hash table of `streaming_groupby` takes, so that it grows, by the groups the device reports.
TEST_F(GPUAggregation, GroupByManyKeys)
{
    const size_t rows = 2'000'000;
    std::mt19937_64 random(2);
    std::vector<UInt64> keys(rows);
    std::vector<Int32> values(rows);
    for (size_t i = 0; i < rows; ++i)
    {
        keys[i] = random() % 500'000;
        values[i] = static_cast<Int32>(random() % 1000);
    }
    checkGroupBy(keys, values, 4 * 1024 * 1024);
}

/// Keys in runs, as in a part sorted by them, which the device reduces over their runs before it combines them.
TEST_F(GPUAggregation, GroupBySortedKeys)
{
    const size_t rows = 1'500'000;
    std::mt19937_64 random(3);
    std::vector<UInt64> keys(rows);
    std::vector<Int32> values(rows);
    for (size_t i = 0; i < rows; ++i)
    {
        keys[i] = i / 37;
        values[i] = static_cast<Int32>(random() % 100'000);
    }
    checkGroupBy(keys, values, small_batch_bytes, /*with_avg=*/ false);
    checkGroupBy(keys, values, small_batch_bytes);
}

TEST_F(GPUAggregation, GroupByWithoutRows)
{
    const DataTypePtr key_type = std::make_shared<DataTypeUInt64>();
    const DataTypePtr value_type = std::make_shared<DataTypeInt32>();
    GroupByGPUAccumulator accumulator(
        {key_type}, {value_type}, {{.aggregation = GPUAggregationKind::Sum, .column = 0, .result_type = GPUElementType::Int64}}, small_batch_bytes);

    const Groups groups = groupsOf(accumulator, {key_type}, {std::make_shared<DataTypeInt64>()});
    EXPECT_EQ(groups.rows, 0);
    EXPECT_EQ(groups.keys[0]->size(), 0);
    EXPECT_EQ(groups.aggregates[0]->size(), 0);
}

TEST_F(GPUAggregation, GroupByStrings)
{
    const size_t rows = 400'000;
    std::mt19937_64 random(4);
    std::vector<std::string> keys(rows);
    std::vector<std::string> values(rows);
    std::vector<UInt16> numbers(rows);
    for (size_t i = 0; i < rows; ++i)
    {
        keys[i] = "key_" + std::to_string(random() % 1000);
        values[i] = "v" + std::to_string(random() % 1'000'000);
        numbers[i] = static_cast<UInt16>(random());
    }

    std::map<std::string, std::tuple<std::string, std::string, UInt64>> expected;
    for (size_t i = 0; i < rows; ++i)
    {
        auto [it, inserted] = expected.try_emplace(keys[i], values[i], values[i], 0);
        auto & [min, max, sum] = it->second;
        min = std::min(min, values[i]);
        max = std::max(max, values[i]);
        sum += numbers[i];
    }

    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    const DataTypePtr number_type = std::make_shared<DataTypeUInt16>();
    std::vector<GPUGroupByAggregate> aggregates = {
        {.aggregation = GPUAggregationKind::Min, .column = 0, .result_type = GPUElementType::String},
        {.aggregation = GPUAggregationKind::Max, .column = 0, .result_type = GPUElementType::String},
        {.aggregation = GPUAggregationKind::Sum, .column = 1, .result_type = GPUElementType::UInt64},
    };

    GroupByGPUAccumulator accumulator({string_type}, {string_type, number_type}, std::move(aggregates), small_batch_bytes);
    inBlocks(rows, 30'000, [&](size_t begin, size_t end)
    {
        accumulator.add({stringColumnOf(slice(keys, begin, end))}, {stringColumnOf(slice(values, begin, end)), columnOf(slice(numbers, begin, end))});
    });

    const Groups groups = groupsOf(accumulator, {string_type}, {string_type, string_type, std::make_shared<DataTypeUInt64>()});
    ASSERT_EQ(groups.rows, expected.size());

    const auto & sums = assert_cast<const ColumnUInt64 &>(*groups.aggregates[2]).getData();
    for (size_t row = 0; row < groups.rows; ++row)
    {
        const std::string key(groups.keys[0]->getDataAt(row));
        const auto it = expected.find(key);
        ASSERT_NE(it, expected.end()) << "an unexpected key " << key;
        const auto & [min, max, sum] = it->second;
        EXPECT_EQ(std::string(groups.aggregates[0]->getDataAt(row)), min) << "key " << key;
        EXPECT_EQ(std::string(groups.aggregates[1]->getDataAt(row)), max) << "key " << key;
        EXPECT_EQ(sums[row], sum) << "key " << key;
    }
}

/// A `FixedString(8)` key is grouped by its bytes as an integer as wide, and comes back as it was.
TEST_F(GPUAggregation, GroupByFixedString)
{
    const size_t rows = 200'000;
    std::mt19937_64 random(5);
    std::vector<std::string> keys(rows);
    std::vector<UInt8> values(rows);
    for (size_t i = 0; i < rows; ++i)
    {
        keys[i] = std::string(8, '\0');
        const UInt64 key = random() % 300;
        memcpy(keys[i].data(), &key, sizeof(key));
        keys[i][7] = static_cast<char>('a' + key % 26);
        values[i] = static_cast<UInt8>(random());
    }

    std::map<std::string, UInt64> expected;
    for (size_t i = 0; i < rows; ++i)
        expected[keys[i]] += values[i];

    const DataTypePtr key_type = std::make_shared<DataTypeFixedString>(8);
    const DataTypePtr value_type = std::make_shared<DataTypeUInt8>();

    GroupByGPUAccumulator accumulator(
        {key_type}, {value_type}, {{.aggregation = GPUAggregationKind::Sum, .column = 0, .result_type = GPUElementType::UInt64}}, small_batch_bytes);
    inBlocks(rows, 50'000, [&](size_t begin, size_t end)
    {
        auto column = ColumnFixedString::create(8);
        for (size_t i = begin; i < end; ++i)
            column->insertData(keys[i].data(), keys[i].size());
        accumulator.add({std::move(column)}, {columnOf(slice(values, begin, end))});
    });

    const Groups groups = groupsOf(accumulator, {key_type}, {std::make_shared<DataTypeUInt64>()});
    ASSERT_EQ(groups.rows, expected.size());

    const auto & sums = assert_cast<const ColumnUInt64 &>(*groups.aggregates[0]).getData();
    for (size_t row = 0; row < groups.rows; ++row)
    {
        const std::string key(groups.keys[0]->getDataAt(row));
        const auto it = expected.find(key);
        ASSERT_NE(it, expected.end());
        EXPECT_EQ(sums[row], it->second);
    }
}

/// Destroyed with batches in flight and without `finalize`, as a query that is cancelled is.
TEST_F(GPUAggregation, DestroyedWithoutFinalize)
{
    const DataTypePtr key_type = std::make_shared<DataTypeUInt64>();
    const DataTypePtr value_type = std::make_shared<DataTypeInt32>();
    std::vector<UInt64> keys(100'000);
    std::vector<Int32> values(keys.size());
    for (size_t i = 0; i < keys.size(); ++i)
    {
        keys[i] = i % 1000;
        values[i] = static_cast<Int32>(i);
    }

    GroupByGPUAccumulator accumulator(
        {key_type}, {value_type}, {{.aggregation = GPUAggregationKind::Sum, .column = 0, .result_type = GPUElementType::Int64}}, small_batch_bytes);
    inBlocks(keys.size(), 10'000, [&](size_t begin, size_t end)
    {
        accumulator.add({columnOf(slice(keys, begin, end))}, {columnOf(slice(values, begin, end))});
    });
}

#endif
