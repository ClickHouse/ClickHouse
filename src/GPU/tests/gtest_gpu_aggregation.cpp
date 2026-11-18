#include "config.h"

#if USE_GPU

#include <GPU/GPUAccumulator.h>
#include <GPU/GPUDevice.h>
#include <GPU/GPUTypeMapping.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ServerSettings.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <cuda_runtime_api.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <functional>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <optional>
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

/// Small batches, so that a test sends many, and every lane takes several of them.
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

/// Groups on the device as the processors of `GPUAggregatingStep` do: the batches of one uploader are handed to the
/// lanes in turn, and the lanes are merged by parts of the keys or, with an `avg` or without keys, into one.
class DeviceGroupBy
{
public:
    DeviceGroupBy(
        const DataTypes & key_types,
        const DataTypes & value_types,
        std::vector<GPUGroupByAggregate> aggregates_,
        size_t batch_bytes,
        std::optional<CompiledGPUExpression> where_ = {},
        size_t num_lanes = 3)
        : keys(key_types)
        , values(value_types)
        , aggregates(std::move(aggregates_))
        , where(std::move(where_))
        , by_parts(!keys.empty() && std::ranges::none_of(aggregates, [](const auto & aggregate) { return aggregate.aggregation == GPUAggregationKind::Mean; }))
        , uploader(uploadTypes(), batch_bytes)
    {
        const auto decision = std::make_shared<GPUGroupingDecision>();
        for (size_t i = 0; i < num_lanes; ++i)
        {
            lanes.push_back(std::make_unique<GPUGroupingLane>(keys, values, aggregates, where, decision));
            if (by_parts)
                lanes.back()->splitIntoParts(num_lanes);
        }
    }

    void add(const Columns & key_columns, const Columns & value_columns, const Columns & where_columns = {})
    {
        Columns columns = key_columns;
        columns.insert(columns.end(), value_columns.begin(), value_columns.end());
        columns.insert(columns.end(), where_columns.begin(), where_columns.end());
        if (uploader.wouldOverflow(columns))
            send();
        uploader.add(columns);
    }

    /// The lanes that hold the groups, once merged, and how many each holds.
    std::vector<std::pair<GPUGroupingLane *, size_t>> finish()
    {
        send();
        std::vector<std::pair<GPUGroupingLane *, size_t>> result;
        if (by_parts)
        {
            for (auto & lane : lanes)
                lane->partition(lanes.size());
            for (size_t part = 0; part < lanes.size(); ++part)
            {
                merged.push_back(std::make_unique<GPUGroupingLane>(keys, values, aggregates, where, std::make_shared<GPUGroupingDecision>()));
                for (const auto & lane : lanes)
                    merged.back()->absorbPart(*lane, part);
                result.emplace_back(merged.back().get(), merged.back()->finalize());
            }
            return result;
        }

        size_t groups = 0;
        for (const auto & lane : lanes)
            groups += lane->numGroups().value_or(0);
        if (lanes.front()->numGroups().value_or(0) != 0)
            lanes.front()->reserveGroups(groups);
        for (size_t i = 1; i < lanes.size(); ++i)
            lanes.front()->absorb(*lanes[i]);
        result.emplace_back(lanes.front().get(), lanes.front()->finalize());
        return result;
    }

private:
    DataTypes uploadTypes() const
    {
        DataTypes types = keys;
        types.insert(types.end(), values.begin(), values.end());
        if (where)
            for (const auto & column : where->columns)
                types.push_back(column.type);
        return types;
    }

    void send()
    {
        if (auto batch = uploader.send())
            lanes[next_lane++ % lanes.size()]->add(std::move(*batch));
    }

    const DataTypes keys;
    const DataTypes values;
    const std::vector<GPUGroupByAggregate> aggregates;
    const std::optional<CompiledGPUExpression> where;
    const bool by_parts;

    std::vector<std::unique_ptr<GPUGroupingLane>> lanes;
    std::vector<std::unique_ptr<GPUGroupingLane>> merged;
    size_t next_lane = 0;
    GPUBatchUploader uploader;
};

/// Reduces on the device without `GROUP BY`, as `GPUAggregatingStep` does: one uploader and the reductions of its batches.
class DeviceReduce
{
public:
    DeviceReduce(const DataTypes & argument_types, const DataTypes & result_types, const std::vector<GPUAggregationKind> & aggregations, size_t batch_bytes)
        : accumulator(argument_types, result_types, aggregations), uploader(argument_types, batch_bytes)
    {
    }

    void add(const Columns & columns)
    {
        if (uploader.wouldOverflow(columns))
            send();
        uploader.add(columns);
    }

    std::vector<Field> finalize()
    {
        send();
        return accumulator.finalize();
    }

private:
    void send()
    {
        if (auto batch = uploader.send())
            accumulator.add(std::move(*batch));
    }

    GPUAccumulator accumulator;
    GPUBatchUploader uploader;
};

/// The groups on the device, by the row of each, of every lane that holds them.
struct Groups
{
    MutableColumns keys;
    MutableColumns aggregates;
    size_t rows = 0;
    /// Before a `HAVING`.
    size_t rows_grouped = 0;
};

Groups groupsOf(
    DeviceGroupBy & group_by,
    const DataTypes & key_types,
    const std::vector<DataTypePtr> & aggregate_types,
    const CompiledGPUExpression * having = nullptr,
    const std::vector<size_t> & having_columns = {})
{
    Groups groups;
    for (const auto & type : key_types)
        groups.keys.push_back(type->createColumn());
    for (const auto & type : aggregate_types)
        groups.aggregates.push_back(type->createColumn());

    for (auto [lane, rows] : group_by.finish())
    {
        groups.rows_grouped += rows;
        if (having && rows != 0)
            rows = lane->filterGroups(*having, having_columns);
        if (rows == 0)
            continue;

        MutableColumns lane_keys;
        MutableColumns lane_aggregates;
        for (const auto & type : key_types)
            lane_keys.push_back(type->createColumn());
        for (const auto & type : aggregate_types)
            lane_aggregates.push_back(type->createColumn());
        lane->copyGroupsTo(lane_keys, lane_aggregates);
        for (size_t i = 0; i < lane_keys.size(); ++i)
            groups.keys[i]->insertRangeFrom(*lane_keys[i], 0, rows);
        for (size_t i = 0; i < lane_aggregates.size(); ++i)
            groups.aggregates[i]->insertRangeFrom(*lane_aggregates[i], 0, rows);
        groups.rows += rows;
    }
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

    DeviceReduce accumulator(argument_types, result_types, aggregations, small_batch_bytes);
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

    DeviceReduce accumulator({type, type}, {sum_type, type}, {GPUAggregationKind::Sum, GPUAggregationKind::Min}, small_batch_bytes);
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
    DeviceReduce accumulator({type, type}, {type, type}, {GPUAggregationKind::Min, GPUAggregationKind::Max}, small_batch_bytes);
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

    DeviceGroupBy accumulator({key_type}, {value_type}, std::move(aggregates), batch_bytes);
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
    DeviceGroupBy accumulator(
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

    DeviceGroupBy accumulator({string_type}, {string_type, number_type}, std::move(aggregates), small_batch_bytes);
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

    DeviceGroupBy accumulator(
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

    DeviceGroupBy accumulator(
        {key_type}, {value_type}, {{.aggregation = GPUAggregationKind::Sum, .column = 0, .result_type = GPUElementType::Int64}}, small_batch_bytes);
    inBlocks(keys.size(), 10'000, [&](size_t begin, size_t end)
    {
        accumulator.add({columnOf(slice(keys, begin, end))}, {columnOf(slice(values, begin, end))});
    });
}

namespace
{

const ActionsDAG::Node & addFunction(ActionsDAG & dag, const String & function, ActionsDAG::NodeRawConstPtrs children, const String & name)
{
    return dag.addFunction(FunctionFactory::instance().get(function, getContext().context), std::move(children), name);
}

const ActionsDAG::Node & addConstant(ActionsDAG & dag, const DataTypePtr & type, const Field & value, const String & name)
{
    auto column = type->createColumn();
    column->insert(value);
    return dag.addColumn(ColumnConst::create(std::move(column), 1), type, name);
}

CompiledGPUExpression compilePredicate(ActionsDAG & dag, const ActionsDAG::Node & predicate)
{
    dag.getOutputs() = {&predicate};
    String refusal;
    std::optional<CompiledGPUExpression> compiled = compileGPUFilter(dag, predicate.result_name, refusal);
    if (!compiled)
        throw std::runtime_error("The device refuses the predicate: " + refusal);
    return std::move(*compiled);
}

/// `k < max_key AND v > min_value` over the columns `k`, a `UInt64`, and `v`, an `Int32`.
CompiledGPUExpression keyAndValueBounds(UInt64 max_key, Int32 min_value)
{
    tryRegisterFunctions();
    const DataTypePtr key_type = std::make_shared<DataTypeUInt64>();
    const DataTypePtr value_type = std::make_shared<DataTypeInt32>();

    ActionsDAG dag;
    const auto & k = dag.addInput("k", key_type);
    const auto & v = dag.addInput("v", value_type);
    const auto & key_bound = addFunction(dag, "less", {&k, &addConstant(dag, key_type, max_key, "max_key")}, "key_bound");
    const auto & value_bound = addFunction(dag, "greater", {&v, &addConstant(dag, value_type, min_value, "min_value")}, "value_bound");
    return compilePredicate(dag, addFunction(dag, "and", {&key_bound, &value_bound}, "where"));
}

/// Groups `values` by `keys` by `sum` and `count`, with the `WHERE` of `keyAndValueBounds`, and checks every group.
void checkGroupByWhere(const std::vector<UInt64> & keys, const std::vector<Int32> & values, UInt64 max_key, Int32 min_value, bool keyed = true)
{
    std::map<UInt64, std::pair<Int64, UInt64>> expected;
    for (size_t i = 0; i < keys.size(); ++i)
    {
        if (keys[i] < max_key && values[i] > min_value)
        {
            auto & [sum, count] = expected[keyed ? keys[i] : 0];
            sum += values[i];
            ++count;
        }
    }

    const DataTypePtr key_type = std::make_shared<DataTypeUInt64>();
    const DataTypePtr value_type = std::make_shared<DataTypeInt32>();
    CompiledGPUExpression where = keyAndValueBounds(max_key, min_value);
    ASSERT_EQ(where.columns.size(), 2);

    std::vector<GPUGroupByAggregate> aggregates = {
        {.aggregation = GPUAggregationKind::Sum, .column = 0, .result_type = GPUElementType::Int64},
        {.aggregation = GPUAggregationKind::Count, .column = GPUGroupByAggregate::no_column, .result_type = GPUElementType::UInt64},
    };
    const DataTypes key_types = keyed ? DataTypes{key_type} : DataTypes{};
    DeviceGroupBy accumulator(key_types, {value_type}, std::move(aggregates), small_batch_bytes, std::move(where));

    inBlocks(keys.size(), 65'536, [&](size_t begin, size_t end)
    {
        const ColumnPtr k = columnOf(slice(keys, begin, end));
        const ColumnPtr v = columnOf(slice(values, begin, end));
        accumulator.add(keyed ? Columns{k} : Columns{}, {v}, {k, v});
    });

    const Groups groups = groupsOf(accumulator, key_types, {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeUInt64>()});
    ASSERT_EQ(groups.rows, expected.size());

    const auto & sums = assert_cast<const ColumnInt64 &>(*groups.aggregates[0]).getData();
    const auto & counts = assert_cast<const ColumnUInt64 &>(*groups.aggregates[1]).getData();
    for (size_t row = 0; row < groups.rows; ++row)
    {
        const UInt64 key = keyed ? assert_cast<const ColumnUInt64 &>(*groups.keys[0]).getData()[row] : 0;
        const auto it = expected.find(key);
        ASSERT_NE(it, expected.end()) << "an unexpected key " << key;
        EXPECT_EQ(sums[row], it->second.first) << "key " << key;
        EXPECT_EQ(counts[row], it->second.second) << "key " << key;
        expected.erase(it);
    }
}

void randomKeysAndValues(size_t rows, UInt64 num_keys, std::vector<UInt64> & keys, std::vector<Int32> & values, UInt64 seed)
{
    std::mt19937_64 random(seed);
    keys.resize(rows);
    values.resize(rows);
    for (size_t i = 0; i < rows; ++i)
    {
        keys[i] = random() % num_keys;
        values[i] = static_cast<Int32>(random() % 2'000'001) - 1'000'000;
    }
}

}

/// The `WHERE` leaves out few rows, which are grouped as nulls of the first key.
TEST_F(GPUAggregation, GroupByWhereOfFewRows)
{
    std::vector<UInt64> keys;
    std::vector<Int32> values;
    randomKeysAndValues(1'000'000, 1000, keys, values, 11);
    checkGroupByWhere(keys, values, 990, -950'000);
}

/// The `WHERE` leaves out most rows, which are compacted away before they are grouped.
TEST_F(GPUAggregation, GroupByWhereOfManyRows)
{
    std::vector<UInt64> keys;
    std::vector<Int32> values;
    randomKeysAndValues(1'000'000, 1000, keys, values, 12);
    checkGroupByWhere(keys, values, 500, 600'000);
}

/// Keys in runs with a `WHERE`: the batches reduced over their runs are compacted, as the runs are of keys without nulls.
TEST_F(GPUAggregation, GroupByWhereOfSortedKeys)
{
    std::vector<UInt64> keys(1'000'000);
    std::vector<Int32> values(keys.size());
    std::mt19937_64 random(13);
    for (size_t i = 0; i < keys.size(); ++i)
    {
        keys[i] = i / 41;
        values[i] = static_cast<Int32>(random() % 2'000'001) - 1'000'000;
    }
    checkGroupByWhere(keys, values, 20'000, -900'000);
}

TEST_F(GPUAggregation, GroupByWhereOfNoRows)
{
    std::vector<UInt64> keys;
    std::vector<Int32> values;
    randomKeysAndValues(300'000, 100, keys, values, 14);
    checkGroupByWhere(keys, values, 100, 2'000'000);
}

/// Without keys, the `WHERE` masks the key of every row that the device groups by.
TEST_F(GPUAggregation, GroupByWhereWithoutKeys)
{
    std::vector<UInt64> keys;
    std::vector<Int32> values;
    randomKeysAndValues(700'000, 100, keys, values, 15);
    checkGroupByWhere(keys, values, 60, 0, /*keyed=*/ false);
    checkGroupByWhere(keys, values, 100, 2'000'000, /*keyed=*/ false);
}

/// `HAVING sum > threshold AND k < max_key`, over an aggregate and a key of the groups.
TEST_F(GPUAggregation, GroupByHaving)
{
    tryRegisterFunctions();
    std::vector<UInt64> keys;
    std::vector<Int32> values;
    randomKeysAndValues(1'000'000, 5000, keys, values, 16);

    std::map<UInt64, Int64> sums;
    for (size_t i = 0; i < keys.size(); ++i)
        sums[keys[i]] += values[i];
    const Int64 threshold = 0;
    const UInt64 max_key = 2500;

    const DataTypePtr key_type = std::make_shared<DataTypeUInt64>();
    const DataTypePtr value_type = std::make_shared<DataTypeInt32>();
    const DataTypePtr sum_type = std::make_shared<DataTypeInt64>();

    ActionsDAG dag;
    const auto & sum = dag.addInput("sum", sum_type);
    const auto & k = dag.addInput("k", key_type);
    const auto & large = addFunction(dag, "greater", {&sum, &addConstant(dag, sum_type, threshold, "threshold")}, "large");
    const auto & small_key = addFunction(dag, "less", {&k, &addConstant(dag, key_type, max_key, "max_key")}, "small_key");
    const CompiledGPUExpression having = compilePredicate(dag, addFunction(dag, "and", {&large, &small_key}, "having"));

    std::vector<size_t> having_columns;
    for (const auto & column : having.columns)
        having_columns.push_back(column.name == "k" ? 0 : 1);

    DeviceGroupBy accumulator(
        {key_type}, {value_type}, {{.aggregation = GPUAggregationKind::Sum, .column = 0, .result_type = GPUElementType::Int64}}, small_batch_bytes);
    inBlocks(keys.size(), 65'536, [&](size_t begin, size_t end)
    {
        accumulator.add({columnOf(slice(keys, begin, end))}, {columnOf(slice(values, begin, end))});
    });

    const Groups groups = groupsOf(accumulator, {key_type}, {sum_type}, &having, having_columns);
    ASSERT_EQ(groups.rows_grouped, sums.size());

    std::map<UInt64, Int64> expected;
    for (const auto & [key, value] : sums)
    {
        if (value > threshold && key < max_key)
            expected.emplace(key, value);
    }
    ASSERT_EQ(groups.rows, expected.size());

    const auto & got_keys = assert_cast<const ColumnUInt64 &>(*groups.keys[0]).getData();
    const auto & got_sums = assert_cast<const ColumnInt64 &>(*groups.aggregates[0]).getData();
    ASSERT_EQ(got_keys.size(), expected.size());
    for (size_t row = 0; row < got_keys.size(); ++row)
    {
        const auto it = expected.find(got_keys[row]);
        ASSERT_NE(it, expected.end()) << "an unexpected key " << got_keys[row];
        EXPECT_EQ(got_sums[row], it->second) << "key " << got_keys[row];
    }
}

#endif
