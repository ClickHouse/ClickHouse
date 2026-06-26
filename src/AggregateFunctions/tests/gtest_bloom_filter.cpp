#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupBloomFilterData.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionsBloomFilter.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_register.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

Array bloomFilterParameters()
{
    return {Field(UInt64(64)), Field(UInt64(3))};
}

AggregateFunctionPtr createGroupBloomFilterAggregate(const DataTypePtr & value_type)
{
    tryRegisterAggregateFunctions();

    AggregateFunctionProperties properties;
    return AggregateFunctionFactory::instance().get(
        AggregateFunctionGroupBloomFilterData::name,
        NullsAction::EMPTY,
        {value_type},
        bloomFilterParameters(),
        properties);
}

DataTypePtr createAggregateStateType(const AggregateFunctionPtr & aggregate_function, const DataTypePtr & value_type)
{
    return std::make_shared<DataTypeAggregateFunction>(aggregate_function, DataTypes{value_type}, bloomFilterParameters());
}

template <typename ColumnType, typename Value>
ColumnAggregateFunction::MutablePtr createAggregateColumn(
    const AggregateFunctionPtr & aggregate_function,
    const std::vector<Value> & values)
{
    auto aggregate_column = ColumnAggregateFunction::create(aggregate_function);
    auto value_column = ColumnType::create();
    for (const auto & value : values)
        value_column->insertValue(value);

    const IColumn * columns[] = {value_column.get()};
    Arena arena;
    for (size_t i = 0; i < values.size(); ++i)
    {
        aggregate_column->insertDefault();
        aggregate_function->add(aggregate_column->getData()[i], columns, i, &arena);
    }

    return aggregate_column;
}

ColumnPtr executeBloomFilterContains(
    const ColumnPtr & bloom_column,
    const DataTypePtr & bloom_type,
    const ColumnPtr & value_column,
    const DataTypePtr & value_type,
    size_t rows)
{
    FunctionBloomFilterContains function;
    ColumnsWithTypeAndName arguments;
    arguments.emplace_back(bloom_column, bloom_type, "bf");
    arguments.emplace_back(value_column, value_type, "value");
    return function.executeImpl(arguments, std::make_shared<DataTypeUInt8>(), rows);
}

}

TEST(BloomFilterData, OptimalParameterValidation)
{
    EXPECT_THROW(bloomFilterOptimalParams(10, 0.0), Exception);
    EXPECT_THROW(bloomFilterOptimalParams(10, 1.0), Exception);
    EXPECT_THROW(bloomFilterOptimalParams(0, 0.5), Exception);

    auto [sz, k] = bloomFilterOptimalParams(1, 0.999);
    EXPECT_EQ(sz, 8u);
    EXPECT_GE(k, 1u);
}

/// When k_opt = -ln(p)/ln2 > BLOOM_FILTER_MAX_HASHES, the size must be enlarged so that
/// the actual FPR with k = BLOOM_FILTER_MAX_HASHES does not exceed the requested p.
TEST(BloomFilterData, OptimalParamsRecomputesWhenHashCapExceeded)
{
    auto check_fpr = [](size_t n, double p)
    {
        auto [size_bytes, k] = bloomFilterOptimalParams(n, p);
        EXPECT_LE(size_bytes, BLOOM_FILTER_MAX_SIZE_BYTES);
        EXPECT_EQ(k, BLOOM_FILTER_MAX_HASHES);
        /// actual_fpr = (1 - exp(-k*n/m))^k
        const double m_bits = static_cast<double>(size_bytes) * 8.0;
        const double actual_fpr = std::pow(
            1.0 - std::exp(-static_cast<double>(k) * static_cast<double>(n) / m_bits),
            static_cast<double>(k));
        EXPECT_LE(actual_fpr, p);
    };

    check_fpr(1, 1e-10);    /// k_opt ≈ 33
    check_fpr(1, 1e-100);   /// k_opt ≈ 332; regression: was returning ~64 bytes with actual FPR ≈ 4e-29

    /// recomputed size > 256 MB → throw
    EXPECT_THROW(bloomFilterOptimalParams(100'000'000, 1e-7), Exception);
}

TEST(BloomFilterData, EmptyStateAndContains)
{
    AggregateFunctionGroupBloomFilterData data;
    EXPECT_FALSE(data.isInitialized());

    UInt64 value = 42;
    EXPECT_FALSE(data.contains(reinterpret_cast<const char *>(&value), sizeof(value)));
}

TEST(BloomFilterData, MergeEmptyAndInitializedStates)
{
    AggregateFunctionGroupBloomFilterData lhs;
    AggregateFunctionGroupBloomFilterData empty_rhs;
    lhs.merge(empty_rhs);
    EXPECT_FALSE(lhs.isInitialized());

    AggregateFunctionGroupBloomFilterData rhs;
    rhs.init(64, 3, 7);
    UInt64 value = 42;
    rhs.add(reinterpret_cast<const char *>(&value), sizeof(value));

    lhs.merge(rhs);
    EXPECT_TRUE(lhs.isInitialized());
    EXPECT_TRUE(lhs.contains(reinterpret_cast<const char *>(&value), sizeof(value)));
    EXPECT_EQ(lhs.filter_size_bytes, rhs.filter_size_bytes);
    EXPECT_EQ(lhs.num_hashes, rhs.num_hashes);
    EXPECT_EQ(lhs.seed, rhs.seed);
}

TEST(BloomFilterData, MergeDifferentParametersThrows)
{
    AggregateFunctionGroupBloomFilterData lhs;
    lhs.init(64, 3, 7);

    AggregateFunctionGroupBloomFilterData rhs;
    rhs.init(128, 3, 7);

    EXPECT_THROW(lhs.merge(rhs), Exception);
}

TEST(BloomFilterData, EmptySerializationRoundTrip)
{
    AggregateFunctionGroupBloomFilterData empty;
    empty.filter_size_bytes = 64;
    empty.num_hashes = 3;
    empty.seed = 7;

    WriteBufferFromOwnString out;
    empty.write(out);

    AggregateFunctionGroupBloomFilterData restored;
    ReadBufferFromString in(out.str());
    restored.read(in);

    EXPECT_FALSE(restored.isInitialized());
    EXPECT_EQ(restored.filter_size_bytes, empty.filter_size_bytes);
    EXPECT_EQ(restored.num_hashes, empty.num_hashes);
    EXPECT_EQ(restored.seed, empty.seed);
}

TEST(BloomFilterData, InitializedSerializationRoundTrip)
{
    AggregateFunctionGroupBloomFilterData data;
    data.init(64, 3, 7);
    UInt64 value = 42;
    data.add(reinterpret_cast<const char *>(&value), sizeof(value));

    WriteBufferFromOwnString out;
    data.write(out);

    AggregateFunctionGroupBloomFilterData restored;
    ReadBufferFromString in(out.str());
    restored.read(in);

    EXPECT_TRUE(restored.isInitialized());
    EXPECT_TRUE(restored.contains(reinterpret_cast<const char *>(&value), sizeof(value)));
}

TEST(BloomFilterAggregateFunction, DoesNotAllocateMemoryInArena)
{
    const auto value_type = std::make_shared<DataTypeUInt64>();
    const auto aggregate_function = createGroupBloomFilterAggregate(value_type);

    EXPECT_FALSE(aggregate_function->allocatesMemoryInArena());
}

TEST(BloomFilterContains, WrongNumericBloomColumnThrowsAfterTypeValidation)
{
    const auto value_type = std::make_shared<DataTypeUInt64>();
    const auto aggregate_function = createGroupBloomFilterAggregate(value_type);
    const auto bloom_type = createAggregateStateType(aggregate_function, value_type);

    auto wrong_bloom_column = ColumnUInt64::create();
    wrong_bloom_column->insertValue(42);

    auto probe_column = ColumnUInt64::create();
    probe_column->insertValue(42);

    EXPECT_THROW(
        executeBloomFilterContains(std::move(wrong_bloom_column), bloom_type, std::move(probe_column), value_type, 1),
        Exception);
}

TEST(BloomFilterContains, WrongConstNumericBloomColumnThrowsAfterTypeValidation)
{
    const auto value_type = std::make_shared<DataTypeUInt64>();
    const auto aggregate_function = createGroupBloomFilterAggregate(value_type);
    const auto bloom_type = createAggregateStateType(aggregate_function, value_type);

    auto wrong_bloom_column = ColumnUInt64::create();
    wrong_bloom_column->insertValue(42);
    ColumnPtr const_wrong_bloom_column = ColumnConst::create(std::move(wrong_bloom_column), 1)->getPtr();

    auto probe_column = ColumnUInt64::create();
    probe_column->insertValue(42);

    EXPECT_THROW(
        executeBloomFilterContains(const_wrong_bloom_column, bloom_type, std::move(probe_column), value_type, 1),
        Exception);
}

TEST(BloomFilterContains, WrongStringBloomColumnThrowsAfterTypeValidation)
{
    const auto value_type = std::make_shared<DataTypeString>();
    const auto aggregate_function = createGroupBloomFilterAggregate(value_type);
    const auto bloom_type = createAggregateStateType(aggregate_function, value_type);

    auto wrong_bloom_column = ColumnUInt64::create();
    wrong_bloom_column->insertValue(42);

    auto probe_column = ColumnString::create();
    probe_column->insertData("hello", 5);

    EXPECT_THROW(
        executeBloomFilterContains(std::move(wrong_bloom_column), bloom_type, std::move(probe_column), value_type, 1),
        Exception);
}

TEST(BloomFilterContains, WrongConstStringBloomColumnThrowsAfterTypeValidation)
{
    const auto value_type = std::make_shared<DataTypeString>();
    const auto aggregate_function = createGroupBloomFilterAggregate(value_type);
    const auto bloom_type = createAggregateStateType(aggregate_function, value_type);

    auto wrong_bloom_column = ColumnUInt64::create();
    wrong_bloom_column->insertValue(42);
    ColumnPtr const_wrong_bloom_column = ColumnConst::create(std::move(wrong_bloom_column), 1)->getPtr();

    auto probe_column = ColumnString::create();
    probe_column->insertData("hello", 5);

    EXPECT_THROW(
        executeBloomFilterContains(const_wrong_bloom_column, bloom_type, std::move(probe_column), value_type, 1),
        Exception);
}

TEST(BloomFilterContains, WrongConstDateTime64BloomColumnThrowsAfterTypeValidation)
{
    constexpr UInt32 scale = 3;
    const auto value_type = std::make_shared<DataTypeDateTime64>(scale);
    const auto aggregate_function = createGroupBloomFilterAggregate(value_type);
    const auto bloom_type = createAggregateStateType(aggregate_function, value_type);

    auto wrong_bloom_column = ColumnUInt64::create();
    wrong_bloom_column->insertValue(42);
    ColumnPtr const_wrong_bloom_column = ColumnConst::create(std::move(wrong_bloom_column), 1)->getPtr();

    auto probe_column = ColumnDecimal<DateTime64>::create(0, scale);
    probe_column->insertValue(DateTime64(123456));

    EXPECT_THROW(
        executeBloomFilterContains(const_wrong_bloom_column, bloom_type, std::move(probe_column), value_type, 1),
        Exception);
}

TEST(BloomFilterContains, WrongDateTime64BloomColumnThrowsAfterTypeValidation)
{
    constexpr UInt32 scale = 3;
    const auto value_type = std::make_shared<DataTypeDateTime64>(scale);
    const auto aggregate_function = createGroupBloomFilterAggregate(value_type);
    const auto bloom_type = createAggregateStateType(aggregate_function, value_type);

    auto wrong_bloom_column = ColumnUInt64::create();
    wrong_bloom_column->insertValue(42);

    auto probe_column = ColumnDecimal<DateTime64>::create(0, scale);
    probe_column->insertValue(DateTime64(123456));

    EXPECT_THROW(
        executeBloomFilterContains(std::move(wrong_bloom_column), bloom_type, std::move(probe_column), value_type, 1),
        Exception);
}
