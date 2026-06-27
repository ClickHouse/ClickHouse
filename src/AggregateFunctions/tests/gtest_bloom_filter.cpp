#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupBloomFilterData.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <base/scope_guard.h>
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
    restored.read(in, /*expected_filter_size_bytes=*/64, /*expected_num_hashes=*/3, /*expected_seed=*/7);

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
    restored.read(in, /*expected_filter_size_bytes=*/64, /*expected_num_hashes=*/3, /*expected_seed=*/7);

    EXPECT_TRUE(restored.isInitialized());
    EXPECT_TRUE(restored.contains(reinterpret_cast<const char *>(&value), sizeof(value)));
}

/// Security regression: a forged aggregate state that declares filter_size_bytes much larger
/// than the declared type parameters must be rejected before any allocation or payload read.
/// Without the fix this would allocate up to BLOOM_FILTER_MAX_SIZE_BYTES of memory first.
TEST(BloomFilterData, ForgedLargeSizeRejectedBeforeAllocation)
{
    /// Build a tiny forged payload: header claims MAX size with has_data=1.
    WriteBufferFromOwnString forged;
    writeVarUInt(BLOOM_FILTER_MAX_SIZE_BYTES, forged); // filter_size_bytes
    writeVarUInt(size_t{3}, forged);                   // num_hashes
    writeVarUInt(size_t{0}, forged);                   // seed
    writeBinary(UInt8(1), forged);                     // has_data = 1 (payload follows, but is absent)

    AggregateFunctionGroupBloomFilterData victim;
    ReadBufferFromString in(forged.str());

    /// The declared parameters of the aggregate function are small (64 bytes, 3 hashes, seed 0).
    /// read() must throw INCORRECT_DATA immediately after reading the header, without allocating
    /// BLOOM_FILTER_MAX_SIZE_BYTES or attempting to readStrict that many bytes.
    EXPECT_THROW(
        victim.read(in, /*expected_filter_size_bytes=*/64, /*expected_num_hashes=*/3, /*expected_seed=*/0),
        Exception);
}

/// Verify that a mismatch in num_hashes is also caught before allocation.
TEST(BloomFilterData, ForgedNumHashesMismatchRejected)
{
    WriteBufferFromOwnString forged;
    writeVarUInt(size_t{64}, forged);  // filter_size_bytes matches declared
    writeVarUInt(size_t{10}, forged);  // num_hashes mismatches declared (3)
    writeVarUInt(size_t{0}, forged);   // seed
    writeBinary(UInt8(1), forged);     // has_data = 1

    AggregateFunctionGroupBloomFilterData victim;
    ReadBufferFromString in(forged.str());

    EXPECT_THROW(
        victim.read(in, /*expected_filter_size_bytes=*/64, /*expected_num_hashes=*/3, /*expected_seed=*/0),
        Exception);
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

/// Regression: create() must not allocate the bitset eagerly.
/// Empty/skipped groups (e.g. from -If combinator when all conditions are false,
/// or all-NULL nullable inputs) must stay compact in memory and serialize with has_data = 0.
TEST(BloomFilterAggregateFunction, CreateDoesNotAllocateBitset)
{
    tryRegisterAggregateFunctions();

    const auto value_type = std::make_shared<DataTypeUInt64>();
    const auto aggregate_function = createGroupBloomFilterAggregate(value_type);

    Arena arena;
    AggregateDataPtr place = arena.alignedAlloc(aggregate_function->sizeOfData(), aggregate_function->alignOfData());
    aggregate_function->create(place);
    SCOPE_EXIT({ aggregate_function->destroy(place); });

    const auto & data = *reinterpret_cast<const AggregateFunctionGroupBloomFilterData *>(place);

    // Parameters must be stored ...
    EXPECT_EQ(data.filter_size_bytes, 64u);
    EXPECT_EQ(data.num_hashes, 3u);
    EXPECT_EQ(data.seed, 0u);
    // ... but the bitset must NOT have been allocated yet.
    EXPECT_FALSE(data.isInitialized());

    // Serialization of the empty state must be compact: 3 varints (params) + 1 flag byte,
    // no payload.  The filter_size_bytes is 64 so a populated state would be 64+ bytes.
    WriteBufferFromOwnString out;
    aggregate_function->serialize(place, out, std::nullopt);
    EXPECT_LT(out.str().size(), 16u); // header only, far below 64 bytes of payload
}

/// Regression: add() must lazily allocate the bitset on first value and then find it.
TEST(BloomFilterAggregateFunction, AddLazilyAllocatesAndFinds)
{
    tryRegisterAggregateFunctions();

    const auto value_type = std::make_shared<DataTypeUInt64>();
    const auto aggregate_function = createGroupBloomFilterAggregate(value_type);

    Arena arena;
    AggregateDataPtr place = arena.alignedAlloc(aggregate_function->sizeOfData(), aggregate_function->alignOfData());
    aggregate_function->create(place);
    SCOPE_EXIT({ aggregate_function->destroy(place); });

    const auto & data = *reinterpret_cast<const AggregateFunctionGroupBloomFilterData *>(place);
    EXPECT_FALSE(data.isInitialized());

    // Insert one value through the aggregate function's add() interface.
    auto value_column = ColumnUInt64::create();
    value_column->insertValue(UInt64(42));
    const IColumn * columns[] = {value_column.get()};
    aggregate_function->add(place, columns, 0, &arena);

    // Now the bitset must have been allocated and the value must be found.
    EXPECT_TRUE(data.isInitialized());
    UInt64 probe = 42;
    EXPECT_TRUE(data.contains(reinterpret_cast<const char *>(&probe), sizeof(probe)));
    probe = 100;
    // 100 was never inserted; this is probabilistic but with 64 bytes / 3 hashes false positives are negligible.
    EXPECT_FALSE(data.contains(reinterpret_cast<const char *>(&probe), sizeof(probe)));
}
