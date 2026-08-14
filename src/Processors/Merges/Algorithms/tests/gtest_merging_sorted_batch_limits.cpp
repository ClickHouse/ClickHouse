#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/tests/gtest_global_register.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Merges/Algorithms/MergingSortedAlgorithm.h>
#include <gtest/gtest.h>

using namespace DB;

namespace
{

Chunk makeChunk(const std::vector<UInt64> & keys)
{
    auto key = ColumnUInt64::create();
    auto payload = ColumnString::create();

    for (const auto value : keys)
    {
        key->insertValue(value);
        payload->insertData("01234567890123456789", 20);
    }

    return Chunk(Columns{std::move(key), std::move(payload)}, keys.size());
}

Chunk makeChunk(UInt64 first_key, size_t rows = 100)
{
    std::vector<UInt64> keys;
    keys.reserve(rows);

    for (size_t row = 0; row < rows; ++row)
        keys.push_back(first_key + row);

    return makeChunk(keys);
}

Chunk makeLowCardinalityChunk(UInt64 first_key, size_t rows = 100)
{
    auto key = ColumnUInt64::create();
    auto payload_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    auto payload = payload_type->createColumn();

    for (size_t row = 0; row < rows; ++row)
    {
        key->insertValue(first_key + row);
        payload->insert(Field(String("repeated value")));
    }

    return Chunk(Columns{std::move(key), std::move(payload)}, rows);
}

size_t getFirstChunkSize(
    SortingQueueStrategy strategy,
    size_t max_block_size_bytes,
    bool use_average_block_size,
    UInt64 second_first_key = 50)
{
    auto header = std::make_shared<const Block>(Block{
        {ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "key"},
        {ColumnString::create(), std::make_shared<DataTypeString>(), "payload"},
    });

    SortDescription description;
    description.emplace_back("key");

    const size_t num_inputs = use_average_block_size ? 3 : 2;
    MergingSortedAlgorithm algorithm(
        header,
        num_inputs,
        description,
        1000,
        max_block_size_bytes,
        std::nullopt,
        strategy,
        0,
        nullptr,
        std::nullopt,
        use_average_block_size);

    IMergingAlgorithm::Inputs inputs(num_inputs);
    if (use_average_block_size)
    {
        std::vector<UInt64> first_keys{0, 1, 2, 3, 4};
        for (UInt64 key = 1000; key < 1095; ++key)
            first_keys.push_back(key);

        inputs[0].chunk = makeChunk(first_keys);
        inputs[1].chunk = makeChunk(5, 200);
        inputs[2].chunk = makeChunk(200);
    }
    else
    {
        inputs[0].chunk = makeChunk(0);
        inputs[1].chunk = makeChunk(second_first_key);
    }
    algorithm.initialize(std::move(inputs));

    auto status = algorithm.merge();
    chassert(status.chunk);
    return status.chunk.getNumRows();
}

size_t getFirstLowCardinalityChunkSize(SortingQueueStrategy strategy, size_t max_block_size_bytes)
{
    auto payload_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    auto header = std::make_shared<const Block>(Block{
        {ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "key"},
        {payload_type->createColumn(), payload_type, "payload"},
    });

    SortDescription description;
    description.emplace_back("key");

    MergingSortedAlgorithm algorithm(
        header,
        2,
        description,
        1000,
        max_block_size_bytes,
        std::nullopt,
        strategy,
        0,
        nullptr,
        std::nullopt,
        false);

    IMergingAlgorithm::Inputs inputs(2);
    inputs[0].chunk = makeLowCardinalityChunk(0);
    inputs[1].chunk = makeLowCardinalityChunk(50);
    algorithm.initialize(std::move(inputs));

    auto status = algorithm.merge();
    chassert(status.chunk);
    return status.chunk.getNumRows();
}

Chunk makeAggregateFunctionChunk(const AggregateFunctionPtr & function, const std::vector<UInt64> & keys)
{
    auto key = ColumnUInt64::create();
    auto payload = ColumnAggregateFunction::create(function);
    auto values = ColumnUInt64::create();

    for (UInt64 value = 0; value < 1000; ++value)
        values->insertValue(value);

    const IColumn * arguments[] = {values.get()};
    for (const auto value : keys)
    {
        key->insertValue(value);
        payload->insertDefault();
        for (size_t row = 0; row < values->size(); ++row)
            function->add(payload->getData().back(), arguments, row, &payload->createOrGetArena());
    }

    return Chunk(Columns{std::move(key), std::move(payload)}, keys.size());
}

/// The two inputs of `getFirstAggregateFunctionChunkSize` interleave, so that neither of them is
/// consumed as a whole chunk: the batch queue would insert such a chunk directly, bypassing the
/// block size limit in bytes (see the fast-forward optimization, also present in the default queue).
constexpr size_t aggregate_function_rows_per_input = 50;
constexpr size_t aggregate_function_second_first_key = 25;
constexpr size_t aggregate_function_input_rows = 2 * aggregate_function_rows_per_input;

size_t getFirstAggregateFunctionChunkSize(SortingQueueStrategy strategy)
{
    tryRegisterAggregateFunctions();

    auto argument_type = std::make_shared<DataTypeUInt64>();
    DataTypes argument_types{argument_type};
    AggregateFunctionProperties properties;
    auto function = AggregateFunctionFactory::instance().get("uniqExact", NullsAction::EMPTY, argument_types, {}, properties);
    auto payload_type = std::make_shared<DataTypeAggregateFunction>(function, argument_types, Array{});
    auto header = std::make_shared<const Block>(Block{
        {ColumnUInt64::create(), argument_type, "key"},
        {payload_type->createColumn(), payload_type, "payload"},
    });

    SortDescription description;
    description.emplace_back("key");

    MergingSortedAlgorithm algorithm(
        header,
        2,
        description,
        1000,
        1024,
        std::nullopt,
        strategy,
        0,
        nullptr,
        std::nullopt,
        false);

    std::vector<UInt64> first_keys;
    std::vector<UInt64> second_keys;
    for (size_t row = 0; row < aggregate_function_rows_per_input; ++row)
    {
        first_keys.push_back(row);
        second_keys.push_back(aggregate_function_second_first_key + row);
    }

    IMergingAlgorithm::Inputs inputs(2);
    inputs[0].chunk = makeAggregateFunctionChunk(function, first_keys);
    inputs[1].chunk = makeAggregateFunctionChunk(function, second_keys);
    algorithm.initialize(std::move(inputs));

    auto status = algorithm.merge();
    chassert(status.chunk);
    return status.chunk.getNumRows();
}

}

TEST(MergingSortedBatchLimits, MaxBlockSizeBytes)
{
    EXPECT_EQ(
        getFirstChunkSize(SortingQueueStrategy::Default, 100, false),
        getFirstChunkSize(SortingQueueStrategy::Batch, 100, false));
}

TEST(MergingSortedBatchLimits, MaxBlockSizeBytesFastForward)
{
    EXPECT_EQ(100, getFirstChunkSize(SortingQueueStrategy::Default, 100, false, 200));
    EXPECT_EQ(100, getFirstChunkSize(SortingQueueStrategy::Batch, 100, false, 200));
}

TEST(MergingSortedBatchLimits, AverageBlockSize)
{
    EXPECT_EQ(
        getFirstChunkSize(SortingQueueStrategy::Default, 0, true),
        getFirstChunkSize(SortingQueueStrategy::Batch, 0, true));
}

TEST(MergingSortedBatchLimits, MaxBlockSizeBytesWithRepeatedLowCardinalityValues)
{
    EXPECT_EQ(
        getFirstLowCardinalityChunkSize(SortingQueueStrategy::Default, 100),
        getFirstLowCardinalityChunkSize(SortingQueueStrategy::Batch, 100));
}

TEST(MergingSortedBatchLimits, MaxBlockSizeBytesWithAggregateFunctionState)
{
    const auto default_chunk_size = getFirstAggregateFunctionChunkSize(SortingQueueStrategy::Default);
    EXPECT_EQ(default_chunk_size, getFirstAggregateFunctionChunkSize(SortingQueueStrategy::Batch));
    /// The exact number of rows depends on `sizeOfData` of the aggregate state, which differs
    /// between build types, so only check that the byte limit really did flush the block early
    /// (otherwise the comparison above would be vacuous).
    EXPECT_GT(default_chunk_size, 0uz);
    EXPECT_LT(default_chunk_size, aggregate_function_input_rows);
}
