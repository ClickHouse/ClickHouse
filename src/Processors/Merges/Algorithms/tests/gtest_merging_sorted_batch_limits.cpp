#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
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
