#include <gtest/gtest.h>

#include <array>
#include <bit>
#include <limits>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Merges/DistinctSortedTransform.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/SortingTransform.h>
#include <QueryPipeline/QueryPipeline.h>

using namespace DB;

namespace
{

SharedHeader makeHeader(const DataTypePtr & key_type = std::make_shared<DataTypeUInt64>())
{
    return std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(key_type, "key"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "payload"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt8>(), "flag")});
}

SortDescription runDescription()
{
    SortDescription description;
    description.emplace_back("key", 1, 1);
    description.emplace_back("flag", -1, 1);
    return description;
}

Chunk makeChunk(const Block & header, const Array & keys, UInt64 first_payload, UInt8 flag = 0)
{
    auto columns = header.cloneEmptyColumns();
    const size_t key_pos = header.getPositionByName("key");
    const size_t payload_pos = header.getPositionByName("payload");
    const size_t flag_pos = header.getPositionByName("flag");
    for (const auto & key : keys)
    {
        columns[key_pos]->insert(key);
        columns[payload_pos]->insert(first_payload++);
        columns[flag_pos]->insert(flag);
    }
    return Chunk(std::move(columns), keys.size());
}

std::vector<UInt64> mergePayloads(
    const SharedHeader & header, std::vector<Chunks> sources, size_t block_size,
    bool dynamic_inputs = false, SortDescription description = runDescription())
{
    Block output_header = *header;
    const size_t flag_column_pos = header->getPositionByName("flag");
    output_header.erase(flag_column_pos);
    auto merge = std::make_shared<DistinctSortedTransform>(
        header, std::make_shared<const Block>(output_header), dynamic_inputs ? 0 : sources.size(),
        std::move(description), flag_column_pos, block_size, !dynamic_inputs);
    auto processors = std::make_shared<Processors>();
    if (dynamic_inputs)
    {
        for (size_t i = 0; i < sources.size(); ++i)
            merge->addInput(*header);
        merge->setHaveAllInputs();
    }
    auto input = merge->getInputs().begin();
    for (auto & chunks : sources)
    {
        auto source = std::make_shared<SourceFromChunks>(header, std::move(chunks));
        connect(source->getPort(), *input++);
        processors->push_back(std::move(source));
    }
    auto * output = &merge->getOutputs().front();
    processors->push_back(std::move(merge));
    QueryPipeline pipeline(QueryPlanResourceHolder{}, processors, output);
    PullingPipelineExecutor executor(pipeline);
    std::vector<UInt64> result;
    Block block;
    while (executor.pull(block))
    {
        EXPECT_EQ(block.columns(), 2);
        EXPECT_LE(block.rows(), block_size);
        for (size_t row = 0; row < block.rows(); ++row)
            result.push_back(block.getByPosition(1).column->getUInt(row));
    }
    return result;
}

}

TEST(DistinctSortedAlgorithm, SuppressionAndFirstPayloadAcrossInputs)
{
    const auto header = makeHeader();
    for (const auto & order : {
        std::array<size_t, 4>{0, 1, 2, 3}, std::array<size_t, 4>{1, 0, 3, 2}, std::array<size_t, 4>{0, 2, 3, 1}})
    {
        for (const size_t block_size : {1, 2, 4, 64})
        {
            std::vector<Chunks> sources(4);
            sources[0].push_back(makeChunk(*header, {2u, 4u, 7u}, 100));
            sources[0].push_back(makeChunk(*header, {7u, 8u}, 103));
            sources[1].push_back(makeChunk(*header, {1u, 2u}, 0, 1));
            sources[1].push_back(makeChunk(*header, {4u}, 0, 1));
            sources[2].push_back(makeChunk(*header, {2u, 4u, 7u}, 200));
            sources[2].push_back(makeChunk(*header, {8u, 9u}, 203));
            sources[3].push_back(makeChunk(*header, {3u}, 0, 1));
            sources[3].push_back(makeChunk(*header, {5u, 6u}, 0, 1));
            std::vector<Chunks> ordered;
            for (const auto source : order)
                ordered.push_back(std::move(sources[source]));
            EXPECT_EQ(mergePayloads(header, std::move(ordered), block_size, true), (std::vector<UInt64>{102, 104, 204}));
        }
    }
}

TEST(DistinctSortedAlgorithm, BoundaryKeysSurviveSourceRefills)
{
    const auto numbers = std::make_shared<DataTypeUInt64>();
    const String long_key(65536, 'x');
    const std::vector<std::pair<DataTypePtr, Array>> cases{
        {numbers, {1u, 2u, 3u, 4u}},
        {std::make_shared<DataTypeNullable>(numbers), {1u, 2u, 3u, Null{}}},
        {std::make_shared<DataTypeString>(), {long_key + "a", long_key + "b", long_key + "c", long_key + "d"}},
        {std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), {"a", "b", "c", "d"}},
        {std::make_shared<DataTypeArray>(numbers), {Array{1u}, Array{2u}, Array{3u}, Array{4u}}}};
    for (const auto & [type, keys] : cases)
    {
        SCOPED_TRACE(type->getName());
        const auto header = makeHeader(type);
        for (const size_t block_size : {1, 3, 64})
        {
            std::vector<Chunks> sources(3);
            sources[0].push_back(makeChunk(*header, {keys[0]}, 0, 1));
            sources[0].push_back(makeChunk(*header, {keys[2]}, 0, 1));
            sources[1].push_back(makeChunk(*header, {keys[0]}, 100));
            sources[1].push_back(makeChunk(*header, {keys[1]}, 101));
            sources[1].push_back(makeChunk(*header, {keys[2], keys[3]}, 102));
            sources[2].push_back(makeChunk(*header, keys, 200));
            EXPECT_EQ(mergePayloads(header, std::move(sources), block_size), (std::vector<UInt64>{101, 103}));
        }
    }
}

TEST(DistinctSortedAlgorithm, DuplicateBatchPrefixKeepsRemainingRange)
{
    const auto header = makeHeader();
    std::vector<Chunks> sources(2);
    sources[0].push_back(makeChunk(*header, {1u, 5u}, 100));
    sources[1].push_back(makeChunk(*header, {1u, 2u, 3u, 4u, 5u, 6u, 7u}, 200));
    EXPECT_EQ(mergePayloads(header, std::move(sources), 3), (std::vector<UInt64>{100, 201, 202, 203, 101, 205, 206}));
}

TEST(DistinctSortedAlgorithm, SuppressionMayContainSortEqualKeys)
{
    const auto header = makeHeader(std::make_shared<DataTypeFloat64>());
    const Float64 nan = std::numeric_limits<Float64>::quiet_NaN();
    const Float64 other_nan = std::bit_cast<Float64>(std::bit_cast<UInt64>(nan) ^ 1);
    for (const size_t block_size : {1, 2, 64})
    {
        std::vector<Chunks> sources(3);
        sources[0].push_back(makeChunk(*header, {-0., nan}, 100));
        sources[1].push_back(makeChunk(*header, {-0., 0.}, 0, 1));
        sources[1].push_back(makeChunk(*header, {nan, other_nan}, 0, 1));
        sources[2].push_back(makeChunk(*header, {0., 1., other_nan}, 200));
        EXPECT_EQ(mergePayloads(header, std::move(sources), block_size), (std::vector<UInt64>{201}));
    }
}

TEST(DistinctSortedAlgorithm, SortEqualKeysKeepFirstRepresentation)
{
    const auto header = makeHeader(std::make_shared<DataTypeFloat64>());
    const Float64 nan = std::numeric_limits<Float64>::quiet_NaN();
    const Float64 other_nan = std::bit_cast<Float64>(std::bit_cast<UInt64>(nan) ^ 1);
    std::vector<Chunks> sources(2);
    sources[0].push_back(makeChunk(*header, {-0., nan}, 100));
    sources[1].push_back(makeChunk(*header, {0., other_nan}, 200));
    EXPECT_EQ(mergePayloads(header, std::move(sources), 1), (std::vector<UInt64>{100, 101}));
}

TEST(DistinctSortedAlgorithm, EmptyInputsAndChunks)
{
    const auto header = makeHeader();
    for (const bool dynamic_inputs : {false, true})
    {
        EXPECT_TRUE(mergePayloads(header, {}, 2, dynamic_inputs).empty());
        EXPECT_TRUE(mergePayloads(header, std::vector<Chunks>(2), 2, dynamic_inputs).empty());
        std::vector<Chunks> sources(2);
        sources[0].push_back(makeChunk(*header, {}, 0));
        sources[0].push_back(makeChunk(*header, {1u}, 100));
        sources[0].push_back(makeChunk(*header, {}, 0));
        sources[0].push_back(makeChunk(*header, {1u, 2u}, 101));
        sources[0].push_back(makeChunk(*header, {}, 0));
        EXPECT_EQ(mergePayloads(header, std::move(sources), 2, dynamic_inputs), (std::vector<UInt64>{100, 102}));
    }
}

TEST(DistinctSortedAlgorithm, SuppressionOnlyProgressIsBounded)
{
    const auto header = makeHeader();
    DistinctSortedAlgorithm algorithm(header, 1, runDescription(), 2, 2);
    IMergingAlgorithm::Inputs inputs(1);
    inputs[0].chunk = makeChunk(*header, {1u, 1u, 2u, 3u, 4u}, 0, 1);
    algorithm.initialize(std::move(inputs));
    for (size_t step = 0; step < 2; ++step)
    {
        auto status = algorithm.merge();
        EXPECT_FALSE(status.is_finished);
        EXPECT_EQ(status.required_source, -1);
        EXPECT_EQ(status.chunk.getNumRows(), 0);
        EXPECT_EQ(status.chunk.getNumColumns(), 2);
    }
    auto refill = algorithm.merge();
    EXPECT_EQ(refill.required_source, 0);
    EXPECT_TRUE(algorithm.merge().is_finished);
}

TEST(DistinctSortedAlgorithm, WholeUniqueChunkForwardsColumns)
{
    const auto header = makeHeader();
    DistinctSortedAlgorithm algorithm(header, 1, runDescription(), 2, 64);
    IMergingAlgorithm::Inputs inputs(1);
    inputs[0].chunk = makeChunk(*header, {1u, 2u, 3u}, 100);
    const auto * payload = inputs[0].chunk.getColumns()[1].get();
    algorithm.initialize(std::move(inputs));
    auto status = algorithm.merge();
    EXPECT_EQ(status.required_source, 0);
    EXPECT_EQ(status.chunk.getNumRows(), 3);
    EXPECT_EQ(status.chunk.getNumColumns(), 2);
    EXPECT_EQ(status.chunk.getColumns()[1].get(), payload);
    EXPECT_TRUE(algorithm.merge().is_finished);
}

TEST(DistinctSortedAlgorithm, CompositeKeysAndDescendingOrder)
{
    const auto header = makeHeader();
    SortDescription description;
    description.emplace_back("key", 1, 1);
    description.emplace_back("payload", -1, -1);
    description.emplace_back("flag", -1, 1);
    std::vector<Chunks> sources(3);
    sources[0].push_back(makeChunk(*header, {1u, 2u}, 100));
    sources[1].push_back(makeChunk(*header, {1u, 2u}, 200));
    sources[2].push_back(makeChunk(*header, {1u, 2u}, 100));
    EXPECT_EQ(mergePayloads(header, std::move(sources), 2, false, description), (std::vector<UInt64>{200, 100, 201, 101}));
}

TEST(DistinctSortedAlgorithm, ConstantSparseAndReplicatedColumns)
{
    const auto header = makeHeader();
    std::vector<Chunks> sources(2);
    sources[0].emplace_back(Columns{
        ColumnConst::create(ColumnUInt64::create(1, 1), 1),
        ColumnConst::create(ColumnUInt64::create(1, 100), 1),
        ColumnConst::create(ColumnUInt8::create(1, UInt8{0}), 1)}, 1);
    auto sparse = ColumnSparse::create(ColumnUInt64::create());
    sparse->insert(2u);
    sparse->insert(3u);
    auto payloads = ColumnUInt64::create();
    payloads->insertValue(200);
    payloads->insertValue(201);
    sources[0].emplace_back(Columns{
        std::move(sparse), ColumnReplicated::create(ColumnPtr(std::move(payloads))), ColumnUInt8::create(2, UInt8{0})}, 2);
    auto replicated = makeChunk(*header, {1u, 4u}, 300);
    auto columns = replicated.detachColumns();
    columns[0] = ColumnReplicated::create(columns[0]);
    sources[1].emplace_back(std::move(columns), 2);
    EXPECT_EQ(mergePayloads(header, std::move(sources), 2), (std::vector<UInt64>{100, 200, 201, 301}));
}

TEST(DistinctSortedAlgorithm, UniqueTailSuppressesDuplicatesAcrossItsChunks)
{
    const auto header = makeHeader();
    Block output_header = *header;
    output_header.erase(2);
    Chunks chunks;
    chunks.push_back(makeChunk(*header, {1u, 3u}, 100));
    chunks.push_back(makeChunk(*header, {1u, 2u}, 200));
    chunks.push_back(makeChunk(*header, {1u, 3u}, 300));
    chunks.push_back(makeChunk(*header, {1u, 3u}, 400));
    SortDescription keys;
    keys.emplace_back("key", 1, 1);
    auto tail = std::make_shared<MergeSorterSource>(header, std::move(chunks), keys, 1, 0, MergeSorter::Mode::MergeUniqueChunks);
    auto merge = std::make_shared<DistinctSortedTransform>(
        header, std::make_shared<const Block>(output_header), 1, runDescription(), 2, 2);
    connect(tail->getPort(), merge->getInputs().front());
    auto * output = &merge->getOutputs().front();
    auto processors = std::make_shared<Processors>();
    processors->push_back(std::move(tail));
    processors->push_back(std::move(merge));
    QueryPipeline pipeline(QueryPlanResourceHolder{}, processors, output);
    PullingPipelineExecutor executor(pipeline);
    std::vector<UInt64> payloads;
    Block block;
    while (executor.pull(block))
        for (size_t row = 0; row < block.rows(); ++row)
            payloads.push_back(block.getByPosition(1).column->getUInt(row));
    EXPECT_EQ(payloads, (std::vector<UInt64>{100, 201, 101}));
}

TEST(DistinctSortedAlgorithm, OutputColumnsWithDifferentFlagPositions)
{
    for (const size_t flag_pos : {0, 1, 2})
    {
        SCOPED_TRACE(flag_pos);
        Block block = *makeHeader();
        const auto flag = block.getByName("flag");
        block.erase("flag");
        block.insert(flag_pos, flag);
        const auto header = std::make_shared<const Block>(std::move(block));
        for (const size_t block_size : {2, 64})
        {
            std::vector<Chunks> sources(3);
            sources[0].push_back(makeChunk(*header, {1u, 2u, 3u, 4u, 5u}, 100));
            sources[0].push_back(makeChunk(*header, {5u, 6u, 7u}, 105));
            sources[0].push_back(makeChunk(*header, {20u, 21u}, 108));
            sources[1].push_back(makeChunk(*header, {5u, 8u}, 200));
            sources[2].push_back(makeChunk(*header, {1u, 7u}, 0, 1));
            EXPECT_EQ(mergePayloads(header, std::move(sources), block_size),
                (std::vector<UInt64>{101, 102, 103, 104, 106, 201, 108, 109}));
        }
    }
}
