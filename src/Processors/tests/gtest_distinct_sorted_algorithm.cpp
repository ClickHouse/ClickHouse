#include <gtest/gtest.h>

#include <algorithm>
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
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "arrival"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt8>(), "flag")});
}

SortDescription runDescription()
{
    SortDescription description;
    description.emplace_back("key", 1, 1);
    description.emplace_back("flag", -1, 1);
    description.emplace_back("arrival", 1, 1);
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
        if (header.has("arrival"))
            columns[header.getPositionByName("arrival")]->insert(flag ? 0 : first_payload);
        columns[payload_pos]->insert(first_payload++);
        columns[flag_pos]->insert(flag);
    }
    return Chunk(std::move(columns), keys.size());
}

std::vector<UInt64> mergePayloads(
    const SharedHeaders & headers, SharedHeader output_header, std::vector<Chunks> sources,
    size_t block_size, bool dynamic_inputs, SortDescription description, size_t num_key_columns = 1)
{
    auto merge = std::make_shared<DistinctSortedTransform>(
        dynamic_inputs ? SharedHeaders{} : headers, std::move(output_header), std::move(description), num_key_columns, block_size, !dynamic_inputs);
    auto processors = std::make_shared<Processors>();
    if (dynamic_inputs)
    {
        for (size_t i = 0; i < sources.size(); ++i)
            merge->addInput(*headers[i]);
        merge->setHaveAllInputs();
    }
    auto input = merge->getInputs().begin();
    for (size_t i = 0; i < sources.size(); ++i)
    {
        auto source = std::make_shared<SourceFromChunks>(headers[i], std::move(sources[i]));
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

std::vector<UInt64> mergePayloads(
    const SharedHeader & header, std::vector<Chunks> sources, size_t block_size,
    bool dynamic_inputs = false, SortDescription description = runDescription(), size_t num_key_columns = 1)
{
    Block output_header = *header;
    output_header.erase("flag");
    if (output_header.has("arrival"))
        output_header.erase("arrival");
    SharedHeaders headers(sources.size(), header);
    return mergePayloads(headers, std::make_shared<const Block>(output_header), std::move(sources),
        block_size, dynamic_inputs, std::move(description), num_key_columns);
}

}

TEST(DistinctSortedAlgorithm, SuppressionAndFirstPayloadAcrossInputs)
{
    const auto header = makeHeader();
    for (const auto & order : {
        std::array<size_t, 4>{0, 1, 2, 3}, std::array<size_t, 4>{1, 0, 3, 2}, std::array<size_t, 4>{0, 2, 3, 1}, std::array<size_t, 4>{2, 3, 0, 1}})
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
    Block output_header = *header;
    output_header.erase("flag");
    output_header.erase("arrival");
    DistinctSortedAlgorithm algorithm({header}, std::make_shared<const Block>(output_header), runDescription(), /*num_key_columns=*/ 1, 2);
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
    Block output_header = *header;
    output_header.erase("flag");
    output_header.erase("arrival");
    DistinctSortedAlgorithm algorithm({header}, std::make_shared<const Block>(output_header), runDescription(), /*num_key_columns=*/ 1, 64);
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
    description.emplace_back("arrival", 1, 1);
    std::vector<Chunks> sources(3);
    sources[0].push_back(makeChunk(*header, {1u, 2u}, 100));
    sources[1].push_back(makeChunk(*header, {1u, 2u}, 200));
    sources[2].push_back(makeChunk(*header, {1u, 2u}, 100));
    EXPECT_EQ(mergePayloads(header, std::move(sources), 2, false, description, /*num_key_columns=*/ 2), (std::vector<UInt64>{200, 100, 201, 101}));
}

TEST(DistinctSortedAlgorithm, ConstantSparseAndReplicatedColumns)
{
    const auto header = makeHeader();
    std::vector<Chunks> sources(2);
    sources[0].emplace_back(Columns{
        ColumnConst::create(ColumnUInt64::create(1, 1), 1),
        ColumnConst::create(ColumnUInt64::create(1, 100), 1),
        ColumnConst::create(ColumnUInt64::create(1, 100), 1),
        ColumnConst::create(ColumnUInt8::create(1, UInt8{0}), 1)}, 1);
    auto sparse = ColumnSparse::create(ColumnUInt64::create());
    sparse->insert(2u);
    sparse->insert(3u);
    auto payloads = ColumnUInt64::create();
    payloads->insertValue(200);
    payloads->insertValue(201);
    ColumnPtr arrivals = payloads->clone();
    sources[0].emplace_back(Columns{
        std::move(sparse), ColumnReplicated::create(ColumnPtr(std::move(payloads))), std::move(arrivals),
        ColumnUInt8::create(2, UInt8{0})}, 2);
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
    output_header.erase("flag");
    output_header.erase("arrival");
    Chunks chunks;
    chunks.push_back(makeChunk(*header, {1u, 3u}, 100));
    chunks.push_back(makeChunk(*header, {1u, 2u}, 200));
    chunks.push_back(makeChunk(*header, {1u, 3u}, 300));
    chunks.push_back(makeChunk(*header, {1u, 3u}, 400));
    SortDescription keys;
    keys.emplace_back("key", 1, 1);
    auto tail = std::make_shared<MergeSorterSource>(header, std::move(chunks), keys, 1, 0, MergeSorter::Mode::MergeUniqueChunks);
    auto merge = std::make_shared<DistinctSortedTransform>(
        SharedHeaders{header}, std::make_shared<const Block>(output_header), runDescription(), /*num_key_columns=*/ 1, 2);
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
    for (const size_t flag_pos : {0, 1, 2, 3})
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

TEST(DistinctSortedAlgorithm, KeyOnlySuppressionAndDifferentInputLayouts)
{
    const auto ordinary_header = makeHeader();
    const auto suppression_header = std::make_shared<const Block>(Block{
        ordinary_header->getByName("flag"), ordinary_header->getByName("key"), ordinary_header->getByName("arrival")});
    const auto reordered_header = std::make_shared<const Block>(Block{
        ordinary_header->getByName("arrival"), ordinary_header->getByName("payload"),
        ordinary_header->getByName("flag"), ordinary_header->getByName("key")});
    auto output = *ordinary_header;
    output.erase("flag");
    output.erase("arrival");
    const auto output_header = std::make_shared<const Block>(output);
    for (const size_t block_size : {1, 2, 64})
    {
        for (const bool dynamic_inputs : {false, true})
        {
            std::vector<Chunks> inputs(3);
            inputs[0].push_back(makeChunk(*ordinary_header, {1u, 2u, 3u, 4u}, 100));
            inputs[0].push_back(makeChunk(*ordinary_header, {4u, 8u}, 104));
            auto keys = ColumnUInt64::create();
            keys->insertValue(1);
            keys->insertValue(3);
            inputs[1].emplace_back(Columns{ColumnUInt8::create(2, UInt8{1}), std::move(keys), ColumnUInt64::create(2, UInt64{0})}, 2);
            inputs[1].emplace_back(Columns{ColumnUInt8::create(1, UInt8{1}), ColumnUInt64::create(1, 8), ColumnUInt64::create(1, UInt64{0})}, 1);
            inputs[2].push_back(makeChunk(*reordered_header, {2u, 5u, 6u}, 200));
            inputs[2].push_back(makeChunk(*reordered_header, {7u, 9u}, 203));
            EXPECT_EQ(mergePayloads({ordinary_header, suppression_header, reordered_header}, output_header,
                std::move(inputs), block_size, dynamic_inputs, runDescription()),
                (std::vector<UInt64>{101, 103, 201, 202, 203, 204}));
        }
    }
}

TEST(DistinctSortedAlgorithm, RepeatedOutputColumns)
{
    const auto header = makeHeader();
    const auto output_header = std::make_shared<const Block>(Block{
        header->getByName("payload"), header->getByName("payload")});
    for (const size_t block_size : {2, 64})
    {
        DistinctSortedAlgorithm algorithm({header}, output_header, runDescription(), /*num_key_columns=*/ 1, block_size);
        IMergingAlgorithm::Inputs inputs(1);
        inputs[0].chunk = makeChunk(*header, {1u, 2u, 3u}, 100);
        algorithm.initialize(std::move(inputs));
        size_t rows = 0;
        while (true)
        {
            auto status = algorithm.merge();
            if (status.chunk)
            {
                ASSERT_EQ(status.chunk.getNumColumns(), 2);
                for (const auto & column : status.chunk.getColumns())
                    for (size_t row = 0; row < status.chunk.getNumRows(); ++row)
                        EXPECT_EQ(column->getUInt(row), 100 + rows + row);
                rows += status.chunk.getNumRows();
            }
            if (status.is_finished)
                break;
        }
        EXPECT_EQ(rows, 3);
    }
}

TEST(DistinctSortedAlgorithm, EarliestArrivalCanComeFromEitherInput)
{
    const auto header = makeHeader();
    for (const bool reverse_inputs : {false, true})
    {
        for (const size_t block_size : {1, 2, 64})
        {
            std::vector<Chunks> sources(2);
            sources[0].push_back(makeChunk(*header, {1u, 2u, 3u}, 100));
            auto chunk = makeChunk(*header, {1u, 2u, 3u}, 200);
            auto columns = chunk.detachColumns();
            auto arrivals = ColumnUInt64::create();
            for (const UInt64 arrival : {200, 50, 300})
                arrivals->insertValue(arrival);
            columns[header->getPositionByName("arrival")] = std::move(arrivals);
            sources[1].emplace_back(std::move(columns), 3);
            if (reverse_inputs)
                std::reverse(sources.begin(), sources.end());
            EXPECT_EQ(mergePayloads(header, std::move(sources), block_size), (std::vector<UInt64>{100, 201, 102}));
        }
    }
}

TEST(DistinctSortedAlgorithm, UnorderedInputsRetainOneRepresentativeAndApplySuppression)
{
    auto block = *makeHeader();
    block.erase("arrival");
    const auto header = std::make_shared<const Block>(std::move(block));
    SortDescription description;
    description.emplace_back("key", 1, 1);
    description.emplace_back("flag", -1, 1);
    for (const bool reverse_inputs : {false, true})
    {
        for (const size_t block_size : {1, 2, 64})
        {
            std::vector<Chunks> sources(3);
            sources[0].push_back(makeChunk(*header, {1u, 2u}, 100));
            sources[0].push_back(makeChunk(*header, {3u}, 102));
            sources[1].push_back(makeChunk(*header, {1u, 2u, 3u}, 200));
            sources[2].push_back(makeChunk(*header, {1u}, 0, 1));
            if (reverse_inputs)
                std::reverse(sources.begin(), sources.end());
            const auto payloads = mergePayloads(header, std::move(sources), block_size, false, description);
            ASSERT_EQ(payloads.size(), 2);
            EXPECT_TRUE(payloads[0] == 101 || payloads[0] == 201);
            EXPECT_TRUE(payloads[1] == 102 || payloads[1] == 202);
        }
    }
}
