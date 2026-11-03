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
#include <Processors/Merges/Algorithms/GroupLimitSortedAlgorithm.h>
#include <Processors/Merges/GroupLimitSortedTransform.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/SortingTransform.h>
#include <QueryPipeline/QueryPipeline.h>

using namespace DB;

/// The window `[0, 1)` of `DISTINCT` takes the distinct merge path, and any other window ranks rows one by
/// one, so the merge cases check both windows. Each source of the merge is a sequence of chunks, so the merge
/// refills its inputs.
namespace
{

SharedHeader makeHeader(
    const DataTypePtr & seen_rows_type = std::make_shared<DataTypeUInt8>(),
    const DataTypePtr & key_type = std::make_shared<DataTypeUInt64>())
{
    return std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(key_type, "key"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "payload"),
        ColumnWithTypeAndName(seen_rows_type, "seen_rows")});
}

SortDescription keyDescription()
{
    SortDescription description;
    description.emplace_back("key", 1, 1);
    return description;
}

Chunk makeChunk(const Block & header, const Array & keys, UInt64 first_payload, UInt64 seen_rows = 0)
{
    auto columns = header.cloneEmptyColumns();
    const size_t key_pos = header.getPositionByName("key");
    const size_t payload_pos = header.getPositionByName("payload");
    const size_t seen_rows_pos = header.getPositionByName("seen_rows");
    for (const auto & key : keys)
    {
        columns[key_pos]->insert(key);
        columns[payload_pos]->insert(first_payload++);
        columns[seen_rows_pos]->insert(seen_rows);
    }
    return Chunk(std::move(columns), keys.size());
}

/// The kept ranks `[offset, end)` of each group.
struct Window
{
    UInt64 offset;
    UInt64 end;
};

/// Connects each of `sources` to the next input of `merge`, runs the pipeline and returns the payloads of the
/// output rows, whose chunks hold at most `block_size` rows.
std::vector<UInt64> pullPayloads(const Processors & sources, ProcessorPtr merge, size_t block_size)
{
    auto processors = std::make_shared<Processors>();
    auto input = merge->getInputs().begin();
    for (const auto & source : sources)
    {
        connect(source->getOutputs().front(), *input++);
        processors->push_back(source);
    }
    auto * output = &merge->getOutputs().front();
    processors->push_back(std::move(merge));
    QueryPipeline pipeline(QueryPlanResourceHolder{}, processors, output);
    PullingPipelineExecutor executor(pipeline);
    std::vector<UInt64> payloads;
    Block block;
    while (executor.pull(block))
    {
        EXPECT_EQ(block.columns(), 2);
        EXPECT_LE(block.rows(), block_size);
        const auto & payload = block.getByName("payload").column;
        for (size_t row = 0; row < block.rows(); ++row)
            payloads.push_back(payload->getUInt(row));
    }
    return payloads;
}

/// Merges `sources` through `GroupLimitSortedTransform` and returns the payloads of the output rows. With
/// `dynamic_inputs`, the inputs are added after the transform is constructed.
std::vector<UInt64> mergePayloads(
    const SharedHeaders & headers, const NameAndTypePair & seen_rows, SharedHeader output_header,
    std::vector<Chunks> sources, size_t block_size, Window window, bool dynamic_inputs, SortDescription key_description)
{
    auto merge = std::make_shared<GroupLimitSortedTransform>(
        "GroupLimitSortedTransform", dynamic_inputs ? SharedHeaders{} : headers, std::move(output_header),
        std::move(key_description), seen_rows, window.offset, window.end, block_size, !dynamic_inputs);
    if (dynamic_inputs)
    {
        for (size_t i = 0; i < sources.size(); ++i)
            merge->addInput(*headers[i]);
        merge->setHaveAllInputs();
    }
    Processors source_processors;
    for (size_t i = 0; i < sources.size(); ++i)
        source_processors.push_back(std::make_shared<SourceFromChunks>(headers[i], std::move(sources[i])));
    return pullPayloads(source_processors, std::move(merge), block_size);
}

/// The same with every source of `header`, whose seen counts the output omits.
std::vector<UInt64> mergePayloads(
    const SharedHeader & header, std::vector<Chunks> sources, size_t block_size, Window window,
    bool dynamic_inputs = false, SortDescription key_description = keyDescription())
{
    Block output_header = *header;
    output_header.erase("seen_rows");
    const SharedHeaders headers(sources.size(), header);
    return mergePayloads(
        headers, {"seen_rows", header->getByName("seen_rows").type}, std::make_shared<const Block>(output_header),
        std::move(sources), block_size, window, dynamic_inputs, std::move(key_description));
}

/// The window of `DISTINCT` with its expected payloads, and a ranked window with its own.
using WindowCases = std::vector<std::pair<Window, std::vector<UInt64>>>;

}

TEST(GroupLimitSortedAlgorithm, SeenOnlyProgressIsBounded)
{
    /// Consumed seen rows count toward the block size, in the window of `DISTINCT` and in ranked windows.
    /// The ranks `[0, 1)` of `DISTINCT` and `LIMIT 1 BY` use the distinct merge path; any other window ranks
    /// rows one by one. The seen counts of an unbounded window need 64 bits.
    const auto limit_by_header = makeHeader(std::make_shared<DataTypeUInt64>());
    struct Case
    {
        SharedHeader header;
        UInt64 offset;
        UInt64 end;
        UInt64 seen_rows;
    };
    const std::vector<Case> cases{
        {makeHeader(), 0, 1, 1}, {limit_by_header, 0, 1, 7}, {limit_by_header, 1, 3, 2},
        {limit_by_header, 5, 7, std::numeric_limits<UInt64>::max()}};
    for (const auto & [header, offset, end, seen_rows] : cases)
    {
        SCOPED_TRACE(::testing::Message() << "window=[" << offset << ", " << end << "), seen_rows=" << seen_rows);
        Block output_header = *header;
        output_header.erase("seen_rows");
        GroupLimitSortedAlgorithm algorithm(
            {header}, std::make_shared<const Block>(output_header), keyDescription(),
            NameAndTypePair{"seen_rows", header->getByName("seen_rows").type}, offset, end, 2);
        IMergingAlgorithm::Inputs inputs(1);
        inputs[0].chunk = makeChunk(*header, {1u, 1u, 2u, 3u, 4u}, 0, seen_rows);
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
}

TEST(GroupLimitSortedAlgorithm, WholeChunkOfDistinctKeysForwardsColumns)
{
    const auto header = makeHeader();
    Block output_header = *header;
    output_header.erase("seen_rows");
    GroupLimitSortedAlgorithm algorithm(
        {header}, std::make_shared<const Block>(output_header), keyDescription(),
        NameAndTypePair{"seen_rows", header->getByName("seen_rows").type}, /*group_offset_=*/ 0,
        /*group_limit_end_=*/ 1, 64);
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

TEST(GroupLimitSortedAlgorithm, WithoutSeenRowsEveryRowIsAnInputRow)
{
    /// Without the seen-rows column, as in the merge of the runs of a sort that keeps one row per key, every row
    /// takes a rank in its group: the distinct window keeps the first row of each key in source order, and a
    /// ranked window keeps the rows at its ranks.
    const auto header = std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "key"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "payload")});
    auto make_input = [&](const std::vector<UInt64> & keys, UInt64 first_payload)
    {
        auto columns = header->cloneEmptyColumns();
        for (const auto key : keys)
        {
            columns[0]->insert(key);
            columns[1]->insert(first_payload++);
        }
        return Chunk(std::move(columns), keys.size());
    };

    struct Case
    {
        UInt64 offset;
        UInt64 end;
        std::vector<UInt64> first_keys;
        std::vector<UInt64> second_keys;
        std::vector<std::pair<UInt64, UInt64>> expected;
    };
    /// The first input holds the payloads from 10, the second from 20, and equal keys take the first input first.
    /// An input holds at most the window end of rows of a key.
    const std::vector<Case> cases{
        {0, 1, {1, 2}, {1, 3}, {{1, 10}, {2, 11}, {3, 21}}},
        {1, 2, {1, 2}, {1, 2, 2}, {{1, 20}, {2, 21}}},
    };
    for (const auto & [offset, end, first_keys, second_keys, expected] : cases)
    {
        SCOPED_TRACE(::testing::Message() << "window=[" << offset << ", " << end << ")");
        GroupLimitSortedAlgorithm algorithm(
            {header, header}, header, keyDescription(), /*seen_rows_column=*/ std::nullopt, offset, end, 64);
        IMergingAlgorithm::Inputs inputs(2);
        inputs[0].chunk = make_input(first_keys, 10);
        inputs[1].chunk = make_input(second_keys, 20);
        algorithm.initialize(std::move(inputs));

        /// A request for the next chunk of an exhausted input needs no answer: the merge goes on without it.
        std::vector<std::pair<UInt64, UInt64>> rows;
        bool finished = false;
        for (size_t step = 0; step < 16 && !finished; ++step)
        {
            auto status = algorithm.merge();
            const auto & columns = status.chunk.getColumns();
            for (size_t row = 0; row < status.chunk.getNumRows(); ++row)
                rows.emplace_back(columns[0]->getUInt(row), columns[1]->getUInt(row));
            finished = status.is_finished;
        }
        EXPECT_TRUE(finished);
        EXPECT_EQ(rows, expected);
    }
}

TEST(GroupLimitSortedAlgorithm, InputRowsWithoutSeenCountsRankAfterSeenRows)
{
    /// Input runs store no seen counts. The merge adds zero counts of the seen-count type, so input rows rank
    /// after the seen rows of their key, in the window of `DISTINCT` and in a ranked window.
    const auto seen_type = std::make_shared<DataTypeUInt8>();
    const auto seen_header = std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "key"), ColumnWithTypeAndName(seen_type, "seen_rows")});
    const auto input_header = std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "key"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "payload")});

    struct Case
    {
        UInt64 offset;
        UInt64 end;
        std::vector<UInt64> input_keys;
        std::vector<std::pair<UInt64, UInt64>> expected;
    };
    /// Key 1 was seen once before spilling. The input rows hold payloads from 10, at most the window end of
    /// rows of a key.
    const std::vector<Case> cases{
        {0, 1, {1, 2}, {{2, 11}}},
        {1, 3, {1, 1, 2, 2}, {{1, 10}, {1, 11}, {2, 13}}},
    };
    for (const auto & [offset, end, input_keys, expected] : cases)
    {
        SCOPED_TRACE(::testing::Message() << "window=[" << offset << ", " << end << ")");
        GroupLimitSortedAlgorithm algorithm(
            {seen_header, input_header}, input_header, keyDescription(), NameAndTypePair{"seen_rows", seen_type}, offset, end, 64);
        IMergingAlgorithm::Inputs inputs(2);
        auto seen_columns = seen_header->cloneEmptyColumns();
        seen_columns[0]->insert(1u);
        seen_columns[1]->insert(1u);
        inputs[0].chunk = Chunk(std::move(seen_columns), 1);
        auto input_columns = input_header->cloneEmptyColumns();
        UInt64 payload = 10;
        for (const auto key : input_keys)
        {
            input_columns[0]->insert(key);
            input_columns[1]->insert(payload++);
        }
        inputs[1].chunk = Chunk(std::move(input_columns), input_keys.size());
        algorithm.initialize(std::move(inputs));

        std::vector<std::pair<UInt64, UInt64>> rows;
        bool finished = false;
        for (size_t step = 0; step < 16 && !finished; ++step)
        {
            auto status = algorithm.merge();
            const auto & columns = status.chunk.getColumns();
            for (size_t row = 0; row < status.chunk.getNumRows(); ++row)
                rows.emplace_back(columns[0]->getUInt(row), columns[1]->getUInt(row));
            finished = status.is_finished;
        }
        EXPECT_TRUE(finished);
        EXPECT_EQ(rows, expected);
    }
}

TEST(GroupLimitSortedAlgorithm, SeenRowsAndFirstPayloadsAcrossInputs)
{
    /// Inputs of input rows and of seen rows interleave in every registration order. Equal keys take their
    /// input rows in source order, also when the earlier source has to refill first.
    const auto header = makeHeader();
    const WindowCases cases{{{0, 1}, {102, 104, 204}}, {{0, 2}, {100, 101, 102, 103, 104, 203, 204}}};
    for (const auto & [window, expected] : cases)
    for (const auto & order : {std::array<size_t, 4>{0, 1, 2, 3}, std::array<size_t, 4>{1, 0, 3, 2}, std::array<size_t, 4>{0, 2, 3, 1}})
    for (const size_t block_size : {1, 2, 4, 64})
    {
        SCOPED_TRACE(::testing::Message() << "window=[" << window.offset << ", " << window.end << "), order="
            << order[0] << order[1] << order[2] << order[3] << ", block_size=" << block_size);
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
        EXPECT_EQ(mergePayloads(header, std::move(ordered), block_size, window, /*dynamic_inputs=*/ true), expected);
    }
}

TEST(GroupLimitSortedAlgorithm, BoundaryKeysSurviveSourceRefills)
{
    /// The last key of a group can belong to a chunk that a refill replaces, so the merge keeps a copy of it.
    /// The key types own their values in different ways.
    const auto numbers = std::make_shared<DataTypeUInt64>();
    const String long_key(65536, 'x');
    const std::vector<std::pair<DataTypePtr, Array>> key_cases{
        {numbers, {1u, 2u, 3u, 4u}},
        {std::make_shared<DataTypeNullable>(numbers), {1u, 2u, 3u, Null{}}},
        {std::make_shared<DataTypeString>(), {long_key + "a", long_key + "b", long_key + "c", long_key + "d"}},
        {std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), {"a", "b", "c", "d"}},
        {std::make_shared<DataTypeArray>(numbers), {Array{1u}, Array{2u}, Array{3u}, Array{4u}}}};
    const WindowCases cases{{{0, 1}, {101, 103}}, {{0, 2}, {100, 101, 201, 102, 103, 203}}};
    for (const auto & [type, keys] : key_cases)
    for (const auto & [window, expected] : cases)
    for (const size_t block_size : {1, 3, 64})
    {
        SCOPED_TRACE(::testing::Message() << type->getName() << ", window=[" << window.offset << ", " << window.end
            << "), block_size=" << block_size);
        const auto header = makeHeader(std::make_shared<DataTypeUInt8>(), type);
        std::vector<Chunks> sources(3);
        sources[0].push_back(makeChunk(*header, {keys[0]}, 0, 1));
        sources[0].push_back(makeChunk(*header, {keys[2]}, 0, 1));
        sources[1].push_back(makeChunk(*header, {keys[0]}, 100));
        sources[1].push_back(makeChunk(*header, {keys[1]}, 101));
        sources[1].push_back(makeChunk(*header, {keys[2], keys[3]}, 102));
        sources[2].push_back(makeChunk(*header, keys, 200));
        EXPECT_EQ(mergePayloads(header, std::move(sources), block_size, window), expected);
    }
}

TEST(GroupLimitSortedAlgorithm, BatchStartingWithTheLastKeyKeepsTheRestOfItsRange)
{
    /// The rows of the second input that precede the next key of the first one form a batch, which starts
    /// with the key merged last. The batch continues that key and keeps the rest of its range.
    const auto header = makeHeader();
    const WindowCases cases{{{0, 1}, {100, 201, 202, 203, 101, 205, 206}}, {{0, 2}, {100, 200, 201, 202, 203, 101, 204, 205, 206}}};
    for (const auto & [window, expected] : cases)
    {
        SCOPED_TRACE(::testing::Message() << "window=[" << window.offset << ", " << window.end << ")");
        std::vector<Chunks> sources(2);
        sources[0].push_back(makeChunk(*header, {1u, 5u}, 100));
        sources[1].push_back(makeChunk(*header, {1u, 2u, 3u, 4u, 5u, 6u, 7u}, 200));
        EXPECT_EQ(mergePayloads(header, std::move(sources), 3, window), expected);
    }
}

TEST(GroupLimitSortedAlgorithm, SeenRowsMayContainSortEqualKeys)
{
    /// Seen runs keep the keys that the in-memory filter tells apart by their bits, which can be equal in the
    /// sort order: -0 and 0, or NaNs with different bits. They form one group, whose seen counts add up.
    const auto header = makeHeader(std::make_shared<DataTypeUInt8>(), std::make_shared<DataTypeFloat64>());
    const Float64 nan = std::numeric_limits<Float64>::quiet_NaN();
    const Float64 other_nan = std::bit_cast<Float64>(std::bit_cast<UInt64>(nan) ^ 1);
    const WindowCases cases{{{0, 1}, {201}}, {{0, 3}, {100, 201, 101}}};
    for (const auto & [window, expected] : cases)
    for (const size_t block_size : {1, 2, 64})
    {
        SCOPED_TRACE(::testing::Message() << "window=[" << window.offset << ", " << window.end << "), block_size=" << block_size);
        std::vector<Chunks> sources(3);
        sources[0].push_back(makeChunk(*header, {-0., nan}, 100));
        sources[1].push_back(makeChunk(*header, {-0., 0.}, 0, 1));
        sources[1].push_back(makeChunk(*header, {nan, other_nan}, 0, 1));
        sources[2].push_back(makeChunk(*header, {0., 1., other_nan}, 200));
        EXPECT_EQ(mergePayloads(header, std::move(sources), block_size, window), expected);
    }
}

TEST(GroupLimitSortedAlgorithm, SortEqualKeysKeepTheirRepresentation)
{
    /// -0 and 0, or NaNs with different bits, are one key in the sort order. The kept rows keep their own
    /// values.
    const auto header = makeHeader(std::make_shared<DataTypeUInt8>(), std::make_shared<DataTypeFloat64>());
    const Float64 nan = std::numeric_limits<Float64>::quiet_NaN();
    const Float64 other_nan = std::bit_cast<Float64>(std::bit_cast<UInt64>(nan) ^ 1);
    const WindowCases cases{{{0, 1}, {100, 101}}, {{0, 2}, {100, 200, 101, 201}}};
    for (const auto & [window, expected] : cases)
    {
        SCOPED_TRACE(::testing::Message() << "window=[" << window.offset << ", " << window.end << ")");
        std::vector<Chunks> sources(2);
        sources[0].push_back(makeChunk(*header, {-0., nan}, 100));
        sources[1].push_back(makeChunk(*header, {0., other_nan}, 200));
        EXPECT_EQ(mergePayloads(header, std::move(sources), 1, window), expected);
    }
}

TEST(GroupLimitSortedAlgorithm, EmptyInputsAndChunks)
{
    const auto header = makeHeader();
    const WindowCases cases{{{0, 1}, {100, 102}}, {{0, 2}, {100, 101, 102}}};
    for (const auto & [window, expected] : cases)
    for (const bool dynamic_inputs : {false, true})
    {
        SCOPED_TRACE(::testing::Message() << "window=[" << window.offset << ", " << window.end << "), dynamic_inputs=" << dynamic_inputs);
        EXPECT_TRUE(mergePayloads(header, {}, 2, window, dynamic_inputs).empty());
        EXPECT_TRUE(mergePayloads(header, std::vector<Chunks>(2), 2, window, dynamic_inputs).empty());
        std::vector<Chunks> sources(2);
        sources[0].push_back(makeChunk(*header, {}, 0));
        sources[0].push_back(makeChunk(*header, {1u}, 100));
        sources[0].push_back(makeChunk(*header, {}, 0));
        sources[0].push_back(makeChunk(*header, {1u, 2u}, 101));
        sources[0].push_back(makeChunk(*header, {}, 0));
        EXPECT_EQ(mergePayloads(header, std::move(sources), 2, window, dynamic_inputs), expected);
    }
}

TEST(GroupLimitSortedAlgorithm, CompositeKeysAndDescendingOrder)
{
    const auto header = makeHeader();
    SortDescription description;
    description.emplace_back("key", 1, 1);
    description.emplace_back("payload", -1, -1);
    const WindowCases cases{{{0, 1}, {200, 100, 201, 101}}, {{0, 2}, {200, 100, 100, 201, 101, 101}}};
    for (const auto & [window, expected] : cases)
    {
        SCOPED_TRACE(::testing::Message() << "window=[" << window.offset << ", " << window.end << ")");
        std::vector<Chunks> sources(3);
        sources[0].push_back(makeChunk(*header, {1u, 2u}, 100));
        sources[1].push_back(makeChunk(*header, {1u, 2u}, 200));
        sources[2].push_back(makeChunk(*header, {1u, 2u}, 100));
        EXPECT_EQ(mergePayloads(header, std::move(sources), 2, window, /*dynamic_inputs=*/ false, description), expected);
    }
}

TEST(GroupLimitSortedAlgorithm, ConstantSparseAndReplicatedColumns)
{
    const auto header = makeHeader();
    const WindowCases cases{{{0, 1}, {100, 200, 201, 301}}, {{0, 2}, {100, 300, 200, 201, 301}}};
    for (const auto & [window, expected] : cases)
    {
        SCOPED_TRACE(::testing::Message() << "window=[" << window.offset << ", " << window.end << ")");
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
        EXPECT_EQ(mergePayloads(header, std::move(sources), 2, window), expected);
    }
}

TEST(GroupLimitSortedAlgorithm, RowsOfAKeyInConsecutiveChunksOfOneInput)
{
    /// A sort that keeps the first rows of each key merges its runs into chunks of one row, so the rows of a
    /// key reach the group merge in consecutive chunks of one input.
    const auto header = makeHeader();
    Block output_header = *header;
    output_header.erase("seen_rows");
    for (const auto & [group_limit, expected] : std::vector<std::pair<size_t, std::vector<UInt64>>>{
        {1, {100, 201, 101}}, {2, {100, 200, 201, 101, 301}}})
    {
        SCOPED_TRACE(group_limit);
        Chunks chunks;
        chunks.push_back(makeChunk(*header, {1u, 3u}, 100));
        chunks.push_back(makeChunk(*header, {1u, 2u}, 200));
        chunks.push_back(makeChunk(*header, {1u, 3u}, 300));
        chunks.push_back(makeChunk(*header, {1u, 3u}, 400));
        auto sort = std::make_shared<MergeSorterSource>(
            header, std::move(chunks), keyDescription(), 1, 0, MergeSorter::Mode::LimitGroupRows,
            /*preferred_block_bytes=*/ 0, group_limit);
        auto merge = std::make_shared<GroupLimitSortedTransform>(
            "GroupLimitSortedTransform", SharedHeaders{header}, std::make_shared<const Block>(output_header), keyDescription(),
            NameAndTypePair{"seen_rows", header->getByName("seen_rows").type}, /*group_offset=*/ 0, group_limit, 2);
        EXPECT_EQ(pullPayloads({std::move(sort)}, std::move(merge), 2), expected);
    }
}

TEST(GroupLimitSortedAlgorithm, OutputColumnsWithDifferentSeenRowsPositions)
{
    const WindowCases cases{
        {{0, 1}, {101, 102, 103, 104, 106, 201, 108, 109}},
        {{0, 2}, {100, 101, 102, 103, 104, 105, 106, 107, 201, 108, 109}}};
    for (const size_t seen_rows_pos : {0, 1, 2})
    for (const auto & [window, expected] : cases)
    for (const size_t block_size : {2, 64})
    {
        SCOPED_TRACE(::testing::Message() << "seen_rows_pos=" << seen_rows_pos << ", window=[" << window.offset << ", "
            << window.end << "), block_size=" << block_size);
        Block block = *makeHeader();
        const auto seen_rows = block.getByName("seen_rows");
        block.erase("seen_rows");
        block.insert(seen_rows_pos, seen_rows);
        const auto header = std::make_shared<const Block>(std::move(block));
        std::vector<Chunks> sources(3);
        sources[0].push_back(makeChunk(*header, {1u, 2u, 3u, 4u, 5u}, 100));
        sources[0].push_back(makeChunk(*header, {5u, 6u, 7u}, 105));
        sources[0].push_back(makeChunk(*header, {20u, 21u}, 108));
        sources[1].push_back(makeChunk(*header, {5u, 8u}, 200));
        sources[2].push_back(makeChunk(*header, {1u, 7u}, 0, 1));
        EXPECT_EQ(mergePayloads(header, std::move(sources), block_size, window), expected);
    }
}

TEST(GroupLimitSortedAlgorithm, KeyOnlySeenRowsAndDifferentInputLayouts)
{
    /// Seen inputs need only the keys and the counts, and every input orders its columns its own way.
    const auto input_header = makeHeader();
    const auto seen_header = std::make_shared<const Block>(Block{
        input_header->getByName("seen_rows"), input_header->getByName("key")});
    const auto reordered_header = std::make_shared<const Block>(Block{
        input_header->getByName("payload"), input_header->getByName("seen_rows"), input_header->getByName("key")});
    auto output = *input_header;
    output.erase("seen_rows");
    const auto output_header = std::make_shared<const Block>(output);
    const NameAndTypePair seen_rows{"seen_rows", input_header->getByName("seen_rows").type};
    const WindowCases cases{
        {{0, 1}, {101, 103, 201, 202, 203, 204}},
        {{0, 2}, {100, 101, 200, 102, 103, 104, 201, 202, 203, 105, 204}}};
    for (const auto & [window, expected] : cases)
    for (const size_t block_size : {1, 2, 64})
    for (const bool dynamic_inputs : {false, true})
    {
        SCOPED_TRACE(::testing::Message() << "window=[" << window.offset << ", " << window.end << "), block_size="
            << block_size << ", dynamic_inputs=" << dynamic_inputs);
        std::vector<Chunks> inputs(3);
        inputs[0].push_back(makeChunk(*input_header, {1u, 2u, 3u, 4u}, 100));
        inputs[0].push_back(makeChunk(*input_header, {4u, 8u}, 104));
        auto keys = ColumnUInt64::create();
        keys->insertValue(1);
        keys->insertValue(3);
        inputs[1].emplace_back(Columns{ColumnUInt8::create(2, UInt8{1}), std::move(keys)}, 2);
        inputs[1].emplace_back(Columns{ColumnUInt8::create(1, UInt8{1}), ColumnUInt64::create(1, 8)}, 1);
        inputs[2].push_back(makeChunk(*reordered_header, {2u, 5u, 6u}, 200));
        inputs[2].push_back(makeChunk(*reordered_header, {7u, 9u}, 203));
        EXPECT_EQ(mergePayloads({input_header, seen_header, reordered_header}, seen_rows, output_header,
            std::move(inputs), block_size, window, dynamic_inputs, keyDescription()), expected);
    }
}

TEST(GroupLimitSortedAlgorithm, RepeatedOutputColumns)
{
    /// The output projection is shared by both merge paths, so the window of `DISTINCT` checks it.
    const auto header = makeHeader();
    const auto output_header = std::make_shared<const Block>(Block{header->getByName("payload"), header->getByName("payload")});
    for (const size_t block_size : {2, 64})
    {
        SCOPED_TRACE(block_size);
        GroupLimitSortedAlgorithm algorithm(
            {header}, output_header, keyDescription(), NameAndTypePair{"seen_rows", header->getByName("seen_rows").type},
            /*group_offset_=*/ 0, /*group_limit_end_=*/ 1, block_size);
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
