#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/DistributedTopKCandidateGateTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

using namespace DB;

namespace
{

SharedHeader makeHeader()
{
    return std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "key"),
        ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "payload"),
    });
}

Chunk makeChunk(std::initializer_list<UInt64> values)
{
    auto keys = ColumnUInt64::create();
    auto payload = ColumnUInt64::create();
    for (UInt64 value : values)
    {
        keys->insertValue(value);
        payload->insertValue(value + 100);
    }
    return Chunk(Columns{std::move(keys), std::move(payload)}, values.size());
}

Chunks makeChunks(std::initializer_list<std::initializer_list<UInt64>> chunk_values)
{
    Chunks chunks;
    chunks.reserve(chunk_values.size());
    for (const auto & values : chunk_values)
        chunks.push_back(makeChunk(values));
    return chunks;
}

SortDescription makeSortDescription()
{
    SortDescription result;
    result.emplace_back("key");
    return result;
}

std::vector<UInt64> executeGate(Chunks chunks, QueryCoordinationCallback callback, UInt64 limit = 5)
{
    auto header = makeHeader();
    auto source = std::make_shared<SourceFromChunks>(header, std::move(chunks));
    Pipe pipe(source);
    pipe.addTransform(std::make_shared<DistributedTopKCandidateGateTransform>(header, limit, makeSortDescription(), std::move(callback)));

    QueryPipeline pipeline(std::move(pipe));
    PullingPipelineExecutor executor(pipeline);

    std::vector<UInt64> result;
    Chunk output;
    while (executor.pull(output))
    {
        const auto & payload = assert_cast<const ColumnUInt64 &>(*output.getColumns().at(1));
        for (size_t row = 0; row < output.getNumRows(); ++row)
            result.push_back(payload.getElement(row));
    }
    return result;
}

}

TEST(DistributedTopKCandidateGateTransform, SelectsOrdinalsAcrossOriginalChunks)
{
    size_t calls = 0;
    auto result = executeGate(
        makeChunks({{1, 2}, {3, 4, 5}}),
        [&](QueryCoordinationRequest request)
        {
            ++calls;
            EXPECT_EQ(request.kind, QueryCoordinationRequestKind::DistributedTopKCandidates);
            EXPECT_EQ(request.mode, QueryCoordinationRequestMode::Candidates);
            EXPECT_EQ(request.payload.columns(), 1u);
            EXPECT_EQ(request.payload.rows(), 5u);
            EXPECT_EQ(request.payload.getByPosition(0).name, "key");
            return QueryCoordinationResponse{
                .mode = QueryCoordinationResponseMode::Selected,
                .selected_ordinals = {1, 4},
            };
        });

    EXPECT_EQ(calls, 1u);
    EXPECT_EQ(result, (std::vector<UInt64>{102, 105}));
}

TEST(DistributedTopKCandidateGateTransform, FallsBackToAllRows)
{
    auto result = executeGate(
        makeChunks({{1, 2}, {3}}),
        [](QueryCoordinationRequest request)
        {
            EXPECT_EQ(request.mode, QueryCoordinationRequestMode::Candidates);
            EXPECT_EQ(request.payload.rows(), 3u);
            EXPECT_EQ(request.payload.columns(), 1u);
            return QueryCoordinationResponse{
                .mode = QueryCoordinationResponseMode::FallbackAll,
                .selected_ordinals = {}};
        });

    EXPECT_EQ(result, (std::vector<UInt64>{101, 102, 103}));
}

TEST(DistributedTopKCandidateGateTransform, AnnouncesEmptyInput)
{
    size_t calls = 0;
    auto result = executeGate(
        {},
        [&](QueryCoordinationRequest request)
        {
            ++calls;
            EXPECT_EQ(request.payload.rows(), 0u);
            return QueryCoordinationResponse{
                .mode = QueryCoordinationResponseMode::Selected,
                .selected_ordinals = {}};
        });

    EXPECT_EQ(calls, 1u);
    EXPECT_TRUE(result.empty());
}

TEST(DistributedTopKCandidateGateTransform, RejectsInvalidOrdinals)
{
    EXPECT_THROW(
        executeGate(
            makeChunks({{1, 2}}),
            [](QueryCoordinationRequest)
            {
                return QueryCoordinationResponse{
                    .mode = QueryCoordinationResponseMode::Selected,
                    .selected_ordinals = {0, 0},
                };
            }),
        Exception);

    EXPECT_THROW(
        executeGate(
            makeChunks({{1, 2}}),
            [](QueryCoordinationRequest)
            {
                return QueryCoordinationResponse{
                    .mode = QueryCoordinationResponseMode::Selected,
                    .selected_ordinals = {2},
                };
            }),
        Exception);

    EXPECT_THROW(
        executeGate(
            makeChunks({{1, 2}}),
            [](QueryCoordinationRequest)
            {
                return QueryCoordinationResponse{
                    .mode = QueryCoordinationResponseMode::FallbackAll,
                    .selected_ordinals = {0},
                };
            }),
        Exception);
}

TEST(DistributedTopKCandidateGateTransform, RejectsRowsBeyondMarkedLimit)
{
    size_t calls = 0;
    EXPECT_THROW(
        executeGate(
            makeChunks({{1, 2}, {3}}),
            [&](QueryCoordinationRequest)
            {
                ++calls;
                return QueryCoordinationResponse{
                    .mode = QueryCoordinationResponseMode::FallbackAll,
                    .selected_ordinals = {}};
            },
            2),
        Exception);
    EXPECT_EQ(calls, 0u);
}
