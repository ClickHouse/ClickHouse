#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/Runtime/PipelineExecutor.h>
#include <Processors/ISink.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/ScatterByPartitionTransform.h>
#include <Common/assert_cast.h>

#include <algorithm>
#include <numeric>

using namespace DB;

namespace
{

struct TestChunkInfo : public ChunkInfoCloneable<TestChunkInfo>
{
    TestChunkInfo() = default;
    TestChunkInfo(const TestChunkInfo &) = default;
};

SharedHeader makeHeader()
{
    return std::make_shared<const Block>(
        Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "x")});
}

Chunk makeChunk(size_t rows)
{
    auto column = ColumnUInt64::create();
    for (size_t i = 0; i < rows; ++i)
        column->insertValue(i);
    Columns columns;
    columns.emplace_back(std::move(column));
    return Chunk(std::move(columns), rows);
}

class CollectingSink : public ISink
{
public:
    explicit CollectingSink(SharedHeader header) : ISink(std::move(header)) {}
    String getName() const override { return "CollectingSink"; }
    Chunks collected;

protected:
    void consume(Chunk chunk) override { collected.push_back(std::move(chunk)); }
};

Chunk makeSingleValueChunk(UInt64 value)
{
    auto column = ColumnUInt64::create();
    column->insertValue(value);
    Columns columns;
    columns.emplace_back(std::move(column));
    return Chunk(std::move(columns), 1);
}

std::vector<UInt64> collectedValues(const Chunks & chunks)
{
    std::vector<UInt64> values;
    values.reserve(chunks.size());
    for (const auto & chunk : chunks)
        values.push_back(assert_cast<const ColumnUInt64 &>(*chunk.getColumns().front()).getElement(0));
    return values;
}

/// Takes a single chunk and closes its input, like a satisfied LIMIT downstream.
class OneChunkSink final : public IProcessor
{
public:
    explicit OneChunkSink(SharedHeader header_) : IProcessor({header_}, {}) { }

    String getName() const override { return "OneChunkSink"; }

    Status prepare() override
    {
        auto & input = inputs.front();

        if (!collected.empty() || input.isFinished())
        {
            input.close();
            return Status::Finished;
        }

        input.setNeeded();
        if (!input.hasData())
            return Status::NeedData;

        collected.push_back(input.pull());
        input.close();
        return Status::Finished;
    }

    Chunks collected;
};

void runSingleThreaded(const Processors & processors)
{
    auto shared_processors = std::make_shared<Processors>(processors);
    QueryStatusPtr status;
    PipelineExecutor executor(shared_processors, status);
    executor.execute(1, false);
}

}

/// Round-robin scatter must spread whole chunks across all outputs, starting at the given
/// bucket, and keep each chunk's ChunkInfo (the exchange sinks serialize it).
TEST(ScatterRoundRobin, SpreadsChunksAndKeepsChunkInfo)
{
    constexpr size_t bucket_count = 3;
    constexpr size_t start_bucket = 2;
    auto header = makeHeader();

    Chunks input;
    for (size_t rows = 1; rows <= 5; ++rows)
        input.push_back(makeChunk(rows));
    input[0].getChunkInfos().add(std::make_shared<TestChunkInfo>());

    auto source = std::make_shared<SourceFromChunks>(header, std::move(input));
    auto scatter = ScatterByPartitionTransform::createRoundRobin(header, bucket_count, start_bucket);
    connect(source->getPort(), scatter->getInputs().front());

    auto processors = std::make_shared<Processors>();
    processors->push_back(source);
    processors->push_back(scatter);

    std::vector<std::shared_ptr<CollectingSink>> sinks;
    for (auto & output : scatter->getOutputs())
    {
        auto sink = std::make_shared<CollectingSink>(header);
        connect(output, sink->getPort());
        sinks.push_back(sink);
        processors->push_back(sink);
    }

    QueryStatusPtr status;
    PipelineExecutor executor(processors, status);
    executor.execute(1, false);

    /// Chunk i (1-based rows count i) goes to bucket (start_bucket + i - 1) % bucket_count:
    /// bucket 0 gets chunks 2 and 5, bucket 1 gets chunk 3, bucket 2 gets chunks 1 and 4.
    ASSERT_EQ(sinks[0]->collected.size(), 2u);
    EXPECT_EQ(sinks[0]->collected[0].getNumRows(), 2u);
    EXPECT_EQ(sinks[0]->collected[1].getNumRows(), 5u);
    ASSERT_EQ(sinks[1]->collected.size(), 1u);
    EXPECT_EQ(sinks[1]->collected[0].getNumRows(), 3u);
    ASSERT_EQ(sinks[2]->collected.size(), 2u);
    EXPECT_EQ(sinks[2]->collected[0].getNumRows(), 1u);
    EXPECT_EQ(sinks[2]->collected[1].getNumRows(), 4u);

    /// The first chunk carried a ChunkInfo; it landed in bucket 2 and must keep it.
    EXPECT_FALSE(sinks[2]->collected[0].getChunkInfos().empty());
}

/// A finished output never becomes pushable again, so the transform must drop its chunk and keep
/// serving the others instead of waiting forever.
TEST(ScatterRoundRobin, FinishedOutputDoesNotWedgeThePipeline)
{
    constexpr size_t bucket_count = 3;
    constexpr size_t total_chunks = 12;
    auto header = makeHeader();

    Chunks input;
    for (size_t i = 0; i < total_chunks; ++i)
        input.push_back(makeSingleValueChunk(i));

    auto source = std::make_shared<SourceFromChunks>(header, std::move(input));
    auto scatter = ScatterByPartitionTransform::createRoundRobin(header, bucket_count, /*start_bucket=*/0);
    connect(source->getPort(), scatter->getInputs().front());

    Processors processors{source, scatter};

    auto early_finisher = std::make_shared<OneChunkSink>(header);
    std::vector<std::shared_ptr<CollectingSink>> sinks;
    size_t bucket = 0;
    for (auto & output : scatter->getOutputs())
    {
        if (bucket++ == 1)
        {
            connect(output, early_finisher->getInputs().front());
            processors.push_back(early_finisher);
            continue;
        }

        auto sink = std::make_shared<CollectingSink>(header);
        connect(output, sink->getPort());
        sinks.push_back(sink);
        processors.push_back(sink);
    }

    runSingleThreaded(processors);

    EXPECT_EQ(collectedValues(early_finisher->collected), (std::vector<UInt64>{1}));
    EXPECT_EQ(collectedValues(sinks[0]->collected), (std::vector<UInt64>{0, 3, 6, 9}));
    EXPECT_EQ(collectedValues(sinks[1]->collected), (std::vector<UInt64>{2, 5, 8, 11}));
}

/// Hash mode scatters rows into per-output chunks that live until they are handed over, so a chunk
/// that could not be pushed must hold the transform at `PortFull`: consuming the next input chunk
/// would scatter on top of the pending one.
TEST(ScatterByPartition, DoesNotPullAnotherChunkWhileOneIsPending)
{
    constexpr size_t bucket_count = 2;
    constexpr size_t rows = 25;
    auto header = makeHeader();

    auto scatter = std::make_shared<ScatterByPartitionTransform>(header, bucket_count, ColumnNumbers{0});

    OutputPort upstream(header);
    connect(upstream, scatter->getInputs().front());

    InputPort accepting(header);
    InputPort refusing(header);
    auto output_it = scatter->getOutputs().begin();
    connect(*output_it++, accepting);
    connect(*output_it, refusing);

    /// `refusing` never asks for data, so the scatter cannot hand over its bucket.
    accepting.setNeeded();

    ASSERT_EQ(scatter->prepare(), IProcessor::Status::NeedData);
    upstream.push(makeChunk(rows));
    ASSERT_EQ(scatter->prepare(), IProcessor::Status::Ready);
    scatter->work();

    EXPECT_EQ(scatter->prepare(), IProcessor::Status::PortFull);
    ASSERT_TRUE(accepting.hasData());
    ASSERT_FALSE(refusing.hasData());

    /// Once the refusing output accepts, the pending bucket is handed over and input is wanted again.
    refusing.setNeeded();
    EXPECT_EQ(scatter->prepare(), IProcessor::Status::NeedData);
    ASSERT_TRUE(refusing.hasData());

    std::vector<UInt64> routed;
    for (auto * port : {&accepting, &refusing})
    {
        const auto chunk = port->pull();
        const auto & column = assert_cast<const ColumnUInt64 &>(*chunk.getColumns().front());
        for (size_t i = 0; i < chunk.getNumRows(); ++i)
            routed.push_back(column.getElement(i));
    }

    std::sort(routed.begin(), routed.end());
    std::vector<UInt64> expected(rows);
    std::iota(expected.begin(), expected.end(), 0);
    EXPECT_EQ(routed, expected);
}
