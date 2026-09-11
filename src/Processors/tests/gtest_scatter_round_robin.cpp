#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/Runtime/PipelineExecutor.h>
#include <Processors/ISink.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/ScatterByPartitionTransform.h>

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
