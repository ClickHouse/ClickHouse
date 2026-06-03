#include <gtest/gtest.h>

#include <Processors/Transforms/BufferedShardByHashTransform.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/ISink.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Interpreters/JoinUtils.h>

#include <Columns/ColumnsNumber.h>
#include <Common/WeakHash.h>
#include <DataTypes/DataTypesNumber.h>

using namespace DB;

namespace
{

/// Sink that pulls every chunk and counts the rows it received. Unlike `NullSink` it does not
/// close its input on the first `prepare`, so the upstream `ConcatProcessor` is driven to
/// consume its inputs sequentially - which is what reproduces the stuck-pipeline scenario.
/// Counting the rows lets the test assert that the queued chunks actually drained downstream
/// rather than being silently dropped.
class CountingSink final : public ISink
{
public:
    explicit CountingSink(SharedHeader header_) : ISink(std::move(header_)) {}
    String getName() const override { return "CountingSink"; }

    size_t total_rows = 0;

protected:
    void consume(Chunk chunk) override { total_rows += chunk.getNumRows(); }
};

SharedHeader makeHeader()
{
    return std::make_shared<Block>(Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "k")});
}

/// Replicate the transform's routing so the test can place data on a chosen shard.
size_t shardOf(UInt64 key, size_t num_shards)
{
    auto col = ColumnUInt64::create();
    col->getData().push_back(key);
    WeakHash32 hash(1);
    hash.update(col->getWeakHash32());
    auto selector = JoinCommon::hashToSelector(hash, [n = num_shards](size_t h) { return ((h & 0xFFFFFFFF) * n) >> 32; });
    return selector[0];
}

/// Smallest non-negative key that routes to `target_shard`.
UInt64 keyForShard(size_t target_shard, size_t num_shards)
{
    for (UInt64 k = 0; k < 100000; ++k)
        if (shardOf(k, num_shards) == target_shard)
            return k;
    ADD_FAILURE() << "no key routes to shard " << target_shard;
    return 0;
}

/// One chunk of `num_rows` with a single constant key, so every row hashes to one shard.
Chunk makeSkewedChunk(size_t num_rows, UInt64 key)
{
    auto col = ColumnUInt64::create();
    col->getData().resize_fill(num_rows, key);
    Columns columns;
    columns.emplace_back(std::move(col));
    return Chunk(std::move(columns), num_rows);
}

}

/// Every row hashes to the last shard, so the first shard's output queue stays empty while
/// the last shard's queue fills (the `ConcatProcessor` is stuck on its first, empty input and
/// never pulls the last input). The transform must finish the empty first output once the
/// input is exhausted, otherwise `Concat` waits on it forever and the queued data on the last
/// shard never drains (#106237). With the fix the executor drains the pipeline and returns;
/// without it the empty output is never finished and `PipelineExecutor` reports `Pipeline stuck`.
///
/// We push more than `MAX_QUEUE_LENGTH` chunks so the loaded shard's queue hits the soft cap
/// while the sibling port is still empty and asking. This exercises the cap-bypass path
/// (`any_queue_at_capacity && has_pushable_empty_port`) in addition to the empty-output finish
/// path. The counting sink asserts every row drained downstream, proving the queued chunks were
/// pushed through `Concat` rather than dropped.
TEST(BufferedShardByHashTransform, SkewedInputDoesNotStallConcat)
{
    constexpr size_t num_shards = 2;
    constexpr size_t num_chunks = 12; /// > MAX_QUEUE_LENGTH (10) so the loaded queue overshoots the soft cap
    constexpr size_t rows_per_chunk = 64;

    /// Route everything to the last shard - the one `Concat` consumes last - so the empty
    /// first input is the one that must be finished to let `Concat` advance.
    const UInt64 key = keyForShard(num_shards - 1, num_shards);

    Chunks chunks;
    for (size_t i = 0; i < num_chunks; ++i)
        chunks.emplace_back(makeSkewedChunk(rows_per_chunk, key));

    auto source = std::make_shared<SourceFromChunks>(makeHeader(), std::move(chunks));
    auto transform = std::make_shared<BufferedShardByHashTransform>(makeHeader(), num_shards, ColumnNumbers{0});
    auto concat = std::make_shared<ConcatProcessor>(makeHeader(), num_shards);
    auto sink = std::make_shared<CountingSink>(makeHeader());

    connect(source->getPort(), transform->getInputs().front());

    auto transform_out = transform->getOutputs().begin();
    for (auto & concat_in : concat->getInputs())
    {
        connect(*transform_out, concat_in);
        ++transform_out;
    }

    connect(concat->getOutputPort(), sink->getPort());

    auto * sink_ptr = sink.get();

    auto processors = std::make_shared<Processors>();
    processors->emplace_back(std::move(source));
    processors->emplace_back(std::move(transform));
    processors->emplace_back(std::move(concat));
    processors->emplace_back(std::move(sink));

    QueryStatusPtr element;
    PipelineExecutor executor(processors, element);
    executor.execute(1, false);

    EXPECT_EQ(sink_ptr->total_rows, num_chunks * rows_per_chunk);
}
