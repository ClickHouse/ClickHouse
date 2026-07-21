#include <gtest/gtest.h>

#include <algorithm>
#include <vector>

#include <Common/Exception.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Aggregator.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/ISink.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

using namespace DB;

namespace
{

SharedHeader makeHeader()
{
    return std::make_shared<const Block>(
        Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "x")});
}

Chunk makeBucketChunk(Int32 bucket, std::vector<Int32> announced_delayed_buckets)
{
    auto column = ColumnUInt64::create();
    column->insertValue(UInt64(bucket));
    Columns columns;
    columns.emplace_back(std::move(column));
    Chunk chunk(std::move(columns), 1);

    auto info = std::make_shared<AggregatedChunkInfo>();
    info->bucket_num = bucket;
    info->out_of_order_buckets = std::move(announced_delayed_buckets);
    chunk.getChunkInfos().add(std::move(info));
    return chunk;
}

class BucketOrderCollectingSink : public ISink
{
public:
    explicit BucketOrderCollectingSink(SharedHeader header) : ISink(std::move(header)) {}
    String getName() const override { return "BucketOrderCollectingSink"; }
    std::vector<Int32> bucket_order;

protected:
    void consume(Chunk chunk) override
    {
        auto info = chunk.getChunkInfos().get<ChunksToMerge>();
        ASSERT_TRUE(info);
        bucket_order.push_back(info->bucket_num);
    }
};

AggregatingTransformParamsPtr makeParams(SharedHeader header)
{
    Aggregator::Params params(
        /*keys_=*/ {"x"}, /*aggregates_=*/ {}, /*overflow_row_=*/ false, /*max_threads_=*/ 1,
        /*max_block_size_=*/ 65536, /*min_hit_rate_to_use_consecutive_keys_optimization_=*/ 0.5,
        /*serialize_string_with_zero_byte_=*/ false, /*enable_packed_string_keys_=*/ false);
    return std::make_shared<AggregatingTransformParams>(header, params, /*final_=*/ false);
}

std::vector<Int32> runGroupingAggregated(bool produce_buckets_in_order, std::vector<Chunks> per_input_chunks)
{
    auto header = makeHeader();
    const size_t num_inputs = per_input_chunks.size();

    auto transform = std::make_shared<GroupingAggregatedTransform>(*header, num_inputs, makeParams(header), produce_buckets_in_order);
    auto sink = std::make_shared<BucketOrderCollectingSink>(std::make_shared<const Block>(Block{}));

    Processors processors;
    auto input_port = transform->getInputs().begin();
    for (auto & chunks : per_input_chunks)
    {
        auto source = std::make_shared<SourceFromChunks>(header, std::move(chunks));
        connect(source->getPort(), *input_port++);
        processors.push_back(std::move(source));
    }
    connect(transform->getOutputs().front(), sink->getPort());
    processors.push_back(transform);
    processors.push_back(sink);

    auto processors_ptr = std::make_shared<Processors>(std::move(processors));
    QueryStatusPtr status;
    PipelineExecutor executor(processors_ptr, status);
    executor.execute(1, false);

    return sink->bucket_order;
}

/// One input delivering buckets 12, 14, 15, 16 with bucket 13 announced as delayed, then 13.
Chunks makeDelayedBucketInput()
{
    Chunks input;
    input.push_back(makeBucketChunk(12, {}));
    input.push_back(makeBucketChunk(14, {13}));
    input.push_back(makeBucketChunk(15, {13}));
    input.push_back(makeBucketChunk(16, {13}));
    input.push_back(makeBucketChunk(13, {}));
    return input;
}

}

/// Without the in-order requirement a delayed bucket is emitted as soon as it is resolved, i.e.
/// after a bucket with a higher number.
TEST(GroupingAggregatedBucketOrder, DelayedBucketEmittedLateInRelaxedMode)
{
    std::vector<Chunks> inputs;
    inputs.push_back(makeDelayedBucketInput());
    auto order = runGroupingAggregated(/*produce_buckets_in_order=*/ false, std::move(inputs));
    ASSERT_EQ(order.size(), 5u);
    ASSERT_FALSE(std::ranges::is_sorted(order));
}

/// With the in-order requirement the transform must wait for the delayed bucket: its output
/// carries no delay announcements, so a downstream `GroupingAggregatedTransform` would reject
/// an out-of-order bucket.
TEST(GroupingAggregatedBucketOrder, DelayedBucketWaitedForInOrderMode)
{
    std::vector<Chunks> inputs;
    inputs.push_back(makeDelayedBucketInput());
    auto order = runGroupingAggregated(/*produce_buckets_in_order=*/ true, std::move(inputs));
    ASSERT_EQ(order, (std::vector<Int32>{12, 13, 14, 15, 16}));
}

/// An announced bucket that turns out empty is cleared by the announcing input's next chunk;
/// the in-order mode must emit the other input's copy of it in place, not wait forever.
TEST(GroupingAggregatedBucketOrder, EmptyDelayedBucketDoesNotBlockInOrderMode)
{
    Chunks announcing;
    announcing.push_back(makeBucketChunk(12, {}));
    announcing.push_back(makeBucketChunk(14, {13}));
    announcing.push_back(makeBucketChunk(15, {}));

    Chunks plain;
    plain.push_back(makeBucketChunk(12, {}));
    plain.push_back(makeBucketChunk(13, {}));
    plain.push_back(makeBucketChunk(14, {}));
    plain.push_back(makeBucketChunk(15, {}));

    std::vector<Chunks> inputs;
    inputs.push_back(std::move(announcing));
    inputs.push_back(std::move(plain));
    auto order = runGroupingAggregated(/*produce_buckets_in_order=*/ true, std::move(inputs));
    ASSERT_EQ(order, (std::vector<Int32>{12, 13, 14, 15}));
}

/// An input that finishes with a pending announcement can no longer deliver the bucket; the
/// in-order mode must release the announcement instead of waiting for that input.
TEST(GroupingAggregatedBucketOrder, FinishedInputReleasesItsAnnouncements)
{
    Chunks announcing;
    announcing.push_back(makeBucketChunk(12, {}));
    announcing.push_back(makeBucketChunk(14, {13}));

    Chunks plain;
    plain.push_back(makeBucketChunk(12, {}));
    plain.push_back(makeBucketChunk(13, {}));
    plain.push_back(makeBucketChunk(14, {}));

    std::vector<Chunks> inputs;
    inputs.push_back(std::move(announcing));
    inputs.push_back(std::move(plain));
    auto order = runGroupingAggregated(/*produce_buckets_in_order=*/ true, std::move(inputs));
    ASSERT_EQ(order, (std::vector<Int32>{12, 13, 14}));
}

/// A late bucket no previous chunk of the same input announced means part of an already emitted
/// bucket would be emitted again; the transform must report it instead of duplicating groups.
/// The check throws `LOGICAL_ERROR`, which aborts debug and sanitizer builds, so the test runs
/// only in release builds.
#if !defined(DEBUG_OR_SANITIZER_BUILD)
TEST(GroupingAggregatedBucketOrder, UnannouncedLateBucketThrows)
{
    Chunks input;
    input.push_back(makeBucketChunk(14, {}));
    input.push_back(makeBucketChunk(13, {}));

    std::vector<Chunks> inputs;
    inputs.push_back(std::move(input));
    ASSERT_THROW(runGroupingAggregated(/*produce_buckets_in_order=*/ false, std::move(inputs)), Exception);
}
#endif
