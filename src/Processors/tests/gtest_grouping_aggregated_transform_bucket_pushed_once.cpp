#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>

#include <Processors/Chunk.h>
#include <Processors/Port.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>

#include <Interpreters/Aggregator.h>

#include <map>
#include <memory>
#include <vector>

using namespace DB;

namespace
{

SharedHeader oneColumnHeader()
{
    return std::make_shared<const Block>(
        Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "key")});
}

/// One partially-aggregated two-level chunk for `bucket`, carrying the given
/// out_of_order_buckets metadata. It is empty by default, which means that the producer
/// reports no delayed buckets. `MergingAggregatedBucketTransform` copies the metadata
/// stamped by `GroupingAggregatedTransform` now, but it did not before, and the tests
/// below still feed empty metadata on purpose, to check that the consumer coped with a
/// producer which reorders the buckets without reporting them.
Chunk makeBucketChunk(Int32 bucket, std::vector<Int32> ooo = {})
{
    auto col = ColumnUInt64::create();
    col->insertValue(static_cast<UInt64>(bucket));
    Columns columns;
    columns.emplace_back(std::move(col));
    Chunk chunk(std::move(columns), 1);

    auto info = std::make_shared<AggregatedChunkInfo>();
    info->bucket_num = bucket;
    info->is_overflows = false;
    info->out_of_order_buckets = std::move(ooo);
    chunk.getChunkInfos().add(std::move(info));
    return chunk;
}

AggregatingTransformParamsPtr makeMergeParams(const SharedHeader & header)
{
    Aggregator::Params params(
        /*keys_=*/ Names{"key"},
        /*aggregates_=*/ AggregateDescriptions{},
        /*overflow_row_=*/ false,
        /*max_threads_=*/ 1,
        /*max_block_size_=*/ 65536,
        /*min_hit_rate_to_use_consecutive_keys_optimization_=*/ 0.5f,
        /*serialize_string_with_zero_byte_=*/ false,
        /*enable_packed_string_keys_=*/ true);
    return std::make_shared<AggregatingTransformParams>(header, params, /*final=*/ false);
}

/// One chunk pushed by GroupingAggregatedTransform: the bucket and the out_of_order_buckets
/// metadata it stamped (the buckets still delayed by some input when it pushed). The metadata
/// is exactly what a downstream MergingAggregatedBucketTransform copies into its output, so it
/// is what a NEXT distributed layer's GroupingAggregatedTransform receives.
struct PushedBucket
{
    Int32 bucket;
    std::vector<Int32> ooo;
};

PushedBucket chunkPushed(const Chunk & chunk)
{
    auto info = chunk.getChunkInfos().get<ChunksToMerge>();
    chassert(info);
    return PushedBucket{info->bucket_num, info->out_of_order_buckets};
}

}

/// An input can send several chunks of the same bucket: the producer converts the aggregation variants
/// of a bucket one by one. A bucket must not be pushed while an input is still at it, the chunks of it
/// which arrive after the push are merged and pushed a second time, and the keys of the bucket are
/// returned twice. The inputs are fed by hand, the random driver above delivers everything upfront.
TEST(GroupingAggregatedOOO, BucketNotPushedWhileAnInputIsStillAtIt)
{
    auto header = oneColumnHeader();
    auto params = makeMergeParams(header);
    auto transform = std::make_shared<GroupingAggregatedTransform>(*header, 2, params);

    std::vector<std::unique_ptr<OutputPort>> feeders;
    auto in_it = transform->getInputs().begin();
    for (size_t i = 0; i < 2; ++i, ++in_it)
    {
        feeders.emplace_back(std::make_unique<OutputPort>(header));
        connect(*feeders.back(), *in_it);
    }
    auto consumer = std::make_unique<InputPort>(transform->getOutputs().front().getSharedHeader());
    connect(transform->getOutputs().front(), *consumer);
    consumer->setNeeded();

    IProcessor::UpdatedInputPorts all_inputs;
    for (auto & in : transform->getInputs())
        all_inputs.push_back(&in);
    IProcessor & proc = *transform;

    std::vector<Int32> pushed;
    auto step = [&]
    {
        for (int guard = 0; guard < 1000; ++guard)
        {
            consumer->setNeeded();
            IProcessor::UpdatedOutputPorts updated_outputs{&transform->getOutputs().front()};
            auto status = proc.prepare(all_inputs, updated_outputs);

            while (consumer->hasData())
            {
                consumer->setNeeded();
                pushed.push_back(chunkPushed(consumer->pull()).bucket);
            }

            if (status == IProcessor::Status::Ready)
            {
                proc.work();
                continue;
            }
            break;
        }
    };

    /// Both inputs postpone bucket 1 and send bucket 3, so bucket 1 is delayed and not pushed in order.
    feeders[0]->push(makeBucketChunk(3, {1})); step();
    feeders[1]->push(makeBucketChunk(3, {1})); step();
    /// Input 0 resolves bucket 1 and is done.
    feeders[0]->push(makeBucketChunk(1, {})); step();
    feeders[0]->finish(); step();
    /// Input 1 resolves bucket 1 as well: nothing delays the bucket any more and every input is AT it,
    /// but input 1 still has the rest of the same bucket to send.
    feeders[1]->push(makeBucketChunk(1, {})); step();
    /// Nothing to deliver for a moment: this is when the transform pushes the bucket.
    step();
    /// The producer converts the aggregation variants of a bucket one by one, so here comes the rest.
    feeders[1]->push(makeBucketChunk(1, {})); step();
    feeders[1]->push(makeBucketChunk(4, {})); step();
    feeders[1]->finish();
    for (int i = 0; i < 20; ++i)
        step();

    std::map<Int32, int> counts;
    for (auto bucket : pushed)
        counts[bucket]++;
    for (auto [bucket, count] : counts)
        EXPECT_EQ(count, 1) << "bucket " << bucket << " pushed " << count << " times - it is merged and "
                            << "returned twice, which duplicates its keys in the result";
}
