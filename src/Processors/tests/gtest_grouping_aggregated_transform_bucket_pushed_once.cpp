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

/// One partially aggregated two level chunk of `bucket`, reporting `ooo` as the buckets which are
/// delayed by its producer and can still arrive after it.
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

/// Drives `GroupingAggregatedTransform` by hand: sends a chunk to a chosen input and runs `prepare`
/// until it needs something again. Sending one chunk at a time is what makes the order reproducible:
/// `prepare` returns `NeedData` in the same round it reads a chunk, so it pushes in the next one, and
/// a `step` with nothing sent is exactly the moment when the transform pushes.
struct Driver
{
    std::shared_ptr<GroupingAggregatedTransform> transform;
    /// `prepare` and `work` are protected, drive them through the public interface of the base class.
    IProcessor * proc = nullptr;
    std::vector<std::unique_ptr<OutputPort>> feeders;
    std::unique_ptr<InputPort> consumer;
    IProcessor::UpdatedInputPorts all_inputs;
    std::vector<Int32> pushed;
    IProcessor::Status last_status = IProcessor::Status::NeedData;

    explicit Driver(size_t num_inputs)
    {
        auto header = oneColumnHeader();
        transform = std::make_shared<GroupingAggregatedTransform>(*header, num_inputs, makeMergeParams(header));
        proc = transform.get();

        auto in_it = transform->getInputs().begin();
        for (size_t i = 0; i < num_inputs; ++i, ++in_it)
        {
            feeders.emplace_back(std::make_unique<OutputPort>(header));
            connect(*feeders.back(), *in_it);
        }

        consumer = std::make_unique<InputPort>(transform->getOutputs().front().getSharedHeader());
        connect(transform->getOutputs().front(), *consumer);
        consumer->setNeeded();

        for (auto & in : transform->getInputs())
            all_inputs.push_back(&in);
    }

    void step()
    {
        for (int guard = 0; guard < 1000; ++guard)
        {
            consumer->setNeeded();
            IProcessor::UpdatedOutputPorts updated_outputs{&transform->getOutputs().front()};
            last_status = proc->prepare(all_inputs, updated_outputs);

            while (consumer->hasData())
            {
                consumer->setNeeded();
                auto info = consumer->pull().getChunkInfos().get<ChunksToMerge>();
                pushed.push_back(info->bucket_num);
            }

            if (last_status == IProcessor::Status::Ready)
            {
                proc->work();
                continue;
            }
            return;
        }
    }

    void send(size_t input, Int32 bucket, std::vector<Int32> ooo = {})
    {
        feeders[input]->push(makeBucketChunk(bucket, std::move(ooo)));
        step();
    }

    void finish()
    {
        for (auto & feeder : feeders)
            feeder->finish();
        for (int i = 0; i < 20; ++i)
            step();
    }

    std::map<Int32, int> counts() const
    {
        std::map<Int32, int> result;
        for (auto bucket : pushed)
            result[bucket]++;
        return result;
    }
};

}

/// An input can send several chunks of the same bucket, the producer converts the aggregation variants
/// of a bucket one by one. A bucket must not be pushed while an input is still at it: the chunks of it
/// which arrive after the push are merged and pushed a second time, and the keys of the bucket are
/// returned twice.
TEST(GroupingAggregatedOOO, BucketNotPushedWhileAnInputIsStillAtIt)
{
    Driver d(2);

    /// Both inputs postpone bucket 1 and send bucket 3, so bucket 1 is delayed and not pushed in order.
    d.send(0, 3, {1});
    d.send(1, 3, {1});
    /// Input 0 resolves bucket 1 and is done.
    d.send(0, 1);
    d.feeders[0]->finish();
    d.step();
    /// Input 1 resolves bucket 1 as well: nothing delays the bucket any more and every input is AT it,
    /// but input 1 still has the rest of the same bucket to send.
    d.send(1, 1);
    /// Nothing is sent for a moment: this is when the transform pushes the bucket.
    d.step();
    /// The rest of bucket 1 from input 1.
    d.send(1, 1);
    d.send(1, 4);
    d.finish();

    for (auto [bucket, count] : d.counts())
        EXPECT_EQ(count, 1) << "bucket " << bucket << " is pushed " << count << " times, it is merged and "
                            << "returned twice, which duplicates its keys in the result";
}

/// The highest bucket can be delayed as well, and then no input ever reads a bucket after it, so it is
/// not pushed as soon as it is resolved. It still has to be pushed, once, when the inputs finish.
TEST(GroupingAggregatedOOO, HighestBucketDelayedIsStillPushedOnce)
{
    Driver d(2);

    d.send(0, 0, {255});
    d.send(1, 0, {255});
    d.send(0, 255);
    d.send(1, 255);
    d.step();
    d.finish();

    auto counts = d.counts();
    EXPECT_EQ(counts[0], 1) << "bucket 0 is pushed " << counts[0] << " times";
    EXPECT_EQ(counts[255], 1) << "the highest bucket is pushed " << counts[255] << " times, it has to be pushed once";
    EXPECT_EQ(d.last_status, IProcessor::Status::Finished) << "the transform is stuck on the delayed bucket";
}

/// Only the highest bucket waits for the inputs to finish. A delayed bucket below it is pushed as soon
/// as every input has read a bucket after it, without waiting for the end of the query.
TEST(GroupingAggregatedOOO, DelayedBucketBelowTheHighestIsPushedBeforeTheEnd)
{
    Driver d(2);

    d.send(0, 0, {254});
    d.send(1, 0, {254});
    d.send(0, 254);
    d.send(1, 254);
    d.send(0, 255);
    d.send(1, 255);
    d.step();

    /// The inputs are still open here.
    EXPECT_EQ(d.counts()[254], 1) << "bucket 254 is not pushed while the inputs are open, a delayed bucket "
                                  << "has to be pushed as soon as every input has read a bucket after it";

    d.finish();
    for (auto [bucket, count] : d.counts())
        EXPECT_EQ(count, 1) << "bucket " << bucket << " is pushed " << count << " times";
}
