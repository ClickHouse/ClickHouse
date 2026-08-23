#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>

#include <Processors/Chunk.h>
#include <Processors/Port.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>

#include <Interpreters/Aggregator.h>

#include <pcg_random.hpp>

#include <algorithm>

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
    std::vector<InputPort *> in_ports;
    std::unique_ptr<InputPort> consumer;
    /// The executor reports only the ports whose edge has really changed, see `ExecutingGraph::updateNode`.
    /// Reporting all of them would let the transform observe states which never happen in a real pipeline.
    std::vector<size_t> updated_inputs;
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
            in_ports.push_back(&in);
    }

    void step()
    {
        for (int guard = 0; guard < 1000; ++guard)
        {
            consumer->setNeeded();
            IProcessor::UpdatedOutputPorts updated_outputs{&transform->getOutputs().front()};
            IProcessor::UpdatedInputPorts updated_inputs_ports;
            for (auto input : updated_inputs)
                updated_inputs_ports.push_back(in_ports[input]);
            updated_inputs.clear();
            last_status = proc->prepare(updated_inputs_ports, updated_outputs);

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
        updated_inputs.push_back(input);
        step();
    }

    void finishInput(size_t input)
    {
        feeders[input]->finish();
        updated_inputs.push_back(input);
        step();
    }

    void finish()
    {
        for (size_t input = 0; input < feeders.size(); ++input)
        {
            if (!feeders[input]->isFinished())
            {
                feeders[input]->finish();
                updated_inputs.push_back(input);
            }
        }
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
/// of a bucket one by one. A bucket must not be pushed while an input can still send more of it: the
/// chunks which arrive after the push are merged and pushed a second time, and the keys of the bucket
/// are returned twice.
///
/// The producer here postponed buckets 6 and 7, and resolves them in order, so it reports 7 as still
/// delayed when it sends 6. Nothing above bucket 7 is ever sent, which is what keeps `current_bucket`
/// behind it and lets the transform push while an input is at the bucket.
TEST(GroupingAggregatedOOO, BucketNotPushedWhileAnInputCanStillSendIt)
{
    Driver d(2);

    d.send(0, 6, {7});
    d.send(1, 6, {7});
    /// Both resolve bucket 7, so nothing delays it any more and every input is AT it.
    d.send(0, 7);
    d.send(1, 7);
    /// The rest of bucket 7 from input 0.
    d.send(0, 7);
    d.finish();

    for (auto [bucket, count] : d.counts())
        EXPECT_EQ(count, 1) << "bucket " << bucket << " is pushed " << count << " times, it is merged and "
                            << "returned twice, which duplicates its keys in the result";
}

/// An input which has finished cannot send more chunks of a bucket, so it must not hold a delayed
/// bucket back. Otherwise a source which ends right on a delayed bucket keeps it until the whole query
/// is over, although every input which can still send something has moved past it long ago.
TEST(GroupingAggregatedOOO, FinishedInputDoesNotHoldADelayedBucket)
{
    Driver d(2);

    /// Both inputs postpone bucket 1 and send bucket 3.
    d.send(0, 3, {1});
    d.send(1, 3, {1});
    /// Input 0 resolves bucket 1 and ends right on it.
    d.send(0, 1);
    d.finishInput(0);
    /// Input 1 resolves bucket 1 and moves past it. Nothing can send anything of bucket 1 any more.
    d.send(1, 1);
    d.send(1, 4);
    d.step();

    /// Input 1 is still open here.
    EXPECT_EQ(d.counts()[1], 1) << "bucket 1 is not pushed while input 1 is open, a finished input which "
                               << "stopped at the bucket must not hold it until the end of the query";

    d.finish();
    for (auto [bucket, count] : d.counts())
        EXPECT_EQ(count, 1) << "bucket " << bucket << " is pushed " << count << " times";
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
TEST(GroupingAggregatedOOO, DelayedBucketIsPushedBeforeTheEnd)
{
    Driver d(2);

    /// Both inputs postpone bucket 1 and send bucket 2, then resolve bucket 1, then move past it.
    d.send(0, 2, {1});
    d.send(1, 2, {1});
    d.send(0, 1);
    d.send(1, 1);
    d.send(0, 3);
    d.send(1, 3);

    /// Both inputs are still open here.
    EXPECT_EQ(d.counts()[1], 1) << "bucket 1 is not pushed while the inputs are open, a delayed bucket has "
                                << "to be pushed as soon as every input has read a bucket after it";

    d.finish();
    for (auto [bucket, count] : d.counts())
        EXPECT_EQ(count, 1) << "bucket " << bucket << " is pushed " << count << " times";
}

namespace
{

/// The stream of one producer: the buckets in order, except that some are postponed and sent later,
/// which is what `enable_producing_buckets_out_of_order_in_aggregation` does. Every chunk reports the
/// buckets which are postponed and not sent yet, and a bucket can be split into several chunks.
std::vector<std::pair<Int32, std::vector<Int32>>> generateStream(pcg64 & rng, Int32 num_buckets)
{
    std::vector<std::pair<Int32, std::vector<Int32>>> stream;
    std::vector<Int32> postponed;
    Int32 next = 0;

    auto emit = [&](Int32 bucket)
    {
        std::vector<Int32> reported;
        for (auto p : postponed)
            if (p != bucket)
                reported.push_back(p);
        stream.emplace_back(bucket, reported);
        /// The producer converts the variants of a bucket one by one, so it can send it in parts.
        if (rng() % 4 == 0)
            stream.emplace_back(bucket, reported);
    };

    while (next < num_buckets || !postponed.empty())
    {
        const bool resolve = !postponed.empty() && (next >= num_buckets || rng() % 3 == 0);
        if (resolve)
        {
            const size_t index = rng() % postponed.size();
            const Int32 bucket = postponed[index];
            postponed.erase(postponed.begin() + index);
            emit(bucket);
        }
        else if (rng() % 4 == 0)
        {
            postponed.push_back(next++);
            std::ranges::sort(postponed);
        }
        else
        {
            emit(next++);
        }
    }
    return stream;
}

}

/// Whatever the producers do and in whatever order their chunks arrive, every bucket has to be pushed
/// exactly once and the transform has to finish. Being stricter about when a delayed bucket can be
/// pushed may only postpone a push, the in order push and the drain of the finished inputs still take
/// every bucket, so the transform must never get stuck holding one.
TEST(GroupingAggregatedOOO, EveryBucketIsPushedOnceAndTheTransformFinishesFuzz)
{
    constexpr Int32 num_buckets = 16;
    for (uint64_t seed = 0; seed < 300; ++seed)
    {
        pcg64 rng(seed);
        const size_t num_inputs = 2 + rng() % 2;

        Driver d(num_inputs);
        std::vector<std::vector<std::pair<Int32, std::vector<Int32>>>> streams;
        std::vector<size_t> positions(num_inputs, 0);
        for (size_t i = 0; i < num_inputs; ++i)
            streams.push_back(generateStream(rng, num_buckets));

        /// Interleave the streams at random, one chunk at a time. A chunk can only be sent to an input
        /// whose port is free, the transform reads an input only while it needs the buckets it sends.
        int idle = 0;
        for (;;)
        {
            std::vector<size_t> ready;
            bool anything_left = false;
            for (size_t i = 0; i < num_inputs; ++i)
            {
                if (positions[i] >= streams[i].size())
                    continue;
                anything_left = true;
                if (d.feeders[i]->canPush())
                    ready.push_back(i);
            }

            if (!anything_left)
                break;

            if (ready.empty())
            {
                /// Every port is busy, let the transform read them.
                d.step();
                ASSERT_LT(++idle, 100) << "seed " << seed << ": the transform stopped reading the inputs";
                continue;
            }

            idle = 0;
            const size_t input = ready[rng() % ready.size()];
            const auto & [bucket, reported] = streams[input][positions[input]++];
            d.send(input, bucket, reported);
            /// Sometimes let the transform breathe, this is when it pushes.
            if (rng() % 3 == 0)
                d.step();
        }

        d.finish();

        auto counts = d.counts();
        for (Int32 bucket = 0; bucket < num_buckets; ++bucket)
            EXPECT_EQ(counts[bucket], 1) << "seed " << seed << ": bucket " << bucket << " is pushed "
                                         << counts[bucket] << " times";
        EXPECT_EQ(d.last_status, IProcessor::Status::Finished)
            << "seed " << seed << ": the transform did not finish, it is stuck holding a bucket";

        if (::testing::Test::HasFailure())
            break;
    }
}
