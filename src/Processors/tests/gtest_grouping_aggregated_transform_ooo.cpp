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
#include <deque>
#include <iostream>
#include <map>
#include <optional>
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

/// Manual, protocol-faithful driver for GroupingAggregatedTransform.
///
/// We connect our own feeder OutputPorts to the transform's inputs and our own
/// consumer InputPort to its output. We then call prepare()/work() in a loop.
/// Each input has a queue of chunks to deliver, IN THE ORDER WE CHOOSE. Whenever
/// the transform marks an input "needed" and has no data on it, we deliver that
/// input's next queued chunk (or finish() it when its queue is empty).
///
/// This faithfully follows the executor's contract (only push to a needed,
/// empty port) yet gives full control over the per-input bucket order, so a
/// specific out-of-order arrival is reproduced deterministically - no scheduling
/// luck and no executor heuristics.
class ManualGroupingDriver
{
public:
    /// per_input[i] = the ordered list of buckets input i will deliver.
    ManualGroupingDriver(std::vector<std::deque<Chunk>> per_input, pcg64 & rng_)
        : num_inputs(per_input.size())
        , rng(rng_)
        , header(oneColumnHeader())
        , params(makeMergeParams(header))
        , transform(std::make_shared<GroupingAggregatedTransform>(*header, num_inputs, params))
        , queues(std::move(per_input))
        , input_finished(num_inputs, false)
    {
        feeders.reserve(num_inputs);
        auto in_it = transform->getInputs().begin();
        for (size_t i = 0; i < num_inputs; ++i, ++in_it)
        {
            feeders.emplace_back(std::make_unique<OutputPort>(header));
            connect(*feeders.back(), *in_it);
        }

        /// GroupingAggregatedTransform's output port has an EMPTY header.
        consumer = std::make_unique<InputPort>(transform->getOutputs().front().getSharedHeader());
        connect(transform->getOutputs().front(), *consumer);
        consumer->setNeeded();
    }

    /// Run to completion, delivering input chunks on demand. Returns the ordered
    /// list of bucket numbers the transform pushed downstream.
    std::vector<Int32> run()
    {
        runPushed();
        std::vector<Int32> buckets;
        buckets.reserve(pushed_full.size());
        for (const auto & p : pushed_full)
            buckets.push_back(p.bucket);
        return buckets;
    }

    /// Like run(), but returns each pushed chunk's (bucket, stamped out_of_order_buckets), so a
    /// caller can faithfully forward this transform's output (and its metadata) into a
    /// downstream layer - exactly what MergingAggregatedBucketTransform does in a multi-layer
    /// distributed query.
    const std::vector<PushedBucket> & runPushed()
    {
        IProcessor::UpdatedInputPorts all_inputs;
        for (auto & in : transform->getInputs())
            all_inputs.push_back(&in);

        IProcessor & proc = *transform; // drive via the public IProcessor interface (class is final)

        for (int guard = 0; guard < 1000000; ++guard)
        {
            consumer->setNeeded();
            IProcessor::UpdatedOutputPorts updated_outputs{&transform->getOutputs().front()};
            auto status = proc.prepare(all_inputs, updated_outputs);

            /// Drain any output the transform pushed.
            while (consumer->hasData())
            {
                consumer->setNeeded();
                pushed_full.push_back(chunkPushed(consumer->pull()));
            }

            if (status == IProcessor::Status::Ready)
            {
                proc.work();
                continue;
            }

            if (status == IProcessor::Status::Finished)
                break;

            /// status is NeedData / PortFull. Try to satisfy a needed input.
            bool delivered = deliverToNeededInput();

            if (!delivered && status == IProcessor::Status::NeedData)
            {
                /// The transform wants data but every needed input is exhausted;
                /// finish them so it can drain. If none left to finish -> stuck.
                if (!finishExhaustedNeededInputs())
                    break;
            }
            else if (!delivered && status == IProcessor::Status::PortFull)
            {
                /// Output is full but we always drain immediately, so this only
                /// happens transiently; loop again.
                continue;
            }
        }
        return pushed_full;
    }

    std::vector<PushedBucket> pushed_full;
    size_t num_inputs;

private:
    /// If any input port is needed and empty and has a queued chunk, push it. A RANDOM one of them, so
    /// that the order in which the inputs arrive is fuzzed as well, not only what every input sends.
    bool deliverToNeededInput()
    {
        const size_t start = std::uniform_int_distribution<size_t>(0, num_inputs - 1)(rng);
        for (size_t k = 0; k < num_inputs; ++k)
        {
            const size_t i = (start + k) % num_inputs;
            if (input_finished[i] || queues[i].empty() || !feeders[i]->canPush())
                continue;
            feeders[i]->push(std::move(queues[i].front()));
            queues[i].pop_front();
            return true;
        }
        return false;
    }

    /// Finish needed-but-empty inputs. Returns true if at least one was finished.
    bool finishExhaustedNeededInputs()
    {
        bool any = false;
        size_t i = 0;
        for (; i < num_inputs; ++i)
        {
            if (input_finished[i])
                continue;
            if (feeders[i]->canPush() && queues[i].empty())
            {
                feeders[i]->finish();
                input_finished[i] = true;
                any = true;
            }
        }
        return any;
    }

    pcg64 & rng;
    SharedHeader header;
    AggregatingTransformParamsPtr params;
    std::shared_ptr<GroupingAggregatedTransform> transform;
    std::vector<std::unique_ptr<OutputPort>> feeders;
    std::unique_ptr<InputPort> consumer;
    std::vector<std::deque<Chunk>> queues;
    std::vector<bool> input_finished;
};

std::vector<std::deque<Chunk>> buildInputs(std::vector<std::vector<std::pair<Int32, std::vector<Int32>>>> spec)
{
    std::vector<std::deque<Chunk>> inputs;
    for (auto & per_input : spec)
    {
        std::deque<Chunk> q;
        for (auto & [bucket, ooo] : per_input)
            q.push_back(makeBucketChunk(bucket, ooo));
        inputs.push_back(std::move(q));
    }
    return inputs;
}

void report(const std::vector<Int32> & pushed)
{
    std::cerr << "PUSHED ORDER:";
    for (auto b : pushed)
        std::cerr << ' ' << b;
    std::cerr << std::endl;
}

/// GroupingAggregatedTransform is ALLOWED to emit buckets out of order (that is the
/// purpose of the out-of-order optimization, PR #80179); the downstream
/// SortingAggregatedTransform re-sorts them. The ONLY hard invariant is: each
/// bucket is emitted EXACTLY ONCE. A duplicate is precisely the upstream cause of
/// the "SortingAggregatedTransform already got bucket" LOGICAL_ERROR.
void expectEachBucketOnce(const std::vector<Int32> & pushed)
{
    report(pushed);

    std::map<Int32, int> counts;
    for (auto b : pushed)
        counts[b]++;
    for (auto [bucket, c] : counts)
        EXPECT_EQ(c, 1) << "bucket " << bucket << " pushed " << c
                        << " times - a duplicate push is the 'SortingAggregatedTransform already got bucket' LOGICAL_ERROR";
}

}

namespace
{
/// Faithfully model ONE producer (ConvertingAggregatedToChunksTransform on a remote
/// shard): walk current_bucket 0..NUM, and for each, EITHER emit it now OR postpone
/// it (add to the pending set, capacity NUM_OOO_BUCKETS=4) and emit a later one
/// first; pending buckets get flushed out of order. Each EMITTED chunk carries the
/// CURRENT pending set as its out_of_order_buckets snapshot - exactly what
/// `ConvertingAggregatedToChunksTransform` stamps onto the chunk. This is the precise shape
/// of metadata the initiator's GroupingAggregatedTransform receives per RemoteSource.
std::vector<std::pair<Int32, std::vector<Int32>>> genProducerStream(pcg64 & rng, Int32 num_buckets)
{
    std::vector<std::pair<Int32, std::vector<Int32>>> out;
    std::vector<Int32> pending; // postponed-but-not-yet-sent, kept sorted
    Int32 cur = 0;

    auto emit = [&](Int32 b)
    {
        /// snapshot = pending set NOT counting b itself (b is being sent now)
        std::vector<Int32> snap;
        for (auto p : pending)
            if (p != b)
                snap.push_back(p);
        out.emplace_back(b, snap);
    };

    while (cur < num_buckets || !pending.empty())
    {
        bool can_postpone = cur < num_buckets && pending.size() < 4;
        bool flush_pending = !pending.empty()
            && (cur >= num_buckets || (std::uniform_int_distribution<int>(0, 2)(rng) == 0));

        if (flush_pending)
        {
            /// flush a random pending bucket (out of order)
            size_t k = std::uniform_int_distribution<size_t>(0, pending.size() - 1)(rng);
            Int32 b = pending[k];
            emit(b);
            pending.erase(pending.begin() + k);
        }
        else if (can_postpone && std::uniform_int_distribution<int>(0, 1)(rng) == 0)
        {
            pending.push_back(cur);
            std::sort(pending.begin(), pending.end());
            ++cur;
        }
        else if (cur < num_buckets)
        {
            emit(cur);
            ++cur;
        }
    }
    return out;
}
}

namespace
{
/// Run ONE "remote dist-layer shard": a real GroupingAggregatedTransform fed `num_inner`
/// CONTRACT-FAITHFUL producer streams (what the data shards send it). Its output is a bounded
/// reordering of [0..K), each bucket once, with the out_of_order_buckets metadata it STAMPED
/// on each pushed chunk. A downstream MergingAggregatedBucketTransform copies that metadata
/// into the AggregatedChunkInfo, so this (bucket, ooo) sequence is EXACTLY what the initiator's
/// GroupingAggregatedTransform receives from this shard. Returning the stamped metadata (rather
/// than dropping it) models the FIXED re-merge path; dropping it modelled the bug.
std::vector<PushedBucket> runRemoteShard(pcg64 & rng, size_t num_inner, Int32 num_buckets)
{
    std::vector<std::deque<Chunk>> inputs;
    for (size_t i = 0; i < num_inner; ++i)
    {
        auto stream = genProducerStream(rng, num_buckets);
        std::deque<Chunk> q;
        for (auto & [bucket, ooo] : stream)
            q.push_back(makeBucketChunk(bucket, ooo));
        inputs.push_back(std::move(q));
    }
    ManualGroupingDriver d(std::move(inputs), rng);
    return d.runPushed(); // reordered bucket sequence, each bucket once, WITH stamped metadata
}
}

/// FAITHFUL 2-LAYER dist-on-dist model (this is the 01223_dist_on_dist topology):
///   data shards -> [inner GroupingAggregatedTransform per dist_layer shard] -> initiator
///   GroupingAggregatedTransform.
/// Each of the initiator's N inputs is one remote dist_layer shard whose output is a
/// bounded-reordered permutation of [0..K). All shards cover the SAME bucket range (same key
/// space), so the initiator must MERGE bucket b from all N inputs into a single push.
///
/// The inner shard's GroupingAggregatedTransform may emit out of order; whether that out-of-
/// order stream carries delayed-bucket metadata is EXACTLY the bug-vs-fix axis:
///   - BEFORE the fix: pushData stamped nothing and MergingAggregatedBucketTransform copied
///     nothing -> the initiator saw an out-of-order stream with EMPTY metadata -> double push.
///   - AFTER the fix: the stamped metadata flows through, so the initiator's in-order push path
///     correctly skips a bucket still owed -> no double push.
/// This test forwards the inner shard's stamped metadata, so it FAILS pre-fix and PASSES
/// post-fix - a genuine regression test for the production change.
TEST(GroupingAggregatedOOO, TwoLayerDistMetadataFuzz)
{
    pcg64 rng(0x2A4E12340BADULL);
    int total = 0;
    for (int iter = 0; iter < 10000; ++iter)
    {
        size_t num_outer = std::uniform_int_distribution<size_t>(2, 4)(rng); // remote dist_layer shards
        Int32 num_buckets = std::uniform_int_distribution<Int32>(3, 12)(rng);

        std::vector<std::deque<Chunk>> outer_inputs;
        for (size_t s = 0; s < num_outer; ++s)
        {
            size_t num_inner = std::uniform_int_distribution<size_t>(2, 3)(rng); // data shards under this dist_layer
            auto shard_out = runRemoteShard(rng, num_inner, num_buckets);
            std::deque<Chunk> q;
            for (const auto & p : shard_out)
                q.push_back(makeBucketChunk(p.bucket, p.ooo)); // metadata forwarded by MergingAggregatedBucketTransform
            outer_inputs.push_back(std::move(q));
        }

        ManualGroupingDriver d(std::move(outer_inputs), rng);
        auto pushed = d.run();

        std::map<Int32, int> counts;
        for (auto b : pushed)
            counts[b]++;
        for (auto [bucket, c] : counts)
            ASSERT_EQ(c, 1) << "iter " << iter << ": bucket " << bucket << " pushed " << c
                            << " times (outer_inputs=" << num_outer << ", buckets=" << num_buckets << ")";
        ++total;
    }
    std::cerr << "TwoLayerDistMetadataFuzz iterations checked: " << total << std::endl;
}

/// A producer can declare a bucket delayed and then resolve it empty, dropping it without ever
/// sending it. The consumer decrements the count of that bucket in `out_of_order_buckets` back to
/// 0, but the entry itself stays, and the bucket is never buffered in `chunks_map`. Such a zero
/// count entry must not be reported as owed to the next layer, otherwise that layer holds back the
/// real chunks of the bucket from all the other inputs until the stream finishes.

/// What an inner dist-layer shard stamps when it had a delayed-then-empty bucket.
/// input: (0, ooo=[1]) then (2, ooo=[]) - declare 1 delayed while sending 0, then resolve 1
/// empty and send 2 (bucket 1 is NEVER delivered). Bucket 1 ends up as a zero-count zombie in
/// the shard's out_of_order_buckets. The stamped metadata on the shard's pushed chunks (0 and
/// 2) must NOT list bucket 1 - it is not owed by anyone, the shard simply skipped it.
TEST(GroupingAggregatedOOO, EmptyDelayedBucketNotStampedAsOwed)
{
    pcg64 rng(0xE0B0CE7);
    auto inputs = buildInputs({
        {{0, {1}}, {2, {}}},   // single shard: postpone 1, resolve it empty, emit 2
    });
    ManualGroupingDriver d(std::move(inputs), rng);
    const auto & pushed = d.runPushed();

    /// All buckets must still be pushed exactly once.
    std::vector<Int32> bucket_order;
    for (const auto & p : pushed)
        bucket_order.push_back(p.bucket);
    expectEachBucketOnce(bucket_order);

    for (const auto & p : pushed)
    {
        EXPECT_EQ(std::ranges::count(p.ooo, 1), 0)
            << "bucket 1 was resolved empty (never delivered), it must not be stamped as still-owed "
            << "on the push of bucket " << p.bucket
            << " - forwarding a zero-count entry breaks the downstream memory-efficient merge";
        /// Sanity: a bucket is never owed to itself.
        EXPECT_EQ(std::ranges::count(p.ooo, p.bucket), 0);
    }
}

/// Faithful producer model EXTENDED to resolve postponed buckets EMPTY at random: when a
/// postponed bucket is "flushed", with some probability it is dropped (empty) instead of
/// emitted - exactly the producer's empty-bucket path. The stamped snapshot then naturally
/// excludes a bucket the moment it is resolved (emitted OR dropped), so forwarding the inner
/// shard's stamped metadata downstream must never duplicate a bucket - including across the
/// sparse/empty cases the prior fuzz never produced.
namespace
{
std::vector<std::pair<Int32, std::vector<Int32>>> genProducerStreamWithEmpties(pcg64 & rng, Int32 num_buckets)
{
    std::vector<std::pair<Int32, std::vector<Int32>>> out;
    std::vector<Int32> pending; // postponed-but-not-yet-resolved, kept sorted
    Int32 cur = 0;

    auto emit = [&](Int32 b)
    {
        std::vector<Int32> snap;
        for (auto p : pending)
            if (p != b)
                snap.push_back(p);
        out.emplace_back(b, snap);
    };

    while (cur < num_buckets || !pending.empty())
    {
        bool can_postpone = cur < num_buckets && pending.size() < 4;
        bool resolve_pending = !pending.empty()
            && (cur >= num_buckets || (std::uniform_int_distribution<int>(0, 2)(rng) == 0));

        if (resolve_pending)
        {
            size_t k = std::uniform_int_distribution<size_t>(0, pending.size() - 1)(rng);
            Int32 b = pending[k];
            pending.erase(pending.begin() + k);
            /// Resolve it: 1/3 of the time the postponed bucket turns out EMPTY -> dropped,
            /// never emitted (but already removed from the pending snapshot, like the producer).
            if (std::uniform_int_distribution<int>(0, 2)(rng) != 0)
                emit(b);
        }
        else if (can_postpone && std::uniform_int_distribution<int>(0, 1)(rng) == 0)
        {
            pending.push_back(cur);
            std::sort(pending.begin(), pending.end());
            ++cur;
        }
        else if (cur < num_buckets)
        {
            /// In-order buckets can also be empty and skipped entirely.
            if (std::uniform_int_distribution<int>(0, 4)(rng) != 0)
                emit(cur);
            ++cur;
        }
    }
    return out;
}

std::vector<PushedBucket> runRemoteShardWithEmpties(pcg64 & rng, size_t num_inner, Int32 num_buckets)
{
    std::vector<std::deque<Chunk>> inputs;
    for (size_t i = 0; i < num_inner; ++i)
    {
        auto stream = genProducerStreamWithEmpties(rng, num_buckets);
        std::deque<Chunk> q;
        for (auto & [bucket, ooo] : stream)
            q.push_back(makeBucketChunk(bucket, ooo));
        inputs.push_back(std::move(q));
    }
    ManualGroupingDriver d(std::move(inputs), rng);
    return d.runPushed();
}
}

TEST(GroupingAggregatedOOO, TwoLayerDistSparseEmptyBucketFuzz)
{
    pcg64 rng(0x5A4E12340E47ULL);
    int total = 0;
    for (int iter = 0; iter < 12000; ++iter)
    {
        size_t num_outer = std::uniform_int_distribution<size_t>(2, 4)(rng);
        Int32 num_buckets = std::uniform_int_distribution<Int32>(3, 12)(rng);

        std::vector<std::deque<Chunk>> outer_inputs;
        for (size_t s = 0; s < num_outer; ++s)
        {
            size_t num_inner = std::uniform_int_distribution<size_t>(2, 3)(rng);
            auto shard_out = runRemoteShardWithEmpties(rng, num_inner, num_buckets);
            std::deque<Chunk> q;
            for (const auto & p : shard_out)
                q.push_back(makeBucketChunk(p.bucket, p.ooo)); // metadata forwarded by MergingAggregatedBucketTransform

            /// Sanity: a bucket is never owed to itself. (The exact minimal "empty-resolved bucket
            /// must not be stamped as owed" case is pinned by EmptyDelayedBucketNotStampedAsOwed;
            /// this fuzz is the broad end-to-end no-duplicate safety net across sparse/empty shapes.)
            for (const auto & p : shard_out)
                for (auto owed : p.ooo)
                    ASSERT_NE(owed, p.bucket) << "iter " << iter << ": bucket owed to itself";
            outer_inputs.push_back(std::move(q));
        }

        ManualGroupingDriver d(std::move(outer_inputs), rng);
        auto pushed = d.run();

        std::map<Int32, int> counts;
        for (auto b : pushed)
            counts[b]++;
        for (auto [bucket, c] : counts)
            ASSERT_EQ(c, 1) << "iter " << iter << ": bucket " << bucket << " pushed " << c
                            << " times (outer_inputs=" << num_outer << ", buckets=" << num_buckets << ")";
        ++total;
    }
    std::cerr << "TwoLayerDistSparseEmptyBucketFuzz iterations checked: " << total << std::endl;
}

/// The tests above model an intermediate layer as `GroupingAggregatedTransform` alone, which is
/// only a half of it: `addMergingAggregatedMemoryEfficientTransform` builds
/// `GroupingAggregated -> Resize -> N x MergingAggregatedBucket -> SortingAggregated`, and the
/// mergers finish in an order unrelated to the order they started in. So `SortingAggregatedTransform`
/// can emit a bucket after a bigger one which `GroupingAggregatedTransform` emitted earlier, an
/// inversion which no metadata stamped upstream describes: when 6 is delayed, pushed, and 8 is
/// pushed after it, the stamp on 8 does not mention 6. That is why this transform re-derives the
/// delayed buckets instead of forwarding the stamp. These tests drive it with a randomized merger
/// completion order.

namespace
{

/// A chunk pushed by SortingAggregatedTransform carries AggregatedChunkInfo (it is the output
/// of MergingAggregatedBucketTransform), not ChunksToMerge.
PushedBucket chunkPushedAggregated(const Chunk & chunk)
{
    auto info = chunk.getChunkInfos().get<AggregatedChunkInfo>();
    chassert(info);
    return PushedBucket{info->bucket_num, info->out_of_order_buckets};
}

/// Manual, protocol-faithful driver for SortingAggregatedTransform, in the same style as
/// ManualGroupingDriver: we own the ports and decide WHICH merger delivers its next chunk
/// next, which is exactly the freedom the real ResizeProcessor + concurrent mergers have.
class ManualSortingDriver
{
public:
    ManualSortingDriver(std::vector<std::deque<Chunk>> per_input, pcg64 & rng_)
        : num_inputs(per_input.size())
        , rng(rng_)
        , header(oneColumnHeader())
        , params(makeMergeParams(header))
        , transform(std::make_shared<SortingAggregatedTransform>(num_inputs, params))
        , queues(std::move(per_input))
        , input_finished(num_inputs, false)
    {
        feeders.reserve(num_inputs);
        auto in_it = transform->getInputs().begin();
        for (size_t i = 0; i < num_inputs; ++i, ++in_it)
        {
            feeders.emplace_back(std::make_unique<OutputPort>(header));
            connect(*feeders.back(), *in_it);
        }

        consumer = std::make_unique<InputPort>(transform->getOutputs().front().getSharedHeader());
        connect(transform->getOutputs().front(), *consumer);
        consumer->setNeeded();
    }

    const std::vector<PushedBucket> & runPushed()
    {
        for (int guard = 0; guard < 1000000; ++guard)
        {
            consumer->setNeeded();
            auto status = transform->prepare();

            while (consumer->hasData())
            {
                consumer->setNeeded();
                pushed.push_back(chunkPushedAggregated(consumer->pull()));
            }

            if (status == IProcessor::Status::Finished)
                break;

            if (!deliverToNeededInput() && status == IProcessor::Status::NeedData
                && !finishExhaustedNeededInputs())
                break;
        }
        return pushed;
    }

    std::vector<PushedBucket> pushed;
    size_t num_inputs;

private:
    /// Deliver from a RANDOM eligible merger, so that a chunk sitting in a slow merger can be
    /// overtaken by chunks from faster ones - the whole point of this layer.
    bool deliverToNeededInput()
    {
        const size_t start = std::uniform_int_distribution<size_t>(0, num_inputs - 1)(rng);
        for (size_t k = 0; k < num_inputs; ++k)
        {
            const size_t i = (start + k) % num_inputs;
            if (input_finished[i] || queues[i].empty() || !feeders[i]->canPush())
                continue;
            feeders[i]->push(std::move(queues[i].front()));
            queues[i].pop_front();
            return true;
        }
        return false;
    }

    bool finishExhaustedNeededInputs()
    {
        bool any = false;
        for (size_t i = 0; i < num_inputs; ++i)
        {
            if (input_finished[i])
                continue;
            if (feeders[i]->canPush() && queues[i].empty())
            {
                feeders[i]->finish();
                input_finished[i] = true;
                any = true;
            }
        }
        return any;
    }

    pcg64 & rng;
    SharedHeader header;
    AggregatingTransformParamsPtr params;
    std::shared_ptr<SortingAggregatedTransform> transform;
    std::vector<std::unique_ptr<OutputPort>> feeders;
    std::unique_ptr<InputPort> consumer;
    std::vector<std::deque<Chunk>> queues;
    std::vector<bool> input_finished;
};

/// ResizeProcessor + N concurrent MergingAggregatedBucketTransform-s: every chunk goes to
/// exactly one merger, each merger preserves the order it received, and the metadata is copied
/// into AggregatedChunkInfo (MergingAggregatedBucketTransform::transform).
std::vector<std::deque<Chunk>> fanOutToMergers(const std::vector<PushedBucket> & from_grouping, size_t num_mergers, pcg64 & rng)
{
    std::vector<std::deque<Chunk>> mergers(num_mergers);
    for (const auto & p : from_grouping)
        mergers[std::uniform_int_distribution<size_t>(0, num_mergers - 1)(rng)].push_back(makeBucketChunk(p.bucket, p.ooo));
    return mergers;
}

/// The contract EVERY layer owes the next GroupingAggregatedTransform: if a bucket is emitted
/// after a bigger one, that bigger one must have announced it as delayed. Otherwise the
/// consumer finalizes the smaller bucket with partial data and pushes it AGAIN when the late
/// chunk arrives - the duplicated keys this whole file is about.
void expectDelayedBucketsAnnounced(const std::vector<PushedBucket> & pushed, int iter = -1)
{
    for (size_t i = 0; i < pushed.size(); ++i)
    {
        for (size_t j = i + 1; j < pushed.size(); ++j)
        {
            if (pushed[j].bucket >= pushed[i].bucket)
                continue;
            ASSERT_NE(std::ranges::find(pushed[i].ooo, pushed[j].bucket), pushed[i].ooo.end())
                << "iter " << iter << ": bucket " << pushed[j].bucket << " was emitted after bucket "
                << pushed[i].bucket << ", which did not announce it as delayed";
        }
    }
}

/// A full intermediate layer: GroupingAggregated -> Resize -> N mergers -> SortingAggregated.
std::vector<PushedBucket> runDistLayer(pcg64 & rng, size_t num_inner, Int32 num_buckets, size_t num_mergers)
{
    std::vector<std::deque<Chunk>> inputs;
    for (size_t i = 0; i < num_inner; ++i)
    {
        std::deque<Chunk> q;
        for (auto & [bucket, ooo] : genProducerStream(rng, num_buckets))
            q.push_back(makeBucketChunk(bucket, ooo));
        inputs.push_back(std::move(q));
    }

    ManualGroupingDriver grouping(std::move(inputs), rng);
    auto mergers = fanOutToMergers(grouping.runPushed(), num_mergers, rng);
    ManualSortingDriver sorting(std::move(mergers), rng);
    return sorting.runPushed();
}

}

/// The minimal shape of the inversion this layer creates on its own.
/// GroupingAggregated pushed 6 while it was delayed, and 8 after 6 had already gone, so the
/// stamp on 8 does NOT mention 6. Put 6 on a slow merger and 8 on a fast one: SortingAggregated
/// emits 8 first, and it must announce 6 itself - nobody upstream did.
TEST(GroupingAggregatedOOO, SortingLayerAnnouncesInversionItCreated)
{
    pcg64 rng(0xB0A710ULL);

    /// merger 0 got buckets 6 and 9, merger 1 got 8 - and merger 0 is slow.
    std::vector<std::deque<Chunk>> mergers(2);
    mergers[0].push_back(makeBucketChunk(6, {}));
    mergers[0].push_back(makeBucketChunk(9, {}));
    mergers[1].push_back(makeBucketChunk(8, {6}));

    ManualSortingDriver d(std::move(mergers), rng);
    const auto & pushed = d.runPushed();

    std::vector<Int32> order;
    for (const auto & p : pushed)
        order.push_back(p.bucket);
    expectEachBucketOnce(order);
    expectDelayedBucketsAnnounced(pushed);
}

/// Fuzz the merging layer alone: contract-faithful GroupingAggregated output, fanned out to
/// 2..6 mergers completing in random order. Whatever SortingAggregatedTransform emits must
/// still satisfy the contract for the next node.
TEST(GroupingAggregatedOOO, SortingLayerRederivesDelayedBucketsFuzz)
{
    pcg64 rng(0x50A7C0DEULL);
    int total = 0;
    for (int iter = 0; iter < 25000; ++iter)
    {
        const size_t num_inner = std::uniform_int_distribution<size_t>(2, 4)(rng);
        const Int32 num_buckets = std::uniform_int_distribution<Int32>(3, 12)(rng);
        const size_t num_mergers = std::uniform_int_distribution<size_t>(2, 6)(rng);

        auto layer_out = runDistLayer(rng, num_inner, num_buckets, num_mergers);

        std::map<Int32, int> counts;
        for (const auto & p : layer_out)
            counts[p.bucket]++;
        for (auto [bucket, c] : counts)
            ASSERT_EQ(c, 1) << "iter " << iter << ": bucket " << bucket << " emitted " << c << " times";

        expectDelayedBucketsAnnounced(layer_out, iter);
        ++total;
    }
    std::cerr << "SortingLayerRederivesDelayedBucketsFuzz iterations checked: " << total << std::endl;
}

/// The complete dist-on-dist topology, with the merging layer in place:
///     data shards -> [GroupingAggregated -> Resize -> N mergers -> SortingAggregated] -> initiator
/// This is the gap the Grouping-only model leaves: with the metadata merely forwarded rather
/// than re-derived after sorting, the initiator pushes a bucket twice.
TEST(GroupingAggregatedOOO, TwoLayerDistThroughSortingLayerFuzz)
{
    pcg64 rng(0x7A9E5041ULL);
    int total = 0;
    for (int iter = 0; iter < 7000; ++iter)
    {
        const size_t num_outer = std::uniform_int_distribution<size_t>(2, 4)(rng);
        const Int32 num_buckets = std::uniform_int_distribution<Int32>(3, 12)(rng);

        std::vector<std::deque<Chunk>> outer_inputs;
        for (size_t s = 0; s < num_outer; ++s)
        {
            const size_t num_inner = std::uniform_int_distribution<size_t>(2, 3)(rng);
            const size_t num_mergers = std::uniform_int_distribution<size_t>(2, 6)(rng);
            std::deque<Chunk> q;
            for (const auto & p : runDistLayer(rng, num_inner, num_buckets, num_mergers))
                q.push_back(makeBucketChunk(p.bucket, p.ooo));
            outer_inputs.push_back(std::move(q));
        }

        ManualGroupingDriver d(std::move(outer_inputs), rng);
        auto pushed = d.run();

        std::map<Int32, int> counts;
        for (auto b : pushed)
            counts[b]++;
        for (auto [bucket, c] : counts)
            ASSERT_EQ(c, 1) << "iter " << iter << ": bucket " << bucket << " pushed " << c
                            << " times (outer_inputs=" << num_outer << ", buckets=" << num_buckets << ")";
        ++total;
    }
    std::cerr << "TwoLayerDistThroughSortingLayerFuzz iterations checked: " << total << std::endl;
}
