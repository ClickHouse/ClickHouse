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
/// out_of_order_buckets metadata. EMPTY by default - which is exactly what a
/// dist layer's MergingAggregatedBucketTransform produces: it builds a FRESH
/// AggregatedChunkInfo (MergingAggregatedMemoryEfficientTransform.cpp:388-392)
/// that does NOT copy out_of_order_buckets. So a downstream
/// GroupingAggregatedTransform receives a stream that may be OUT OF ORDER in
/// bucket_num but carries NO "this bucket was delayed" metadata.
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

/// A SINGLE-LEVEL chunk (bucket_num = -1) carrying `num_keys` distinct keys, so
/// that convertBlockToTwoLevel splits it across MANY two-level buckets. This is
/// what a remote shard sends when its data stayed below group_by_two_level_threshold.
Chunk makeSingleLevelChunk(Int64 num_keys)
{
    auto col = ColumnUInt64::create();
    for (Int64 k = 0; k < num_keys; ++k)
        col->insertValue(static_cast<UInt64>(k));
    Columns columns;
    columns.emplace_back(std::move(col));
    Chunk chunk(std::move(columns), num_keys);

    auto info = std::make_shared<AggregatedChunkInfo>();
    info->bucket_num = -1; // single level
    info->is_overflows = false;
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
        /*serialize_string_with_zero_byte_=*/ false);
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
    explicit ManualGroupingDriver(std::vector<std::deque<Chunk>> per_input)
        : num_inputs(per_input.size())
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
    /// If any input port is needed and empty and has a queued chunk, push it.
    bool deliverToNeededInput()
    {
        size_t i = 0;
        auto in_it = transform->getInputs().begin();
        for (; i < num_inputs; ++i, ++in_it)
        {
            if (input_finished[i])
                continue;
            if (!feeders[i]->canPush())
                continue; // not needed yet, or still holds data
            if (queues[i].empty())
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

/// Some tests additionally want to confirm well-behaved ascending output.
void expectEachBucketOnceAndAscending(const std::vector<Int32> & pushed)
{
    expectEachBucketOnce(pushed);
    for (size_t i = 1; i < pushed.size(); ++i)
        EXPECT_LE(pushed[i - 1], pushed[i])
            << "buckets pushed out of ascending order (" << pushed[i - 1] << " then " << pushed[i] << ")";
}

}

/// Sanity: a single in-order input -> each bucket pushed exactly once, ascending.
TEST(GroupingAggregatedOOO, InOrderSingleInputOk)
{
    auto inputs = buildInputs({{{0, {}}, {1, {}}, {2, {}}}});
    ManualGroupingDriver d(std::move(inputs));
    expectEachBucketOnceAndAscending(d.run());
}

/// Two in-order inputs -> still ascending, each bucket once.
TEST(GroupingAggregatedOOO, InOrderTwoInputsOk)
{
    auto inputs = buildInputs({
        {{0, {}}, {2, {}}, {5, {}}},
        {{0, {}}, {1, {}}, {5, {}}},
    });
    ManualGroupingDriver d(std::move(inputs));
    expectEachBucketOnceAndAscending(d.run());
}

/// THE BUG. Both inputs are OUT OF ORDER and carry EMPTY out_of_order_buckets
/// metadata - exactly what a dist layer emits after its MergingAggregatedBucket
/// Transform strips the metadata (cpp:388-392) and its own GroupingAggregated
/// Transform was allowed to emit buckets out of order.
///
/// input0: 0, 6        input1: 5, 0   (input1 steps BACKWARDS 5 -> 0, no metadata)
///
/// Expected-correct behaviour: every bucket pushed exactly once, in ascending
/// order. If the consumer instead advances past bucket 0 (pushing it once with
/// only input0's data) and then re-pushes it when input1's late 0 arrives, we get
/// a duplicate / out-of-order push == the LOGICAL_ERROR signature.
TEST(GroupingAggregatedOOO, OutOfOrderEmptyMetadataDoublePush)
{
    auto inputs = buildInputs({
        {{0, {}}, {6, {}}},
        {{5, {}}, {0, {}}},
    });
    ManualGroupingDriver d(std::move(inputs));
    expectEachBucketOnceAndAscending(d.run());
}

/// Wider variant: more inputs, more aggressive backward steps.
TEST(GroupingAggregatedOOO, OutOfOrderEmptyMetadataManyBackwardSteps)
{
    auto inputs = buildInputs({
        {{0, {}}, {10, {}}},
        {{7, {}}, {1, {}}},
        {{8, {}}, {2, {}}},
    });
    ManualGroupingDriver d(std::move(inputs));
    expectEachBucketOnce(d.run());
}

/// NON-EMPTY out_of_order_buckets metadata - this is what the INITIATOR sees,
/// because the producer (ConvertingAggregatedToChunksTransform) records each
/// emitted chunk's pending-bucket snapshot in AggregatedChunkInfo and it survives
/// network serialization (BlockInfo.h field 3). The accounting in
/// GroupingAggregatedTransform::addChunk (.cpp:296-303) maintains a per-bucket
/// "how many inputs still owe this bucket" map by diffing each input's previous vs
/// new ooo snapshot. The OOO push path (.cpp:69-82) pushes & ERASES a bucket when
/// its count hits 0; the in-order path (.cpp:84-92) pushes any bucket not in the map.
///
/// Probe: a single input declares bucket 1 as delayed (ooo=[1]) while sending
/// bucket 0, then later actually sends bucket 1 with ooo=[] (no longer delayed).
/// A second input sends 0 then 1 in order. Assert each bucket pushed once,
/// ascending - any duplicate is the upstream cause of the SortingAggregated crash.
TEST(GroupingAggregatedOOO, NonEmptyMetadataDelayedThenDelivered)
{
    auto inputs = buildInputs({
        {{0, {1}}, {1, {}}},   // input0: send 0 (delaying 1), then send 1
        {{0, {}},  {1, {}}},   // input1: in order
    });
    ManualGroupingDriver d(std::move(inputs));
    expectEachBucketOnce(d.run());
}

/// Two inputs both declare the SAME bucket delayed, deliver it at different times,
/// stepping through higher buckets meanwhile - stresses the map count going 2->1->0
/// across the OOO and in-order push paths.
TEST(GroupingAggregatedOOO, NonEmptyMetadataBothDelaySameBucket)
{
    auto inputs = buildInputs({
        {{0, {2}}, {3, {2}}, {2, {}}},   // input0: delays 2 twice, then delivers it late
        {{0, {2}}, {2, {}},  {3, {}}},   // input1: delays 2 once, delivers it earlier
    });
    ManualGroupingDriver d(std::move(inputs));
    expectEachBucketOnce(d.run());
}

/// SINGLE-LEVEL + TWO-LEVEL MIX. A remote shard whose data stayed below
/// group_by_two_level_threshold sends a single-level chunk (bucket=-1); another
/// shard sends two-level buckets. GroupingAggregatedTransform::work() (cpp:316-341)
/// converts the single-level chunk into two-level buckets and APPENDS them to
/// chunks_map. If two-level buckets were already pushed (next_bucket_to_push
/// advanced) before the single-level chunk is converted, the conversion can produce
/// buckets BELOW next_bucket_to_push that get pushed AGAIN on the finish drain.
///
/// input0: a single-level chunk (many keys -> spans many two-level buckets).
/// input1: two-level buckets 0,1,2,3 in order (these get pushed, advancing
///         next_bucket_to_push), delivered BEFORE input0's single-level chunk.
TEST(GroupingAggregatedOOO, SingleLevelAfterTwoLevelPushed)
{
    auto header = oneColumnHeader();
    std::vector<std::deque<Chunk>> inputs(2);
    /// input1 delivers two-level buckets first.
    for (Int32 b : {0, 1, 2, 3})
        inputs[1].push_back(makeBucketChunk(b));
    /// input0 delivers a single-level chunk LAST (after two-level pushed).
    inputs[0].push_back(makeSingleLevelChunk(512));

    ManualGroupingDriver d(std::move(inputs));
    expectEachBucketOnce(d.run());
}

/// Reverse interleave: single-level first, then two-level - the conversion happens
/// while two-level buckets coexist (the guard at cpp:183 path).
TEST(GroupingAggregatedOOO, SingleLevelBeforeTwoLevel)
{
    std::vector<std::deque<Chunk>> inputs(2);
    inputs[0].push_back(makeSingleLevelChunk(512));
    for (Int32 b : {0, 1, 2, 3})
        inputs[1].push_back(makeBucketChunk(b));

    ManualGroupingDriver d(std::move(inputs));
    expectEachBucketOnce(d.run());
}

namespace
{
/// Faithfully model ONE producer (ConvertingAggregatedToChunksTransform on a remote
/// shard): walk current_bucket 0..NUM, and for each, EITHER emit it now OR postpone
/// it (add to the pending set, capacity NUM_OOO_BUCKETS=4) and emit a later one
/// first; pending buckets get flushed out of order. Each EMITTED chunk carries the
/// CURRENT pending set as its out_of_order_buckets snapshot - exactly what
/// AggregatingTransform.cpp:636/651 stamps onto the chunk. This is the precise shape
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

/// Fuzz: generate many random-but-CONTRACT-FAITHFUL producer streams across 2-4
/// inputs and assert the merged output never duplicates a bucket. A single
/// duplicate is the upstream root cause of the SortingAggregatedTransform crash.
TEST(GroupingAggregatedOOO, FuzzNonEmptyMetadataNoDuplicate)
{
    pcg64 rng(0xC10C0FFEEULL);
    int total = 0;
    for (int iter = 0; iter < 20000; ++iter)
    {
        size_t num_inputs = std::uniform_int_distribution<size_t>(2, 4)(rng);
        Int32 num_buckets = std::uniform_int_distribution<Int32>(3, 12)(rng);

        std::vector<std::deque<Chunk>> inputs;
        for (size_t i = 0; i < num_inputs; ++i)
        {
            auto stream = genProducerStream(rng, num_buckets);
            std::deque<Chunk> q;
            for (auto & [bucket, ooo] : stream)
                q.push_back(makeBucketChunk(bucket, ooo));
            inputs.push_back(std::move(q));
        }

        ManualGroupingDriver d(std::move(inputs));
        auto pushed = d.run();

        std::map<Int32, int> counts;
        for (auto b : pushed)
            counts[b]++;
        for (auto [bucket, c] : counts)
            ASSERT_EQ(c, 1) << "iter " << iter << ": bucket " << bucket << " pushed " << c
                            << " times (inputs=" << num_inputs << ", buckets=" << num_buckets << ")";
        ++total;
    }
    std::cerr << "fuzz iterations checked: " << total << std::endl;
}

/// The producer keeps a bucket in its ooo snapshot until it actually SENDS it, so a
/// chunk that delays B carries B in ooo, and the chunk that finally delivers B is
/// the FIRST one without B in ooo. Model an input whose ooo snapshot SHRINKS as it
/// delivers, against a second input that races ahead - the exact desync surface.
TEST(GroupingAggregatedOOO, NonEmptyMetadataShrinkingSnapshotRace)
{
    auto inputs = buildInputs({
        {{0, {1, 2}}, {3, {1, 2}}, {1, {2}}, {2, {}}},   // input0
        {{0, {}},     {1, {}},     {2, {}},  {3, {}}},   // input1 strictly in order
    });
    ManualGroupingDriver d(std::move(inputs));
    expectEachBucketOnce(d.run());
}

/// =====================================================================================
/// THE GAP (run 3): EMPTY-METADATA out-of-order streams, fuzzed broadly + swept.
///
/// The prior fuzz (FuzzNonEmptyMetadataNoDuplicate) only fed CONTRACT-FAITHFUL streams
/// (every reordered chunk carried correct out_of_order_buckets metadata). But the REAL
/// danger is a MIDDLE dist layer that used num_merging_processors<=1
/// (addMergingAggregatedMemoryEfficientTransform.cpp:564-568): there is NO
/// SortingAggregatedTransform to re-sort, AND MergingAggregatedBucketTransform::transform
/// (cpp:388-392) builds a FRESH AggregatedChunkInfo that does NOT copy
/// out_of_order_buckets. So that layer emits a stream that is OUT OF ORDER in bucket_num
/// but carries EMPTY ooo metadata. A downstream GroupingAggregatedTransform fed such a
/// stream has NO "this bucket is still owed" signal, so its in-order push path
/// (cpp:84-92) can finalize a bucket B (next_bucket_to_push advances past B) while an
/// input still has a late B queued; when that late B arrives it resurrects chunks_map[B]
/// and the finish drain (cpp:60-66) re-pushes it => DOUBLE PUSH => the exact upstream
/// cause of "SortingAggregatedTransform already got bucket with number N".
/// =====================================================================================

/// Deterministic consumer-contract demonstration of the EXACT minimal trigger and WHY the fix
/// works. A dist_layer that postpones bucket 0 (emits 1,2 first, then 0) sends, per the OOO
/// optimization, a stream that is out of order in bucket_num.
///   - The BUGGY re-merge path dropped the delayed-bucket metadata, so the initiator received
///     [(1,{}),(2,{}),(0,{})] from that shard - and pushes bucket 0 TWICE (once prematurely in
///     order, once when its late copy arrives), which downstream becomes the
///     SortingAggregatedTransform "already got bucket" logical error.
///   - The FIXED re-merge path forwards the metadata: [(1,{0}),(2,{0}),(0,{})]. With the "0 is
///     still owed" signal the initiator's in-order push path skips bucket 0 until it actually
///     arrives, so it is pushed exactly once.
/// This test asserts the FIXED stream merges cleanly (each bucket once), and documents that the
/// metadata is precisely what prevents the duplicate. The consumer itself is unchanged by the
/// fix; correctness depends on the producer always supplying this metadata (see
/// TwoLayerDistMetadataFuzz for the end-to-end check through the real production code path).
TEST(GroupingAggregatedOOO, DelayedBucketMetadataPreventsDoublePush)
{
    auto inputs = buildInputs({
        {{1, {0}}, {2, {0}}, {0, {}}},   // dist_layer shard 0: postponed 0, metadata forwarded
        {{0, {}},  {1, {}},  {2, {}}},   // dist_layer shard 1: in order
    });
    ManualGroupingDriver d(std::move(inputs));
    expectEachBucketOnce(d.run());
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
    ManualGroupingDriver d(std::move(inputs));
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

        ManualGroupingDriver d(std::move(outer_inputs));
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

/// =====================================================================================
/// SPARSE / EMPTY DELAYED BUCKET (review follow-up).
///
/// The OUT-OF-ORDER optimization lets an input declare a bucket delayed and then resolve
/// it EMPTY: the producer (ConvertingAggregatedToChunksTransform) postpones a bucket, finds
/// it has no rows, and drops it WITHOUT ever sending it - erasing it from the pending set
/// before stamping the next chunk (AggregatingTransform.cpp:592-600,633-639). The consumer
/// mirrors this in `addChunk`: the bucket's count in `out_of_order_buckets` is decremented
/// back to 0, but the map entry is NOT erased (it is erased only on the OOO push path,
/// cpp:91-94) and the bucket is never buffered in `chunks_map`. So a zero-count entry for an
/// empty-resolved bucket lingers.
///
/// When stamping metadata for a downstream re-merge layer, such zero-count entries must NOT
/// be forwarded: a sparse two-level layout (only some of 0..255 present) routinely leaves
/// them, and advertising one as "still owed" makes the next layer hold back the real chunk
/// for that bucket from every other input until the stream finishes, breaking the
/// memory-efficient merge contract (and re-introducing the very double-push this fix removes).
/// =====================================================================================

/// What an inner dist-layer shard stamps when it had a delayed-then-empty bucket.
/// input: (0, ooo=[1]) then (2, ooo=[]) - declare 1 delayed while sending 0, then resolve 1
/// empty and send 2 (bucket 1 is NEVER delivered). Bucket 1 ends up as a zero-count zombie in
/// the shard's out_of_order_buckets. The stamped metadata on the shard's pushed chunks (0 and
/// 2) must NOT list bucket 1 - it is not owed by anyone, the shard simply skipped it.
TEST(GroupingAggregatedOOO, EmptyDelayedBucketNotStampedAsOwed)
{
    auto inputs = buildInputs({
        {{0, {1}}, {2, {}}},   // single shard: postpone 1, resolve it empty, emit 2
    });
    ManualGroupingDriver d(std::move(inputs));
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

/// End-to-end: a downstream initiator must still merge real bucket-1 data once a sibling shard
/// supplies it, NOT buffer it to the end of the stream.
///   - shard A: postpones 1, resolves it EMPTY, emits 0 and 2 (its stamped metadata models the
///     fixed re-merge path: bucket 1 must NOT appear as owed).
///   - shard B: delivers buckets 0,1,2 in order, with real data for bucket 1.
/// The initiator must push 0,1,2 each exactly once. If shard A wrongly advertised bucket 1 as
/// owed, the initiator would withhold shard B's bucket 1 (its in-order path skips owed buckets,
/// cpp:99-107) until the stream finishes - and then could double-push it on the finish drain.
TEST(GroupingAggregatedOOO, EmptyDelayedBucketDownstreamMergesRealData)
{
    /// Run shard A through a real GroupingAggregatedTransform and forward its STAMPED metadata,
    /// exactly as MergingAggregatedBucketTransform would in a multi-layer query.
    std::vector<PushedBucket> shard_a;
    {
        auto inputs = buildInputs({{{0, {1}}, {2, {}}}});
        ManualGroupingDriver da(std::move(inputs));
        shard_a = da.runPushed();
    }

    /// The fix makes shard A NOT advertise the empty bucket 1 as owed. (Pre-fix it does, which
    /// is what holds back shard B's real bucket 1 until the stream ends.)
    for (const auto & p : shard_a)
        EXPECT_EQ(std::ranges::count(p.ooo, 1), 0)
            << "shard A stamped empty bucket 1 as owed on its push of bucket " << p.bucket;

    std::vector<std::deque<Chunk>> outer(2);
    for (const auto & p : shard_a)
        outer[0].push_back(makeBucketChunk(p.bucket, p.ooo));
    for (Int32 b : {0, 1, 2})
        outer[1].push_back(makeBucketChunk(b, {}));

    ManualGroupingDriver d(std::move(outer));
    expectEachBucketOnce(d.run());
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
    ManualGroupingDriver d(std::move(inputs));
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

        ManualGroupingDriver d(std::move(outer_inputs));
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

/// =====================================================================================
/// THE MERGING LAYER (review follow-up).
///
/// Everything above models an intermediate distributed layer as
/// `GroupingAggregatedTransform` alone. That is only the first half of it. The real pipeline
/// built by `addMergingAggregatedMemoryEfficientTransform` is
///
///     GroupingAggregated -> Resize -> N x MergingAggregatedBucket -> SortingAggregated
///
/// whenever `num_merging_processors > 1`, which is the common case (it is
/// `aggregation_memory_efficient_merge_threads`, defaulting to `max_threads`). The N mergers
/// run concurrently and finish in an order unrelated to the order they started in, so:
///
///   1. `SortingAggregatedTransform` can emit a bucket AFTER a bigger one which
///      `GroupingAggregatedTransform` emitted EARLIER. That inversion did not exist upstream,
///      so no metadata stamped upstream describes it: if `GroupingAggregatedTransform` pushed
///      6 (delayed) and then 8, the stamp on 8 does not mention 6, because 6 was no longer
///      pending by then. Forwarding that stamp unchanged is therefore not enough.
///   2. So the delayed-bucket metadata has to be RE-DERIVED when this transform pushes,
///      from the latest announcement of each input minus what it has already pushed
///      (`SortingAggregatedTransform::getDelayedBucketsBefore`).
///
/// These tests drive that transform for real, with a randomized merger completion order.
/// =====================================================================================

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

    ManualGroupingDriver grouping(std::move(inputs));
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

        ManualGroupingDriver d(std::move(outer_inputs));
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
