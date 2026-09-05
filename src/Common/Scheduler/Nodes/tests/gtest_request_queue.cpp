#include <gtest/gtest.h>

#include <Common/Scheduler/Nodes/TimeShared/RequestQueue.h>
#include <Common/Scheduler/Nodes/WorkloadNode.h>
#include <Common/Scheduler/ResourceSchedulingContext.h>
#include <Common/Scheduler/EventQueue.h>
#include <Common/Scheduler/Nodes/tests/ResourceTest.h>
#include <Common/Stopwatch.h>

#include <deque>
#include <memory>
#include <optional>
#include <vector>

using namespace DB;

namespace
{

/// Minimal request that records its id (to assert ordering) and terminal callbacks.
struct TestRequest : public ResourceRequest
{
    int id = 0;
    int failed_count = 0;
    explicit TestRequest(int id_ = 0, ResourceCost cost_ = 1)
        : ResourceRequest(cost_)
        , id(id_)
    {}
    void execute() override {}
    void failed(const std::exception_ptr &) override { ++failed_count; }
};

/// Owns request objects (deque = stable addresses) and contexts; the queue is declared last so it
/// is destroyed first (its purgeQueue fails any still-pending request while requests are alive).
struct Fixture
{
    EventQueue event_queue;
    std::deque<TestRequest> pool;
    std::vector<ResourceSchedulingContextPtr> contexts;
    std::optional<RequestQueue> queue;

    explicit Fixture(SchedulerAlgorithm algo, CostUnit unit = CostUnit::IOByte, Int64 max_queued = std::numeric_limits<Int64>::max())
    {
        queue.emplace(event_queue, SchedulerNodeInfo{}, algo, unit, max_queued);
    }

    ResourceSchedulingContext * makeQuery(
        Float64 weight = 1.0, Float64 factor = 1.0, Float64 age_s = 0, Float64 cpu_s = 0, Float64 io_b = 0,
        UInt64 start_ns = 0, UInt64 priority = 0)
    {
        if (start_ns == 0)
            start_ns = clock_gettime_ns();
        contexts.push_back(std::make_shared<ResourceSchedulingContext>(start_ns, weight, factor, age_s, cpu_s, io_b, priority));
        return contexts.back().get();
    }

    TestRequest * enqueue(int id, ResourceSchedulingContext * ctx, ResourceCost cost = 1)
    {
        pool.emplace_back(id, cost);
        TestRequest * r = &pool.back();
        r->scheduling_context = ctx;
        queue->enqueueRequest(r);
        return r;
    }

    /// Dequeue all and return the sequence of request ids.
    std::vector<int> dequeueIds()
    {
        std::vector<int> ids;
        for (;;)
        {
            auto [req, _] = queue->dequeueRequest();
            if (!req)
                break;
            ids.push_back(static_cast<TestRequest *>(req)->id);
        }
        return ids;
    }

    /// Simulate `ResourceGuard::Request::finish()` feeding the real-vs-estimate error back for this
    /// query on this leaf (the leaf key is the RequestQueue itself, as the algorithms use `this`).
    void addCorrection(ResourceSchedulingContext * ctx, ResourceCost real, ResourceCost estimate)
    {
        ctx->getResourceState(&*queue).cost_correction.fetch_add(
            static_cast<Int64>(real) - static_cast<Int64>(estimate), std::memory_order_relaxed);
    }

    double vruntimeOf(ResourceSchedulingContext * ctx) { return ctx->getResourceState(&*queue).vruntime; }
    Int64 attainedOf(ResourceSchedulingContext * ctx) { return ctx->getResourceState(&*queue).attained_cost; }
};

}

/// fifo: first-come-first-served regardless of query identity.
TEST(RequestQueue, FifoOrder)
{
    Fixture f(SchedulerAlgorithm::Fifo);
    auto * q1 = f.makeQuery();
    auto * q2 = f.makeQuery();
    f.enqueue(1, q1);
    f.enqueue(2, nullptr); // anonymous
    f.enqueue(3, q2);
    f.enqueue(4, q1);
    EXPECT_EQ(f.dequeueIds(), (std::vector<int>{1, 2, 3, 4}));
}

/// fair, equal weights: two queries are interleaved fairly (SFQ round-robin).
TEST(RequestQueue, FairEqualWeightInterleave)
{
    Fixture f(SchedulerAlgorithm::Fair);
    auto * a = f.makeQuery(1.0);
    auto * b = f.makeQuery(1.0);
    f.enqueue(11, a);
    f.enqueue(21, b);
    f.enqueue(12, a);
    f.enqueue(22, b);
    // A1, B1, A2, B2 — each query advances its vruntime by cost/weight.
    EXPECT_EQ(f.dequeueIds(), (std::vector<int>{11, 21, 12, 22}));
}

/// fair, single query: requests stay FIFO.
TEST(RequestQueue, FairSingleQueryFifo)
{
    Fixture f(SchedulerAlgorithm::Fair);
    auto * a = f.makeQuery(1.0);
    f.enqueue(1, a);
    f.enqueue(2, a);
    f.enqueue(3, a);
    EXPECT_EQ(f.dequeueIds(), (std::vector<int>{1, 2, 3}));
}

/// The real-vs-estimate cost correction: clamps to a non-negative charge (never moving a key
/// backward) and carries any unspent refund forward so it converges to real cost long-term.
TEST(RequestQueue, ConsumeCorrectedCostClampsAndCarries)
{
    ResourceSchedulingContext ctx(0, 1.0, 1.0, 0, 0, 0, 0);
    int dummy_leaf = 0;
    auto & s = ctx.getResourceState(&dummy_leaf);
    EXPECT_EQ(s.consumeCorrectedCost(100), 100);          // no correction → identity
    s.cost_correction.fetch_add(50);
    EXPECT_EQ(s.consumeCorrectedCost(100), 150);          // under-estimate → charge extra
    EXPECT_EQ(s.cost_correction.load(), 0);
    s.cost_correction.fetch_add(-30);
    EXPECT_EQ(s.consumeCorrectedCost(100), 70);           // over-estimate → charge less
    EXPECT_EQ(s.cost_correction.load(), 0);
    s.cost_correction.fetch_add(-150);
    EXPECT_EQ(s.consumeCorrectedCost(100), 0);            // big refund → clamp to 0 (never negative)
    EXPECT_EQ(s.cost_correction.load(), -50);             // carry the unspent -50 forward
    EXPECT_EQ(s.consumeCorrectedCost(100), 50);           // applied to the next request
    EXPECT_EQ(s.cost_correction.load(), 0);
}

/// fair: a query whose first request under-estimated its cost has the shortfall folded into its
/// NEXT request's vruntime advance — never rewriting the already-served key, never going backward.
TEST(RequestQueue, FairAppliesCostCorrection)
{
    Fixture f(SchedulerAlgorithm::Fair);
    auto * a = f.makeQuery(1.0);
    f.enqueue(1, a, 10);
    EXPECT_EQ(f.dequeueIds(), (std::vector<int>{1}));
    double vr1 = f.vruntimeOf(a);                         // advanced by the estimate (10)
    f.addCorrection(a, /*real=*/100, /*estimate=*/10);    // request 1 really cost 100
    f.enqueue(2, a, 10);
    EXPECT_EQ(f.dequeueIds(), (std::vector<int>{2}));
    double vr2 = f.vruntimeOf(a);
    EXPECT_GE(vr2, vr1);                                  // never backward
    EXPECT_DOUBLE_EQ(vr2 - vr1, 100.0);                  // charged the corrected cost (10 + 90), not 10
}

/// las: the same correction folds into the query's attained service (its level key), so LAS tracks
/// real bytes/CPU rather than the estimate; attained only ever grows (level never drops).
TEST(RequestQueue, LasAppliesCostCorrection)
{
    Fixture f(SchedulerAlgorithm::Las);
    auto * a = f.makeQuery();
    f.enqueue(1, a, 10);
    EXPECT_EQ(f.dequeueIds(), (std::vector<int>{1}));
    Int64 att1 = f.attainedOf(a);                         // 10 (the estimate)
    f.addCorrection(a, /*real=*/100, /*estimate=*/10);
    f.enqueue(2, a, 10);
    EXPECT_EQ(f.dequeueIds(), (std::vector<int>{2}));
    Int64 att2 = f.attainedOf(a);
    EXPECT_GE(att2, att1);                                // never backward
    EXPECT_EQ(att2 - att1, 100);                          // attained advanced by the corrected cost
}

/// The weight-lowering threshold reads the query's real service (attained + pending correction) with
/// no one-request lag: a correction that pushes real service over the threshold lowers the weight on
/// the very next request, not the one after.
TEST(RequestQueue, FairThresholdSeesPendingCorrectionImmediately)
{
    Fixture f(SchedulerAlgorithm::Fair);
    // weight 1.0, halve it once attained IO >= 50 bytes.
    auto * a = f.makeQuery(/*weight=*/1.0, /*factor=*/0.5, /*age_s=*/0, /*cpu_s=*/0, /*io_b=*/50);
    f.enqueue(1, a, 10);
    EXPECT_EQ(f.dequeueIds(), (std::vector<int>{1}));
    double vr1 = f.vruntimeOf(a);                         // 10: full weight, not yet over threshold
    // Request 1 really moved 100 bytes: its real service (10 + 90) now exceeds 50, so request 2's
    // effective weight must already be lowered (0.5) — the pending correction is peeked, not lagged.
    f.addCorrection(a, /*real=*/100, /*estimate=*/10);
    f.enqueue(2, a, 10);
    EXPECT_EQ(f.dequeueIds(), (std::vector<int>{2}));
    double vr2 = f.vruntimeOf(a);
    // charge = 10 + 90 = 100; at the lowered weight 0.5 the advance is 100 / 0.5 = 200 (it would be
    // 100 if the threshold had not yet seen the correction).
    EXPECT_DOUBLE_EQ(vr2 - vr1, 200.0);
}

/// fair, unequal weights: the heavier query gets a larger share.
TEST(RequestQueue, FairWeighted)
{
    Fixture f(SchedulerAlgorithm::Fair);
    auto * heavy = f.makeQuery(2.0);
    auto * light = f.makeQuery(1.0);
    f.enqueue(1, heavy);
    f.enqueue(2, heavy);
    f.enqueue(3, heavy);
    f.enqueue(9, light);
    // heavy advances vruntime by 0.5/request, light by 1.0 → heavy is served more often.
    EXPECT_EQ(f.dequeueIds(), (std::vector<int>{1, 9, 2, 3}));
}

/// fair, weight lowering by age: an old query is deprioritised versus a fresh one.
TEST(RequestQueue, FairWeightLoweringByAge)
{
    Fixture f(SchedulerAlgorithm::Fair);
    UInt64 now = clock_gettime_ns();
    // Old query: started 100s ago, age threshold 10s, factor 0.01 → weight is strongly lowered.
    auto * old_q = f.makeQuery(/*weight*/ 1.0, /*factor*/ 0.01, /*age_s*/ 10, 0, 0, /*start_ns*/ now - 100'000'000'000ULL);
    auto * new_q = f.makeQuery(/*weight*/ 1.0, /*factor*/ 1.0, /*age_s*/ 10, 0, 0, /*start_ns*/ now);
    f.enqueue(101, old_q);
    f.enqueue(201, new_q);
    f.enqueue(102, old_q);
    f.enqueue(202, new_q);
    // First requests both start at vtime 0; then the old query's vruntime jumps far ahead
    // (cost/0.01), so its second request is served last.
    EXPECT_EQ(f.dequeueIds(), (std::vector<int>{101, 201, 202, 102}));
}

/// fair, weight lowering by attained IO bytes: once a query has attained enough IO its weight is
/// lowered, deprioritising its later requests versus a fresh query. Attained-service thresholds
/// (unlike age) only take effect after the query has been served, so the queue is driven
/// pop-then-push here.
TEST(RequestQueue, FairWeightLoweringByIoBytes)
{
    Fixture f(SchedulerAlgorithm::Fair); // IOByte
    auto * heavy = f.makeQuery(/*weight*/ 1.0, /*factor*/ 0.5, /*age_s*/ 0, /*cpu_s*/ 0, /*io_b*/ 1);
    auto * fresh = f.makeQuery(/*weight*/ 1.0, /*factor*/ 1.0, 0, 0, 0);
    // Serve one 1-byte request from `heavy` so its attained IO (1) reaches the threshold; from here
    // its effective weight is halved (0.5), so its virtual runtime advances twice as fast.
    f.enqueue(0, heavy, 1);
    ASSERT_EQ(f.dequeueIds(), (std::vector<int>{0}));
    f.enqueue(1, heavy, 1); // H1
    f.enqueue(2, heavy, 1); // H2
    f.enqueue(3, fresh, 1); // F1
    f.enqueue(4, fresh, 1); // F2
    f.enqueue(5, fresh, 1); // F3
    // The halved weight pushes heavy's second post-threshold request (id 2) to the back; without
    // the lowering it would land ahead of F3 (id 5).
    EXPECT_EQ(f.dequeueIds(), (std::vector<int>{3, 1, 4, 5, 2}));
}

/// fair, three equal-weight queries are interleaved round-robin.
TEST(RequestQueue, FairEqualWeightThreeQueries)
{
    Fixture f(SchedulerAlgorithm::Fair);
    auto * a = f.makeQuery(1.0);
    auto * b = f.makeQuery(1.0);
    auto * c = f.makeQuery(1.0);
    f.enqueue(11, a); f.enqueue(21, b); f.enqueue(31, c);
    f.enqueue(12, a); f.enqueue(22, b); f.enqueue(32, c);
    EXPECT_EQ(f.dequeueIds(), (std::vector<int>{11, 21, 31, 12, 22, 32}));
}

/// las: the query that has attained the least service is served first.
TEST(RequestQueue, LasFavoursLeastAttained)
{
    Fixture f(SchedulerAlgorithm::Las); // IOByte, base = 1 MiB
    auto * a = f.makeQuery();
    auto * b = f.makeQuery();
    const ResourceCost big = 4 * 1024 * 1024; // > base, so the level rises after one service

    f.enqueue(1, a, big);
    // Serve A once so it accrues attained service (moving it to a higher MLFQ level).
    {
        auto [r, _] = f.queue->dequeueRequest();
        ASSERT_TRUE(r);
        EXPECT_EQ(static_cast<TestRequest *>(r)->id, 1);
    }
    f.enqueue(2, a, big); // A: attained ~4 MiB → level >= 1
    f.enqueue(3, b, big); // B: attained 0 → level 0
    // B (least attained) is served before A's next request.
    EXPECT_EQ(f.dequeueIds(), (std::vector<int>{3, 2}));
}

/// las re-keys a stale request on pop: when a query has several requests queued at once, serving
/// one advances its attained service, so its remaining requests are deferred to their true (higher)
/// level instead of being served ahead of a lower-attained query. This is the IO / multi-request
/// case (concurrent socket ops have independent requests); with one request per query (CPU via
/// CPULeaseAllocation) it is a no-op.
TEST(RequestQueue, LasRekeysStaleRequestsOnPop)
{
    Fixture f(SchedulerAlgorithm::Las); // IOByte, base = 1 MiB
    auto * a = f.makeQuery();
    auto * b = f.makeQuery();
    const ResourceCost mib = 1'048'576; // one base quantum → serving one lifts A to level 1
    f.enqueue(1, a, mib); // A1, A2, A3 all queued while A is at level 0
    f.enqueue(2, a, mib);
    f.enqueue(3, a, mib);
    f.enqueue(9, b, 1);   // B1 (level 0)
    // A1 served → A.attained = 1 MiB → level 1. A2/A3 are now stale (stored level 0); they are
    // re-keyed to level 1 on pop, so B1 (still level 0) is served before them. Without the recheck
    // the order would be the stale {1, 2, 3, 9}.
    EXPECT_EQ(f.dequeueIds(), (std::vector<int>{1, 9, 2, 3}));
}

/// priority: strict order by the query `priority` setting (1 = highest; 0 = none = lowest).
TEST(RequestQueue, PriorityStrictOrder)
{
    Fixture f(SchedulerAlgorithm::Priority);
    auto * p2 = f.makeQuery(1.0, 1.0, 0, 0, 0, 0, /*priority*/ 2);
    auto * p1 = f.makeQuery(1.0, 1.0, 0, 0, 0, 0, /*priority*/ 1);
    auto * p0 = f.makeQuery(1.0, 1.0, 0, 0, 0, 0, /*priority*/ 0); // no priority → lowest
    f.enqueue(20, p2);
    f.enqueue(0, p0);
    f.enqueue(10, p1);
    f.enqueue(21, p2);
    // priority 1 first, then the priority-2 pair (FIFO within), then the priority-0 query last.
    EXPECT_EQ(f.dequeueIds(), (std::vector<int>{10, 20, 21, 0}));
}

/// priority ordering is exact for large values: an integer `Priority` key keeps two priorities that
/// differ only above 2^53 distinct, whereas a double key would collapse them to one value (→ FIFO).
TEST(RequestQueue, PriorityLargeValuesOrderExactly)
{
    Fixture f(SchedulerAlgorithm::Priority);
    const UInt64 p_lo = (1ULL << 53);      // 9007199254740992
    const UInt64 p_hi = (1ULL << 53) + 1;  // 9007199254740993 — equal to p_lo once cast to double
    auto * higher_prec = f.makeQuery(1.0, 1.0, 0, 0, 0, 0, /*priority*/ p_lo); // lower value = higher precedence
    auto * lower_prec = f.makeQuery(1.0, 1.0, 0, 0, 0, 0, /*priority*/ p_hi);
    f.enqueue(2, lower_prec); // enqueue the lower-precedence (larger value) request first
    f.enqueue(1, higher_prec);
    // p_lo < p_hi, so id 1 is served first despite arriving second. A double key would tie the two
    // and fall back to FIFO ({2, 1}).
    EXPECT_EQ(f.dequeueIds(), (std::vector<int>{1, 2}));
}

/// "no priority" (`priority = 0`) sorts strictly after every explicit priority — even the maximum
/// representable one — instead of colliding with it (both mapping to the max key → FIFO).
TEST(RequestQueue, PriorityNoPrioritySortsLast)
{
    Fixture f(SchedulerAlgorithm::Priority);
    const UInt64 huge = static_cast<UInt64>(std::numeric_limits<Int64>::max()); // extreme explicit priority
    auto * no_priority = f.makeQuery(1.0, 1.0, 0, 0, 0, 0, /*priority*/ 0);
    auto * explicit_low = f.makeQuery(1.0, 1.0, 0, 0, 0, 0, /*priority*/ huge);
    f.enqueue(1, no_priority);   // enqueue "no priority" first
    f.enqueue(2, explicit_low);
    // The explicit priority (even INT64_MAX) outranks "no priority", so id 2 is served first.
    EXPECT_EQ(f.dequeueIds(), (std::vector<int>{2, 1}));
}

/// The swap hook migrates all pending requests to the new algorithm; none are lost, and the
/// node identity is unchanged.
TEST(RequestQueue, SwapSchedulerKeepsRequests)
{
    Fixture f(SchedulerAlgorithm::Fifo);
    auto * a = f.makeQuery();
    for (int i = 1; i <= 5; ++i)
        f.enqueue(i, a);
    EXPECT_EQ(f.queue->getScheduler(), SchedulerAlgorithm::Fifo);

    f.queue->setScheduler(SchedulerAlgorithm::Fair);
    EXPECT_EQ(f.queue->getScheduler(), SchedulerAlgorithm::Fair);

    auto [len, cost] = f.queue->getQueueLengthAndCost();
    EXPECT_EQ(len, 5u);
    EXPECT_EQ(cost, 5);
    // All five requests are still there and dequeue (single query → FIFO under fair too).
    EXPECT_EQ(f.dequeueIds(), (std::vector<int>{1, 2, 3, 4, 5}));
}

/// Toggling a fair leaf's algorithm away and back must not double-count the pending backlog's
/// projected virtual runtime: the migrated queries' vruntime is reset on swap-into-fair, so a query
/// is not pushed behind a fresh one by a doubled projection.
TEST(RequestQueue, FairSwapRoundTripNoVruntimeDoubleCount)
{
    Fixture f(SchedulerAlgorithm::Fair);
    auto * a = f.makeQuery(1.0);
    auto * b = f.makeQuery(1.0);
    f.enqueue(1, a); // A1
    f.enqueue(2, a); // A2 (A's projected vruntime is now 2)
    // Swap the leaf's algorithm away from fair and back while A's requests are still queued.
    f.queue->setScheduler(SchedulerAlgorithm::Priority);
    f.queue->setScheduler(SchedulerAlgorithm::Fair);
    f.enqueue(3, b); // B1, a fresh query
    // With the vruntime reset A's backlog is re-projected once (A1 at vstart 0), so A1 leads the
    // fresh B1 and A2 follows. Without it, A's doubled vruntime would push both A1 and A2 behind
    // B1 (i.e. {3, 1, 2}).
    EXPECT_EQ(f.dequeueIds(), (std::vector<int>{1, 3, 2}));
}

/// A correction consumed by `fair::push` (folded into `scheduling_charge`) must survive a live swap
/// to `las`. `fair` charges at push and stores the corrected cost on the request; `las` charges at
/// pop and re-derives it from `scheduling_cost` + the shared `cost_correction`, ignoring
/// `scheduling_charge`. `setScheduler` returns the consumed delta to `cost_correction` before
/// re-pushing, so the migrated request's real cost still lands in attained under `las` — not just
/// the estimate. Regression for the swap silently dropping the correction.
TEST(RequestQueue, FairToLasSwapPreservesCorrection)
{
    Fixture f(SchedulerAlgorithm::Fair);
    auto * a = f.makeQuery();
    f.addCorrection(a, /*real=*/100, /*estimate=*/10);   // +90 pending on the shared state
    f.enqueue(1, a, 10);                                 // fair::push consumes it into scheduling_charge (=100)
    f.queue->setScheduler(SchedulerAlgorithm::Las);      // migrate the pending request to las
    EXPECT_EQ(f.dequeueIds(), (std::vector<int>{1}));    // popped under las
    EXPECT_EQ(f.attainedOf(a), 100);                     // corrected cost preserved across the swap, not 10
}

/// las: the lazy pop-time re-key must see a query's REAL service (attained + pending correction), not
/// just attained_cost. A query that badly under-estimated a finished request has a large pending
/// correction; its next request must be re-keyed to its true (higher) level and deferred behind a
/// genuinely lighter query, not run ahead of it. Regression for the re-key ignoring cost_correction.
TEST(RequestQueue, LasRekeySeesPendingCorrection)
{
    Fixture f(SchedulerAlgorithm::Las);
    auto * a = f.makeQuery();
    auto * c = f.makeQuery();
    f.enqueue(1, a, 10);   // A: enqueued first, keyed at level(attained=0)
    f.enqueue(2, c, 10);   // C: keyed at level(attained=0); ties with A → A sorts first by seq
    // A's finished request really cost far more than estimated → big pending correction on A.
    f.addCorrection(a, /*real=*/64 * 1024 * 1024, /*estimate=*/10);
    // With the fix A's front request re-keys to its true (high) level and defers, so C (truly
    // least-attained) is served first; without it A would pop first on the level-0 seq tie.
    EXPECT_EQ(f.dequeueIds(), (std::vector<int>{2, 1}));
}

/// Cancellation from the middle works for both algorithms; counters update.
TEST(RequestQueue, CancelFromMiddle)
{
    for (auto algo : {SchedulerAlgorithm::Fifo, SchedulerAlgorithm::Fair, SchedulerAlgorithm::Las})
    {
        Fixture f(algo);
        auto * a = f.makeQuery();
        auto * r1 = f.enqueue(1, a);
        auto * r2 = f.enqueue(2, a);
        auto * r3 = f.enqueue(3, a);
        EXPECT_TRUE(f.queue->cancelRequest(r2));
        EXPECT_EQ(f.queue->canceled_requests.load(), 1u);
        EXPECT_EQ(f.dequeueIds(), (std::vector<int>{1, 3}));
        EXPECT_FALSE(f.queue->cancelRequest(r1)); // already dequeued
        (void)r3;
    }
}

/// max_waiting_queries is enforced on the total pending count for both algorithms.
TEST(RequestQueue, MaxWaitingQueries)
{
    for (auto algo : {SchedulerAlgorithm::Fifo, SchedulerAlgorithm::Fair, SchedulerAlgorithm::Las})
    {
        Fixture f(algo, CostUnit::IOByte, /*max_queued*/ 2);
        auto * a = f.makeQuery();
        f.enqueue(1, a);
        f.enqueue(2, a);
        TestRequest overflow(3);
        overflow.scheduling_context = a;
        EXPECT_THROW(f.queue->enqueueRequest(&overflow), DB::Exception);
        EXPECT_EQ(f.queue->rejected_requests.load(), 1u);
    }
}

/// purge fails all pending requests and rejects new ones.
TEST(RequestQueue, Purge)
{
    Fixture f(SchedulerAlgorithm::Fair);
    auto * a = f.makeQuery();
    auto * r1 = f.enqueue(1, a);
    auto * r2 = f.enqueue(2, nullptr);
    f.queue->purgeQueue();
    EXPECT_EQ(r1->failed_count, 1);
    EXPECT_EQ(r2->failed_count, 1);
    TestRequest r3(3);
    EXPECT_THROW(f.queue->enqueueRequest(&r3), DB::Exception);
}

/// reset() clears the per-request scheduling state so a reused request carries nothing stale.
TEST(RequestQueue, ResetClearsSchedulingState)
{
    auto ctx = std::make_shared<ResourceSchedulingContext>(clock_gettime_ns(), 1.0, 1.0, 0, 0, 0, 0);
    TestRequest r(1, 5);
    r.scheduling_context = ctx.get();
    r.scheduling_key = {42.0, 7};
    r.scheduling_priority = Priority{123};
    r.reset(9);
    EXPECT_EQ(r.scheduling_context, nullptr);
    EXPECT_EQ(r.scheduling_key.first, 0.0);
    EXPECT_EQ(r.scheduling_key.second, 0u);
    EXPECT_EQ(r.scheduling_priority.value, 0);
    EXPECT_EQ(r.cost, 9);
}

/// Node-level: the leaf is always a RequestQueue (type "request_queue", basename "queue"); the
/// workload `scheduler` setting selects the algorithm and a change swaps it in place.
TEST(RequestQueue, NodeTypeAndSchedulerSetting)
{
    ResourceTestClass t;

    WorkloadSettings def; // scheduler defaults to "fifo"
    auto node = t.createUnifiedNode("all", def);
    auto * queue = node->getLink().queue;
    EXPECT_EQ(String(queue->getTypeName()), "request_queue");

    WorkloadSettings fair;
    fair.scheduler = "fair";
    t.updateUnifiedNode(node, nullptr, nullptr, fair);
    // Same node object (in-place swap), still a request_queue.
    EXPECT_EQ(node->getLink().queue, queue);
    EXPECT_EQ(String(node->getLink().queue->getTypeName()), "request_queue");
}

/// weight_lowering_factor is clamped to [0, 1] so it can only lower a query's weight: a factor > 1
/// (which would otherwise RAISE the share and invert the setting) is capped at 1.0, and a negative
/// factor is floored at 0.0. A non-positive base weight falls back to 1.0.
TEST(RequestQueue, WeightLoweringFactorClamped)
{
    ResourceSchedulingContext raised(clock_gettime_ns(), 1.0, 10.0, 0, 0, 0, 0);
    EXPECT_EQ(raised.weight_lowering_factor, 1.0);
    ResourceSchedulingContext negative(clock_gettime_ns(), 1.0, -5.0, 0, 0, 0, 0);
    EXPECT_EQ(negative.weight_lowering_factor, 0.0);
    ResourceSchedulingContext normal(clock_gettime_ns(), 1.0, 0.25, 0, 0, 0, 0);
    EXPECT_EQ(normal.weight_lowering_factor, 0.25);
    ResourceSchedulingContext zero_weight(clock_gettime_ns(), 0.0, 1.0, 0, 0, 0, 0);
    EXPECT_EQ(zero_weight.weight, 1.0);
    ResourceSchedulingContext negative_weight(clock_gettime_ns(), -3.0, 1.0, 0, 0, 0, 0);
    EXPECT_EQ(negative_weight.weight, 1.0);
}

TEST(RequestQueue, WeightLoweringThresholdsClampNegativeToDisabled)
{
    // A negative lowering threshold is meaningless and is clamped to 0 (disabled), rather than
    // stored as-is and only read as disabled by the `> 0` checks in effectiveWeight().
    ResourceSchedulingContext negative(clock_gettime_ns(), 1.0, 0.5, -1.0, -2.0, -3.0, 0);
    EXPECT_EQ(negative.weight_lowering_age_seconds, 0.0);
    EXPECT_EQ(negative.weight_lowering_cpu_seconds, 0.0);
    EXPECT_EQ(negative.weight_lowering_io_bytes, 0.0);
    // Positive thresholds are preserved verbatim.
    ResourceSchedulingContext positive(clock_gettime_ns(), 1.0, 0.5, 3.0, 4.0, 5.0, 0);
    EXPECT_EQ(positive.weight_lowering_age_seconds, 3.0);
    EXPECT_EQ(positive.weight_lowering_cpu_seconds, 4.0);
    EXPECT_EQ(positive.weight_lowering_io_bytes, 5.0);
}

/// `fair` accounts virtual runtime from `scheduling_cost` (the query's DECLARED cost), not `cost`
/// (which ResourceBudget::ask rewrites queue-wide). Simulate a budget that inflated query A's first
/// request `cost` far above its declared cost: A must still interleave fairly with B instead of
/// being pushed to the back by another query's estimation error bleeding through the shared budget.
TEST(RequestQueue, FairUsesSchedulingCostNotBudgetAdjustedCost)
{
    Fixture f(SchedulerAlgorithm::Fair);
    auto * a = f.makeQuery(1.0);
    auto * b = f.makeQuery(1.0);

    // A1: declared cost 1 (so scheduling_cost == 1), but `cost` inflated to 1000 as if
    // ResourceBudget::ask() had rewritten it queue-wide after reset().
    f.pool.emplace_back(11, 1);
    TestRequest * a1 = &f.pool.back();
    a1->scheduling_context = a;
    a1->cost = 1000;
    f.queue->enqueueRequest(a1);

    f.enqueue(21, b);
    f.enqueue(12, a);
    f.enqueue(22, b);
    // With scheduling_cost (=1) A and B interleave fairly: 11,21,12,22.
    // Had fair used `cost` (=1000), A's vruntime would jump and A2 would be served last: 11,21,22,12.
    EXPECT_EQ(f.dequeueIds(), (std::vector<int>{11, 21, 12, 22}));
}
