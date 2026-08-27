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
        Float64 weight = 1.0, Float64 factor = 1.0, Float64 age_s = 0, Float64 cpu_s = 0, Float64 io_b = 0, UInt64 start_ns = 0)
    {
        if (start_ns == 0)
            start_ns = clock_gettime_ns();
        contexts.push_back(std::make_shared<ResourceSchedulingContext>(start_ns, weight, factor, age_s, cpu_s, io_b));
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
        while (auto [req, _] = queue->dequeueRequest(); req)
            ids.push_back(static_cast<TestRequest *>(req)->id);
        return ids;
    }
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

/// Cancellation from the middle works for both algorithms; counters update.
TEST(RequestQueue, CancelFromMiddle)
{
    for (auto algo : {SchedulerAlgorithm::Fifo, SchedulerAlgorithm::Fair})
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
    for (auto algo : {SchedulerAlgorithm::Fifo, SchedulerAlgorithm::Fair})
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
    auto ctx = std::make_shared<ResourceSchedulingContext>(clock_gettime_ns(), 1.0, 1.0, 0, 0, 0);
    TestRequest r(1, 5);
    r.scheduling_context = ctx.get();
    r.scheduling_key = {42.0, 7};
    r.reset(9);
    EXPECT_EQ(r.scheduling_context, nullptr);
    EXPECT_EQ(r.scheduling_key.first, 0.0);
    EXPECT_EQ(r.scheduling_key.second, 0u);
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
