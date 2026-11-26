/// Throughput of the time-shared scheduler runtime: enqueue, dequeue on the scheduler thread, finish.
///
/// Every case builds one hierarchy under a `TimeSharedScheduler` and reports `requests_per_second`. Requests
/// cost 1, and each consumer or pending request is a query of its own (`ResourceSchedulingContext`).
/// Constraints are set so they never throttle: the rate is the cost of the nodes, not of a limit.
///
/// Modes:
///   - `Dequeue`: the benchmark thread is the scheduler thread, so there is no hand-off between threads.
///     Every leaf keeps `depth` pending requests. Each iteration dequeues one request from the root,
///     finishes it, enqueues it again into its leaf and processes the events. With `depth:1` every
///     request empties its leaf, so the deactivation and activation of the nodes above it run every time.
///   - `Threads`: the scheduler runs its own thread, and each of `threads` consumers loops on a
///     `ResourceGuard` (enqueue, wait for the grant, finish). Consumer `i` uses leaf `i % leaves`.
///
/// Hierarchies (`leaves` is the number of leaf queues):
///   - `queue_fifo`, `queue_fair`, `queue_las`, `queue_priority`: one `RequestQueue` with this algorithm.
///   - `resources`: `leaves` resources of one `RequestQueue` each, served round-robin by the scheduler.
///   - `fair`: `FairPolicy` over the queues.
///   - `priority`: `PriorityPolicy` over the queues, each with its own priority.
///   - `semaphore`: `SemaphoreConstraint` with `max_requests = 4` over a queue.
///   - `throttler`: `ThrottlerConstraint` over a queue.
///   - `workload`: one `TimeSharedWorkloadNode` without children.
///   - `workload_fair`: workload `all` with `leaves` children.
///   - `workload_priority`: workload `all` with `leaves` children, every second one with `priority = 1`.
///   - `workload_limits`: `workload_fair` with `max_io_requests = 4` and `max_bytes_per_second` on `all`.
///   - `production_development`: `all` with `max_io_requests = 100` over `production` with `weight = 3`
///     and `development` with `max_io_requests = 10`.
///
/// For example:
///   benchmark_time_shared_scheduler --benchmark_filter='^Threads/workload_fair/leaves:16/real_time/threads:16$'

#include <Common/Scheduler/Nodes/TimeShared/FairPolicy.h>
#include <Common/Scheduler/Nodes/TimeShared/PriorityPolicy.h>
#include <Common/Scheduler/Nodes/TimeShared/RequestQueue.h>
#include <Common/Scheduler/Nodes/TimeShared/SemaphoreConstraint.h>
#include <Common/Scheduler/Nodes/TimeShared/ThrottlerConstraint.h>
#include <Common/Scheduler/Nodes/TimeShared/TimeSharedScheduler.h>
#include <Common/Scheduler/Nodes/WorkloadNode.h>
#include <Common/Scheduler/ResourceGuard.h>
#include <Common/Scheduler/ResourceLink.h>
#include <Common/Scheduler/ResourceRequest.h>
#include <Common/Scheduler/ResourceSchedulingContext.h>
#include <Common/Scheduler/WorkloadSettings.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <benchmark/benchmark.h>
#include <fmt/format.h>

#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

using namespace DB;

namespace
{

constexpr ResourceCost request_cost = 1;

/// Speed and burst of a token bucket that never runs out at the rates measured here.
constexpr double unlimited_speed = 1e15;

using Leaves = std::vector<ISchedulerQueue *>;

/// Builds a hierarchy under the scheduler and returns its leaf queues.
using Build = Leaves (*)(TimeSharedScheduler & scheduler, size_t leaves);

template <typename Node, typename... Args>
std::shared_ptr<Node> makeNode(EventQueue & event_queue, const String & name, Args &&... args)
{
    auto node = std::make_shared<Node>(event_queue, std::forward<Args>(args)...);
    node->basename = name;
    return node;
}

ISchedulerQueue * addQueue(ISchedulerNode & parent, const String & name, const SchedulerNodeInfo & info = {},
    SchedulerAlgorithm algorithm = SchedulerAlgorithm::Fifo)
{
    auto queue = makeNode<RequestQueue>(parent.event_queue, name, info, algorithm);
    parent.attachChild(queue);
    return queue.get();
}

template <SchedulerAlgorithm algorithm>
Leaves singleQueue(TimeSharedScheduler & scheduler, size_t)
{
    return {addQueue(scheduler, "queue", {}, algorithm)};
}

Leaves resources(TimeSharedScheduler & scheduler, size_t leaves)
{
    Leaves result;
    for (size_t i = 0; i < leaves; ++i)
        result.push_back(addQueue(scheduler, fmt::format("resource{}", i)));
    return result;
}

Leaves fairPolicy(TimeSharedScheduler & scheduler, size_t leaves)
{
    auto root = makeNode<FairPolicy>(scheduler.event_queue, "fair");
    Leaves result;
    for (size_t i = 0; i < leaves; ++i)
        result.push_back(addQueue(*root, fmt::format("queue{}", i)));
    scheduler.attachChild(root);
    return result;
}

Leaves priorityPolicy(TimeSharedScheduler & scheduler, size_t leaves)
{
    auto root = makeNode<PriorityPolicy>(scheduler.event_queue, "priority");
    Leaves result;
    for (size_t i = 0; i < leaves; ++i)
        result.push_back(addQueue(*root, fmt::format("queue{}", i), SchedulerNodeInfo(1.0, Priority{static_cast<Int64>(i)})));
    scheduler.attachChild(root);
    return result;
}

Leaves semaphoreConstraint(TimeSharedScheduler & scheduler, size_t)
{
    auto root = makeNode<SemaphoreConstraint>(scheduler.event_queue, "semaphore", SchedulerNodeInfo{}, /* max_requests */ 4);
    Leaves result{addQueue(*root, "queue")};
    scheduler.attachChild(root);
    return result;
}

Leaves throttlerConstraint(TimeSharedScheduler & scheduler, size_t)
{
    auto root = makeNode<ThrottlerConstraint>(scheduler.event_queue, "throttler", SchedulerNodeInfo{}, unlimited_speed, unlimited_speed);
    Leaves result{addQueue(*root, "queue")};
    scheduler.attachChild(root);
    return result;
}

using WorkloadChildren = std::vector<std::pair<String, WorkloadSettings>>;

/// Workload `all` with the given children, as `WorkloadResourceManager` builds it for an IO resource.
Leaves workloads(TimeSharedScheduler & scheduler, const WorkloadSettings & all_settings, const WorkloadChildren & children)
{
    auto all = makeNode<TimeSharedWorkloadNode>(scheduler.event_queue, "all", all_settings, CostUnit::IOByte, "io");
    Leaves result;
    for (const auto & [name, settings] : children)
    {
        auto child = makeNode<TimeSharedWorkloadNode>(scheduler.event_queue, name, settings, CostUnit::IOByte, "io");
        all->attachWorkloadChild(child);
        result.push_back(child->getLink().queue);
    }
    if (children.empty())
        result.push_back(all->getLink().queue);
    scheduler.attachChild(all);
    return result;
}

WorkloadChildren equalChildren(size_t leaves)
{
    WorkloadChildren result;
    for (size_t i = 0; i < leaves; ++i)
        result.emplace_back(fmt::format("child{}", i), WorkloadSettings{});
    return result;
}

Leaves workload(TimeSharedScheduler & scheduler, size_t)
{
    return workloads(scheduler, {}, {});
}

Leaves workloadFair(TimeSharedScheduler & scheduler, size_t leaves)
{
    return workloads(scheduler, {}, equalChildren(leaves));
}

Leaves workloadPriority(TimeSharedScheduler & scheduler, size_t leaves)
{
    WorkloadChildren children = equalChildren(leaves);
    for (size_t i = 1; i < leaves; i += 2)
        children[i].second.priority = Priority{1};
    return workloads(scheduler, {}, children);
}

Leaves workloadLimits(TimeSharedScheduler & scheduler, size_t leaves)
{
    WorkloadSettings all;
    all.max_io_requests = 4;
    all.max_bytes_per_second = unlimited_speed;
    all.max_burst_bytes = unlimited_speed;
    return workloads(scheduler, all, equalChildren(leaves));
}

Leaves productionDevelopment(TimeSharedScheduler & scheduler, size_t)
{
    WorkloadSettings all;
    all.max_io_requests = 100;
    WorkloadSettings production;
    production.weight = 3;
    WorkloadSettings development;
    development.max_io_requests = 10;
    return workloads(scheduler, all, {{"production", production}, {"development", development}});
}

std::unique_ptr<ResourceSchedulingContext> makeQuery()
{
    auto query = std::make_unique<ResourceSchedulingContext>(
        clock_gettime_ns(), /* weight */ 1.0, /* weight_lowering_factor */ 1.0, 0.0, 0.0, 0.0, Priority{});
    query->initResourceStates(1);
    return query;
}

/// A link to `queue` on behalf of `query`, as a workload classifier makes it.
ResourceLink makeLink(ISchedulerQueue * queue, ResourceSchedulingContext & query)
{
    return {.queue = queue, .scheduling_context = &query, .scheduling_state = query.resourceState(0)};
}

void processEvents(EventQueue & events)
{
    while (events.tryProcess())
    {
    }
}

struct Request final : ResourceRequest
{
    std::unique_ptr<ResourceSchedulingContext> query = makeQuery();
    ResourceLink link;

    explicit Request(ISchedulerQueue * queue)
        : ResourceRequest(request_cost)
        , link(makeLink(queue, *query))
    {
    }

    void execute() override {}
    void failed(const std::exception_ptr &) override {}
};

/// Args: leaves, depth.
void Dequeue(benchmark::State & state, Build build)
{
    const auto leaves = static_cast<size_t>(state.range(0));
    const auto depth = static_cast<size_t>(state.range(1));

    /// Declared first, so they outlive the scheduler, which fails the requests still pending when destroyed.
    std::vector<std::unique_ptr<Request>> requests;
    TimeSharedScheduler scheduler;
    EventQueue::SchedulerThread scheduler_thread(&scheduler.event_queue);

    for (ISchedulerQueue * leaf : build(scheduler, leaves))
    {
        for (size_t i = 0; i < depth; ++i)
        {
            const auto & request = requests.emplace_back(std::make_unique<Request>(leaf));
            request->link.enqueue(request.get());
        }
    }
    processEvents(scheduler.event_queue);

    for (auto _ [[maybe_unused]] : state)
    {
        ResourceRequest * dequeued = scheduler.dequeueRequest().first;
        if (!dequeued)
        {
            state.SkipWithError("the scheduler has no request to dequeue");
            break;
        }
        auto & request = static_cast<Request &>(*dequeued);
        request.execute();
        request.finish();
        request.reset(request_cost);
        request.link.enqueue(&request);
        processEvents(scheduler.event_queue);
    }

    state.counters["requests_per_second"] = benchmark::Counter(static_cast<double>(state.iterations()), benchmark::Counter::kIsRate);
}

/// State shared by the consumers of one `Threads` case. Thread 0 sets it up before any thread enters
/// the measured loop, and tears it down after all of them have left it.
struct Run
{
    std::unique_ptr<TimeSharedScheduler> scheduler;
    Leaves leaves;
    std::vector<std::unique_ptr<ResourceSchedulingContext>> queries;
};

Run run;

/// Args: leaves.
void Threads(benchmark::State & state, Build build)
{
    const auto leaves = static_cast<size_t>(state.range(0));
    const auto consumer = static_cast<size_t>(state.thread_index());

    if (consumer == 0)
    {
        static std::once_flag global_pool_initialized;
        std::call_once(global_pool_initialized, [] { GlobalThreadPool::initialize(); });

        run.scheduler = std::make_unique<TimeSharedScheduler>();
        run.scheduler->start(ThreadName::TEST_SCHEDULER);

        /// The hierarchy is built on the scheduler thread, as `WorkloadResourceManager` does.
        std::promise<Leaves> built;
        run.scheduler->event_queue.enqueue([&]
        {
            try
            {
                built.set_value(build(*run.scheduler, leaves));
            }
            catch (...)
            {
                built.set_exception(std::current_exception());
            }
        });
        run.leaves = built.get_future().get();

        for (int i = 0; i < state.threads(); ++i)
            run.queries.push_back(makeQuery());
    }

    /// Set on the first iteration: the setup by thread 0 is complete only once the loop has started.
    ResourceLink link;
    for (auto _ [[maybe_unused]] : state)
    {
        if (!link) [[unlikely]]
            link = makeLink(run.leaves[consumer % run.leaves.size()], *run.queries[consumer]);
        ResourceGuard guard(ResourceGuard::Metrics::getIOWrite(), link, request_cost);
        guard.unlock(request_cost);
    }

    state.counters["requests_per_second"] = benchmark::Counter(static_cast<double>(state.iterations()), benchmark::Counter::kIsRate);

    if (consumer == 0)
    {
        run.scheduler->stop();
        run.scheduler.reset();
        run.leaves.clear();
        run.queries.clear();
    }
}

struct Hierarchy
{
    std::string name;
    Build build;
    std::vector<int64_t> leaves;
};

[[maybe_unused]] const bool registered = []
{
    const std::vector<Hierarchy> hierarchies{
        {"queue_fifo", singleQueue<SchedulerAlgorithm::Fifo>, {1}},
        {"queue_fair", singleQueue<SchedulerAlgorithm::Fair>, {1}},
        {"queue_las", singleQueue<SchedulerAlgorithm::Las>, {1}},
        {"queue_priority", singleQueue<SchedulerAlgorithm::Priority>, {1}},
        {"resources", resources, {2, 16}},
        {"fair", fairPolicy, {2, 16, 256}},
        {"priority", priorityPolicy, {2, 16}},
        {"semaphore", semaphoreConstraint, {1}},
        {"throttler", throttlerConstraint, {1}},
        {"workload", workload, {1}},
        {"workload_fair", workloadFair, {2, 16, 256}},
        {"workload_priority", workloadPriority, {2, 16}},
        {"workload_limits", workloadLimits, {2, 16}},
        {"production_development", productionDevelopment, {2}},
    };
    for (const auto & hierarchy : hierarchies)
    {
        benchmark::RegisterBenchmark("Dequeue/" + hierarchy.name, &Dequeue, hierarchy.build)
            ->ArgNames({"leaves", "depth"})
            ->ArgsProduct({hierarchy.leaves, {1, 16}})
            ->UseRealTime();
        benchmark::RegisterBenchmark("Threads/" + hierarchy.name, &Threads, hierarchy.build)
            ->ArgNames({"leaves"})
            ->ArgsProduct({hierarchy.leaves})
            ->ThreadRange(1, 64, 4)
            ->UseRealTime();
    }
    return true;
}();

}
