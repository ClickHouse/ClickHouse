#include <gtest/gtest.h>

#include <Common/ProfileEvents.h>
#include <Common/ThreadProfileEvents.h>
#include <Common/VariableContext.h>

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#if defined(OS_LINUX)
#include <unistd.h>
#endif

namespace ProfileEvents
{
    extern const Event Query;
    extern const Event SelectQuery;
#if defined(OS_LINUX)
    extern const Event PerfCPUCycles;
    extern const Event PerfTaskClock;
#endif
}

/// Drive the real `Thread` -> `User` -> `global_counters` chain: every thread increments its own
/// `Thread`-level counter, propagating up to a shared `User`-level counter (per-CPU layout,
/// so increments scatter across CPU rows and `load` sums them) and the process-wide sharded
/// `global_counters`. Verify each level is accounted exactly; asserting on the `global_counters`
/// delta is what covers the static per-CPU storage (tests run sequentially, and only this test's
/// threads increment during it).
TEST(ProfileEvents, ChainAccountsPerThreadUserAndGlobal)
{
    const ProfileEvents::Count global_before = ProfileEvents::global_counters[ProfileEvents::Query];

    ProfileEvents::Counters user(VariableContext::User, &ProfileEvents::global_counters);

    constexpr size_t num_threads = 16;
    constexpr size_t increments_per_thread = 100'000;

    std::vector<std::unique_ptr<ProfileEvents::Counters>> per_thread;
    for (size_t t = 0; t < num_threads; ++t)
        per_thread.push_back(std::make_unique<ProfileEvents::Counters>(VariableContext::Thread, &user));

    std::vector<std::thread> threads;
    for (size_t t = 0; t < num_threads; ++t)
        threads.emplace_back([&, t]
        {
            for (size_t i = 0; i < increments_per_thread; ++i)
                per_thread[t]->increment(ProfileEvents::Query);
        });

    for (auto & thread : threads)
        thread.join();

    for (size_t t = 0; t < num_threads; ++t)
        EXPECT_EQ((*per_thread[t])[ProfileEvents::Query], increments_per_thread);

    EXPECT_EQ(user[ProfileEvents::Query], num_threads * increments_per_thread);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::Query] - global_before, num_threads * increments_per_thread);
}

/// With per-CPU sharding disabled the `User` level falls back to a single shared atomic per event.
/// Accounting must stay exact; only the layout (and memory) differs from the sharded path above.
TEST(ProfileEvents, ChainAccountsWithPerCPUDisabled)
{
    struct PerCPUGuard
    {
        explicit PerCPUGuard(bool enabled) { ProfileEvents::setUserPerCPUEnabled(enabled); }
        ~PerCPUGuard() { ProfileEvents::setUserPerCPUEnabled(true); }
    } guard(false);

    ProfileEvents::Counters global(VariableContext::Global, nullptr);
    ProfileEvents::Counters user(VariableContext::User, &global);

    constexpr size_t num_threads = 16;
    constexpr size_t increments_per_thread = 100'000;

    std::vector<std::unique_ptr<ProfileEvents::Counters>> per_thread;
    for (size_t t = 0; t < num_threads; ++t)
        per_thread.push_back(std::make_unique<ProfileEvents::Counters>(VariableContext::Thread, &user));

    std::vector<std::thread> threads;
    for (size_t t = 0; t < num_threads; ++t)
        threads.emplace_back([&, t]
        {
            for (size_t i = 0; i < increments_per_thread; ++i)
                per_thread[t]->increment(ProfileEvents::Query);
        });

    for (auto & thread : threads)
        thread.join();

    for (size_t t = 0; t < num_threads; ++t)
        EXPECT_EQ((*per_thread[t])[ProfileEvents::Query], increments_per_thread);

    EXPECT_EQ(user[ProfileEvents::Query], num_threads * increments_per_thread);
    EXPECT_EQ(global[ProfileEvents::Query], num_threads * increments_per_thread);
}

/// `MemoryCredits` of a group whose memory is not charged to the enclosing query are added to its own counters alone.
TEST(ProfileEvents, IncrementLocalDoesNotReachParents)
{
    ProfileEvents::Counters global(VariableContext::Global, nullptr);
    ProfileEvents::Counters query_process(VariableContext::Process, &global);
    ProfileEvents::Counters nested_process(VariableContext::Process, &query_process);

    const ProfileEvents::Count amount = 42;
    nested_process.incrementLocal(ProfileEvents::Query, amount);

    EXPECT_EQ(nested_process[ProfileEvents::Query], amount);
    EXPECT_EQ(query_process[ProfileEvents::Query], 0u);
    EXPECT_EQ(global[ProfileEvents::Query], 0u);
}

TEST(ProfileEvents, SnapshotSetOverwritesSingleEvent)
{
    ProfileEvents::Counters counters(VariableContext::Process, nullptr);
    counters.increment(ProfileEvents::Query, 5);

    auto snapshot = counters.getPartiallyAtomicSnapshot();
    EXPECT_EQ(snapshot[ProfileEvents::Query], 5u);

    snapshot.set(ProfileEvents::Query, 99);
    EXPECT_EQ(snapshot[ProfileEvents::Query], 99u);
    EXPECT_EQ(counters[ProfileEvents::Query], 5u);
}

TEST(ProfileEvents, TraceProfileEventPublishedWhileIncrementing)
{
    ProfileEvents::Counters counters(VariableContext::Thread, nullptr);

    std::thread setter([&] { counters.setTraceProfileEvent(ProfileEvents::SelectQuery); });
    std::thread incrementer([&] { counters.increment(ProfileEvents::Query); });

    setter.join();
    incrementer.join();

    EXPECT_EQ(counters[ProfileEvents::Query], 1);
}

TEST(ProfileEvents, ParentAttachedConcurrentlyWithIncrement)
{
    ProfileEvents::Counters counters(VariableContext::Thread, nullptr);
    std::atomic<bool> attached = false;

    std::thread incrementer([&]
    {
        while (!attached.load(std::memory_order_relaxed))
            ;
        counters.increment(ProfileEvents::Query);
    });

    auto parent = std::make_unique<ProfileEvents::Counters>(VariableContext::User, nullptr);
    counters.setParent(parent.get());
    attached.store(true, std::memory_order_release);

    incrementer.join();

    EXPECT_EQ((*parent)[ProfileEvents::Query], 1);
}

#if defined(OS_LINUX)
namespace
{
int makeMockPerfDescriptor(const DB::PerfEventValue & sample)
{
    int pipe_fds[2] = {-1, -1};
    if (::pipe(pipe_fds) != 0)
        return -1;
    const ssize_t written = ::write(pipe_fds[1], &sample, sizeof(sample));
    int close_rc = ::close(pipe_fds[1]);
    if (written != static_cast<ssize_t>(sizeof(sample)) || close_rc != 0)
    {
        int read_close_rc = ::close(pipe_fds[0]);
        (void)read_close_rc;
        return -1;
    }
    return pipe_fds[0];
}
}

TEST(ProfileEvents, PerfEventsResetAfterClosingDescriptors)
{
    const auto task_clock_indices = DB::PerfEventsCounters::eventIndicesFromString("PerfTaskClock");
    ASSERT_EQ(task_clock_indices.size(), 1);
    const size_t task_idx = task_clock_indices[0];

    DB::PerfEventsCounters counters;

    /// Query 1 records 200 ms on a fresh descriptor and closes descriptors at query end.
    counters.thread_events_descriptors_holder.descriptors[task_idx]
        = makeMockPerfDescriptor(DB::PerfEventValue{.value = 200'000'000, .time_enabled = 200'000'000, .time_running = 200'000'000});
    ASSERT_GE(counters.thread_events_descriptors_holder.descriptors[task_idx], 0);

    ProfileEvents::Counters query1(VariableContext::Thread, nullptr);
    counters.finalizeProfileEvents(query1);
    counters.closeEventDescriptors();
    EXPECT_EQ(query1[ProfileEvents::PerfTaskClock], 200'000'000);

    /// Query 2 on the same thread opens a fresh descriptor starting at 0 and runs a larger 300 ms workload.
    /// Without resetting `previous_values`, this would under-count as 300M - 200M = 100M.
    counters.thread_events_descriptors_holder.descriptors[task_idx]
        = makeMockPerfDescriptor(DB::PerfEventValue{.value = 300'000'000, .time_enabled = 300'000'000, .time_running = 300'000'000});
    ASSERT_GE(counters.thread_events_descriptors_holder.descriptors[task_idx], 0);

    ProfileEvents::Counters query2(VariableContext::Thread, nullptr);
    counters.finalizeProfileEvents(query2);
    counters.closeEventDescriptors();
    EXPECT_EQ(query2[ProfileEvents::PerfTaskClock], 300'000'000);

    /// Query 3 on the same thread opens a fresh descriptor starting at 0 and runs a smaller 100 ms workload.
    /// Without resetting `previous_values`, the decrease guard would drop the reading to 0.
    counters.thread_events_descriptors_holder.descriptors[task_idx]
        = makeMockPerfDescriptor(DB::PerfEventValue{.value = 100'000'000, .time_enabled = 100'000'000, .time_running = 100'000'000});
    ASSERT_GE(counters.thread_events_descriptors_holder.descriptors[task_idx], 0);

    ProfileEvents::Counters query3(VariableContext::Thread, nullptr);
    counters.finalizeProfileEvents(query3);
    counters.closeEventDescriptors();
    EXPECT_EQ(query3[ProfileEvents::PerfTaskClock], 100'000'000);

    /// Also verify releasing an event via `processThreadLocalChanges` resets its `previous_values` slot.
    const auto cpu_cycles_indices = DB::PerfEventsCounters::eventIndicesFromString("PerfCPUCycles");
    ASSERT_EQ(cpu_cycles_indices.size(), 1);
    const size_t cycles_idx = cpu_cycles_indices[0];

    counters.thread_events_descriptors_holder.descriptors[cycles_idx]
        = makeMockPerfDescriptor(DB::PerfEventValue{.value = 500, .time_enabled = 500, .time_running = 500});
    counters.thread_events_descriptors_holder.descriptors[task_idx]
        = makeMockPerfDescriptor(DB::PerfEventValue{.value = 400'000'000, .time_enabled = 400'000'000, .time_running = 400'000'000});
    ASSERT_GE(counters.thread_events_descriptors_holder.descriptors[cycles_idx], 0);
    ASSERT_GE(counters.thread_events_descriptors_holder.descriptors[task_idx], 0);

    ProfileEvents::Counters query4(VariableContext::Thread, nullptr);
    counters.finalizeProfileEvents(query4);
    EXPECT_EQ(query4[ProfileEvents::PerfTaskClock], 400'000'000);

    /// Keep `PerfCPUCycles` open so `events_to_open` is empty, while releasing `PerfTaskClock`.
    ASSERT_TRUE(counters.processThreadLocalChanges("PerfCPUCycles"));
    EXPECT_EQ(counters.thread_events_descriptors_holder.descriptors[task_idx], -1);

    counters.thread_events_descriptors_holder.descriptors[task_idx]
        = makeMockPerfDescriptor(DB::PerfEventValue{.value = 250'000'000, .time_enabled = 250'000'000, .time_running = 250'000'000});
    ASSERT_EQ(::close(counters.thread_events_descriptors_holder.descriptors[cycles_idx]), 0);
    counters.thread_events_descriptors_holder.descriptors[cycles_idx]
        = makeMockPerfDescriptor(DB::PerfEventValue{.value = 900, .time_enabled = 900, .time_running = 900});

    ProfileEvents::Counters query5(VariableContext::Thread, nullptr);
    counters.finalizeProfileEvents(query5);
    counters.closeEventDescriptors();
    EXPECT_EQ(query5[ProfileEvents::PerfTaskClock], 250'000'000);
    EXPECT_EQ(query5[ProfileEvents::PerfCPUCycles], 400);
}
#endif
