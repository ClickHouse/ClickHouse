#include <gtest/gtest.h>

#include <Common/ProfileEvents.h>
#include <Common/VariableContext.h>

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

namespace ProfileEvents
{
    extern const Event Query;
    extern const Event SelectQuery;
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

/// Query-scoped events must start at the outermost Process counters so Thread and nested Process
/// scopes (materialized views, nested async-insert flushes) never observe them.
TEST(ProfileEvents, IncrementAtOutermostProcessSkipsNestedScopes)
{
    ProfileEvents::Counters global(VariableContext::Global, nullptr);
    ProfileEvents::Counters user(VariableContext::User, &global);
    ProfileEvents::Counters query_process(VariableContext::Process, &user);
    ProfileEvents::Counters nested_process(VariableContext::Process, &query_process);
    ProfileEvents::Counters thread(VariableContext::Thread, &nested_process);
    ProfileEvents::Counters scope(VariableContext::Thread, &thread);

    const ProfileEvents::Count amount = 42;
    scope.incrementAtOutermostProcess(ProfileEvents::Query, amount);

    EXPECT_EQ(scope[ProfileEvents::Query], 0u);
    EXPECT_EQ(thread[ProfileEvents::Query], 0u);
    EXPECT_EQ(nested_process[ProfileEvents::Query], 0u);
    EXPECT_EQ(query_process[ProfileEvents::Query], amount);
    EXPECT_EQ(user[ProfileEvents::Query], amount);
    EXPECT_EQ(global[ProfileEvents::Query], amount);
}

TEST(ProfileEvents, IncrementAtOutermostProcessNoProcessIsNoOp)
{
    ProfileEvents::Counters global(VariableContext::Global, nullptr);
    ProfileEvents::Counters thread(VariableContext::Thread, &global);

    thread.incrementAtOutermostProcess(ProfileEvents::Query, 7);

    EXPECT_EQ(thread[ProfileEvents::Query], 0u);
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
