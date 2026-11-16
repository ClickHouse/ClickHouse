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

/// `addExceptCommonAncestors` transfers the counts of one chain to another, like the profile events of a merge,
/// which its own thread group adds to its parents, to the query that ran the merge. Every counter of the two
/// chains must have the counts once, whatever the common ancestors of the chains are. The chains have their own
/// root instead of `global_counters`, so that the test does not depend on other users of the global counters.
TEST(ProfileEvents, AddExceptCommonAncestors)
{
    using ProfileEvents::Counters;
    const auto event = ProfileEvents::SelectQuery;

    /// The chain of the task joins the chain of the query at the root: query -> user -> root, task -> root.
    {
        Counters root(VariableContext::Global, nullptr);
        Counters user(VariableContext::User, &root);
        Counters query(VariableContext::Process, &user);
        Counters task(VariableContext::Process, &root);

        task.increment(event, 5);
        query.addExceptCommonAncestors(task);

        EXPECT_EQ(query[event], 5);
        EXPECT_EQ(user[event], 5);
        EXPECT_EQ(root[event], 5);
        EXPECT_EQ(task[event], 5);
    }

    /// The chain of the task joins the chain of the query below the root: query -> user -> background -> root,
    /// task -> background -> root.
    {
        Counters root(VariableContext::Global, nullptr);
        Counters background(VariableContext::Process, &root);
        Counters user(VariableContext::User, &background);
        Counters query(VariableContext::Process, &user);
        Counters task(VariableContext::Process, &background);

        task.increment(event, 5);
        query.addExceptCommonAncestors(task);

        EXPECT_EQ(query[event], 5);
        EXPECT_EQ(user[event], 5);
        EXPECT_EQ(background[event], 5);
        EXPECT_EQ(root[event], 5);
    }

    /// The task has no parent: every counter of the query chain receives the counts.
    {
        Counters root(VariableContext::Global, nullptr);
        Counters user(VariableContext::User, &root);
        Counters query(VariableContext::Process, &user);
        Counters task(VariableContext::Process, nullptr);

        task.increment(event, 5);
        query.addExceptCommonAncestors(task);

        EXPECT_EQ(query[event], 5);
        EXPECT_EQ(user[event], 5);
        EXPECT_EQ(root[event], 5);
    }

    /// The task is a child of the query: the whole query chain has already received the counts.
    {
        Counters root(VariableContext::Global, nullptr);
        Counters user(VariableContext::User, &root);
        Counters query(VariableContext::Process, &user);
        Counters task(VariableContext::Process, &query);

        task.increment(event, 5);
        query.addExceptCommonAncestors(task);

        EXPECT_EQ(query[event], 5);
        EXPECT_EQ(user[event], 5);
        EXPECT_EQ(root[event], 5);
    }
}
