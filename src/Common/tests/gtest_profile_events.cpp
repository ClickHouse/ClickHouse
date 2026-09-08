#include <gtest/gtest.h>

#include <Common/ProfileEvents.h>
#include <Common/MemoryTracker.h>
#include <Common/VariableContext.h>

#include <atomic>
#include <memory>
#include <limits>
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

/// Exercise every event, including the tail, through single-row and per-CPU parents. The deny
/// scope covers updates only; constructing a snapshot still allocates its dense result buffer.
TEST(ProfileEvents, EveryEventPropagatesWithoutAllocating)
{
    struct PerCPUGuard
    {
        ~PerCPUGuard()
        {
            ProfileEvents::setUserPerCPUEnabled(true);
        }
    } guard;

    for (bool per_cpu : {false, true})
    {
        ProfileEvents::setUserPerCPUEnabled(per_cpu);
        ProfileEvents::Counters global(VariableContext::Global, nullptr);
        ProfileEvents::Counters user(VariableContext::User, &global);
        ProfileEvents::Counters process(VariableContext::Process, &user);
        ProfileEvents::Counters thread(VariableContext::Thread, &process);
        {
            DENY_ALLOCATIONS_IN_SCOPE;
            for (ProfileEvents::Event event(0); event < ProfileEvents::end(); ++event)
            {
                thread.increment(event, 1);
                thread.incrementNoTrace(event, 2);
                thread.incrementSignalSafe(event, 4);
                thread.increment(event, 0);
            }
        }
        for (const auto * counters : {&thread, &process, &user, &global})
        {
            auto snapshot = counters->getPartiallyAtomicSnapshot();
            for (ProfileEvents::Event event(0); event < ProfileEvents::end(); ++event)
            {
                EXPECT_EQ((*counters)[event], 7);
                EXPECT_EQ(snapshot[event], 7);
            }
        }
    }
}

TEST(ProfileEvents, MoveResetAndWrapEveryEvent)
{
    for (auto level : {VariableContext::Thread, VariableContext::Process})
    {
        ProfileEvents::Counters parent(VariableContext::Global, nullptr);
        ProfileEvents::Counters original(level, &parent);
        for (ProfileEvents::Event event(0); event < ProfileEvents::end(); ++event)
            original.incrementNoTrace(event, std::numeric_limits<ProfileEvents::Count>::max());
        ProfileEvents::Counters moved(std::move(original));
        original.reset();
        {
            DENY_ALLOCATIONS_IN_SCOPE;
            for (ProfileEvents::Event event(0); event < ProfileEvents::end(); ++event)
                moved.incrementSignalSafe(event, 2);
        }
        auto snapshot = moved.getPartiallyAtomicSnapshot();
        for (ProfileEvents::Event event(0); event < ProfileEvents::end(); ++event)
        {
            EXPECT_EQ(moved[event], 1);
            EXPECT_EQ(snapshot[event], 1);
            EXPECT_EQ(parent[event], 1);
        }
        {
            DENY_ALLOCATIONS_IN_SCOPE;
            moved.resetCounters();
            for (ProfileEvents::Event event(0); event < ProfileEvents::end(); ++event)
                moved.incrementSignalSafe(event, 3);
        }
        for (ProfileEvents::Event event(0); event < ProfileEvents::end(); ++event)
        {
            EXPECT_EQ(moved[event], 3);
            EXPECT_EQ(parent[event], 4);
        }
        {
            DENY_ALLOCATIONS_IN_SCOPE;
            moved.reset();
            for (ProfileEvents::Event event(0); event < ProfileEvents::end(); ++event)
                moved.incrementSignalSafe(event, 5);
        }
        for (ProfileEvents::Event event(0); event < ProfileEvents::end(); ++event)
        {
            EXPECT_EQ(moved[event], 5);
            EXPECT_EQ(parent[event], 4);
        }
    }
}

TEST(ProfileEvents, ConcurrentTailUpdates)
{
    ProfileEvents::Counters counters(VariableContext::Process, nullptr);
    const ProfileEvents::Event last_event(ProfileEvents::end() - 1);
    constexpr size_t num_threads = 8;
    constexpr size_t increments = 10'000;
    std::vector<std::thread> threads;
    for (size_t i = 0; i < num_threads; ++i)
    {
        threads.emplace_back([&]
        {
            DENY_ALLOCATIONS_IN_SCOPE;
            for (size_t j = 0; j < increments; ++j)
                counters.incrementSignalSafe(last_event);
        });
    }
    for (auto & thread : threads)
        thread.join();
    EXPECT_EQ(counters[last_event], num_threads * increments);
    EXPECT_EQ(counters.getPartiallyAtomicSnapshot()[last_event], num_threads * increments);
}
