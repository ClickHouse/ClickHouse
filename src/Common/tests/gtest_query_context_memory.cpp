#include <gtest/gtest.h>

#include <thread>
#include <tuple>

#include <base/scope_guard.h>
#include <Common/CurrentThread.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/MemoryTrackerSwitcher.h>
#include <Common/QueryIdSwitcher.h>
#include <Common/QueryScope.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace FailPoints
{
    extern const char attach_to_group_failure[];
}

TEST(QueryContextMemory, TracksConstructionTemporaryCopiesAndAttachedCopies)
{
#if defined(SANITIZER)
    GTEST_SKIP() << "Requires ClickHouse allocation interceptors, which sanitizer builds replace";
#else
    auto source = Context::createCopy(getContext().context);
    constexpr Int64 payload_size = 256 * 1024;
    source->setSetting("log_comment", String(payload_size, 'x'));

    std::thread([&]
    {
        ThreadStatus thread;
        Int64 construction_bytes = 0;
        Int64 temporary_copy_bytes = 0;
        Int64 temporary_copy_released = 0;
        Int64 temporary_peak_floor = 0;
        Int64 attached_copy_bytes = 0;
        Int64 attached_copy_released = 0;
        Int64 peak = 0;
        {
            auto scope = QueryScope::createForQueryContext();
            auto * tracker = thread.memory_tracker.getParent();
            const auto before = tracker->get();
            auto context = Context::createCopyForQuery(source);
            construction_bytes = tracker->get() - before;
            const auto before_temporary_copy = tracker->get();
            {
                auto temporary_copy = Context::createCopy(context);
                temporary_copy_bytes = tracker->get() - before_temporary_copy;
                temporary_peak_floor = tracker->get();
            }
            temporary_copy_released = before_temporary_copy + temporary_copy_bytes - tracker->get();
            context->setSetting("log_comment", String{});
            context->setSetting("max_untracked_memory", UInt64{0});
            scope.attachToQueryContext(context);

            const auto before_copy = tracker->get();
            auto copy = Context::createCopy(context);
            attached_copy_bytes = tracker->get() - before_copy;
            copy.reset();
            attached_copy_released = before_copy + attached_copy_bytes - tracker->get();
            peak = tracker->getPeak();
        }
        EXPECT_EQ(CurrentThread::getGroup(), nullptr);
        EXPECT_EQ(thread.memory_tracker.getParent(), &total_memory_tracker);
        EXPECT_GE(construction_bytes, payload_size);
        EXPECT_GE(temporary_copy_bytes, payload_size);
        EXPECT_EQ(temporary_copy_released, temporary_copy_bytes);
        EXPECT_GT(attached_copy_bytes, 0);
        EXPECT_EQ(attached_copy_released, attached_copy_bytes);
        EXPECT_GE(peak, temporary_peak_floor);
    }).join();
#endif
}

TEST(QueryContextMemory, LiveThreadMetadataIsChargedAndReleased)
{
#if defined(SANITIZER)
    GTEST_SKIP() << "Requires ClickHouse allocation interceptors, which sanitizer builds replace";
#else
    auto source = Context::createCopy(getContext().context);
    source->setCurrentQueryId(String(8192, 'i'));
    source->setSetting("max_untracked_memory", UInt64{0});
    source->setSetting("log_queries", false);
    source->setSetting("query_profiler_real_time_period_ns", UInt64{0});
    source->setSetting("query_profiler_cpu_time_period_ns", UInt64{0});
    const String query = "SELECT 1 /*" + String(8192, 'q') + "*/";
    const String nested_id(8192, 'n');

    std::thread([&]
    {
        ThreadStatus thread{ThreadStatus::NoOSThreadTag{}};
        MemoryTracker user(&total_memory_tracker, VariableContext::User);
        std::unique_ptr<char[]> sentinel;
        {
            MemoryTrackerSwitcher user_scope(&user, 0);
            sentinel = std::make_unique<char[]>(16 * 1024 * 1024);
        }
        const Int64 user_floor = user.get();
        SCOPE_EXIT(
        {
            MemoryTrackerSwitcher user_scope(&user, 0);
            sentinel.reset();
        });
        for (size_t iteration = 0; iteration < 3; ++iteration)
        {
            bool rejected = false;
            bool query_id_restored = false;
            Int64 attached_bytes = 0;
            Int64 log_bytes = 0;
            Int64 worker_bytes = 0;
            Int64 query_before = 0;
            Int64 query_after = 0;
            Int64 user_before = 0;
            Int64 user_after = 0;
            {
                auto scope = QueryScope::createForQueryContext();
                auto * tracker = thread.memory_tracker.getParent();
                auto context = Context::createCopyForQuery(source);
                const auto before_attach = tracker->get();
                scope.attachToQueryContext(context, [capture = String(8192, 'c')]
                {
                    std::ignore = capture;
                });
                CurrentThread::flushUntrackedMemory();
                attached_bytes = tracker->get() - before_attach;
                const auto before_logs = tracker->get();
                CurrentThread::attachQueryForLog(query);
                log_bytes = tracker->get() - before_logs;
                {
                    MemoryTrackerSwitcher test_thread_memory_scope(&total_memory_tracker);
                    std::thread([group = CurrentThread::getGroup(), &worker_bytes]
                    {
                        ThreadStatus worker{ThreadStatus::NoOSThreadTag{}};
                        const auto before = group->memory_tracker.get();
                        CurrentThread::attachToGroup(group);
                        CurrentThread::flushUntrackedMemory();
                        worker_bytes = group->memory_tracker.get() - before;
                        CurrentThread::detachFromGroupIfNotDetached();
                    }).join();
                }
                rejected = tracker->tryInsertParent(&user).has_value();
                query_before = tracker->get();
                user_before = user.get();
                {
                    QueryIdSwitcher query_id_scope(nested_id);
                }
                query_id_restored = CurrentThread::getQueryId() == source->getCurrentQueryId();
                query_after = tracker->get();
                user_after = user.get();
            }
            /// This isolated user has no last-query reset to hide retained charges.
            EXPECT_GE(attached_bytes, static_cast<Int64>(source->getCurrentQueryId().size()));
            EXPECT_GE(log_bytes, static_cast<Int64>(2 * query.size()));
            EXPECT_GE(worker_bytes, static_cast<Int64>(source->getCurrentQueryId().size() + query.size()));
            EXPECT_FALSE(rejected);
            EXPECT_TRUE(query_id_restored);
            EXPECT_GT(query_before, 0);
            EXPECT_EQ(query_before, user_before - user_floor);
            EXPECT_EQ(query_after, query_before);
            EXPECT_EQ(user_after, user_before);
            /// The live allocation also prevents saturation at zero from hiding excess frees.
            EXPECT_EQ(user.get(), user_floor) << "iteration " << iteration;
        }
    }).join();
#endif
}

TEST(QueryContextMemory, FailedAttachmentRestoresOuterScope)
{
#if !USE_LIBFIU
    GTEST_SKIP() << "Fault injection is disabled";
#else
    auto source = getContext().context;
    std::thread([&]
    {
        ThreadStatus thread;
        const auto original_batching_limit = thread.untracked_memory_limit;
        FailPointInjection::enableFailPoint(FailPoints::attach_to_group_failure);
        {
            auto scope = QueryScope::createForQueryContext();
            auto * tracker = thread.memory_tracker.getParent();
            auto context = Context::createCopyForQuery(source);
            EXPECT_THROW(scope.attachToQueryContext(context), Exception);
            EXPECT_EQ(CurrentThread::getGroup(), nullptr);
            EXPECT_EQ(thread.memory_tracker.getParent(), tracker);
            const auto before_release = tracker->get();
            context.reset();
            [[maybe_unused]] const auto released = before_release - tracker->get();
#if !defined(SANITIZER)
            /// Sanitizer builds replace allocation interceptors, but still exercise attachment cleanup.
            EXPECT_GT(released, 0);
#endif
        }
        EXPECT_EQ(CurrentThread::getGroup(), nullptr);
        EXPECT_EQ(thread.memory_tracker.getParent(), &total_memory_tracker);
        /// A subsequent query on the same connection can attach normally.
        {
            auto scope = QueryScope::createForQueryContext();
            auto context = Context::createCopy(source);
            scope.attachToQueryContext(context);
            EXPECT_NE(CurrentThread::getGroup(), nullptr);
        }
        EXPECT_EQ(CurrentThread::getGroup(), nullptr);
        /// Setup restores its caller's batching limit before query settings are applied.
        thread.untracked_memory_limit = original_batching_limit;
        {
            auto scope = QueryScope::createForQueryContext();
            auto context = Context::createCopy(source);
        }
        EXPECT_EQ(thread.untracked_memory_limit, original_batching_limit);
    }).join();
#endif
}

TEST(QueryContextMemory, MovingSetupPreservesSingleRestoration)
{
    auto source = getContext().context;
    std::thread([&]
    {
        ThreadStatus thread;
        const auto original_batching_limit = thread.untracked_memory_limit;
        {
            auto scope = QueryScope::createForQueryContext();
            auto * tracker = thread.memory_tracker.getParent();
            QueryScope moved(std::move(scope));
            QueryScope assigned;
            assigned = std::move(moved);
            EXPECT_EQ(thread.memory_tracker.getParent(), tracker);
#ifndef DEBUG_OR_SANITIZER_BUILD
            /// An intentional `LOGICAL_ERROR` aborts in Debug and sanitizer builds.
            EXPECT_THROW(assigned = QueryScope::createForQueryContext(), Exception);
#endif
            EXPECT_EQ(thread.memory_tracker.getParent(), tracker);
            auto context = Context::createCopy(source);
        }
        EXPECT_EQ(CurrentThread::getGroup(), nullptr);
        EXPECT_EQ(thread.memory_tracker.getParent(), &total_memory_tracker);
        EXPECT_EQ(thread.untracked_memory_limit, original_batching_limit);
    }).join();
}

TEST(QueryContextMemory, WeakControlBlockReleasedOnAnotherThreadDoesNotDebitItsQuery)
{
    auto source = Context::createCopy(getContext().context);
    source->setSetting("max_untracked_memory", UInt64{0});
    ContextWeakPtr weak;
    [[maybe_unused]] Int64 released_context_bytes = 0;
    std::thread([&]
    {
        ThreadStatus thread;
        auto scope = QueryScope::createForQueryContext();
        auto * tracker = thread.memory_tracker.getParent();
        auto context = Context::createCopyForQuery(source);
        weak = context;
        scope.attachToQueryContext(context);
        const auto before_release = tracker->get();
        context.reset();
        released_context_bytes = before_release - tracker->get();
    }).join();
    EXPECT_TRUE(weak.expired());
#if !defined(SANITIZER)
    /// Sanitizer builds still exercise cross-thread release and the explicit tracking calls below.
    EXPECT_GT(released_context_bytes, 0);
#endif

    Int64 unrelated_query_delta = 0;
    Int64 pending_after_release = 0;
    std::thread([&]
    {
        ThreadStatus thread;
        auto scope = QueryScope::createForQueryContext();
        auto * tracker = thread.memory_tracker.getParent();
        auto unrelated_context = Context::createCopyForQuery(source);
        thread.untracked_memory_limit = 1024;
        std::ignore = CurrentMemoryTracker::allocNoThrow(25);
        const auto before_release = tracker->get();
        {
            DENY_ALLOCATIONS_IN_SCOPE;
            weak.reset();
        }
        unrelated_query_delta = tracker->get() - before_release;
        pending_after_release = thread.untracked_memory.load();
        std::ignore = CurrentMemoryTracker::free(25);
    }).join();
    EXPECT_EQ(unrelated_query_delta, 0);
    EXPECT_EQ(pending_after_release, 25);
}

TEST(QueryContextMemory, WeakBookkeepingDoesNotRetainQueryCharge)
{
    auto source = getContext().context;
    std::thread([&]
    {
        ThreadStatus thread;
        auto scope = QueryScope::createForQueryContext();
        auto * tracker = thread.memory_tracker.getParent();
        const auto before_copy = tracker->get();
        auto context = Context::createCopyForQuery(source);
        ContextWeakPtr weak = context;
        context.reset();
        EXPECT_EQ(tracker->get(), before_copy);
        EXPECT_TRUE(weak.expired());
    }).join();
}

TEST(QueryContextMemory, FailedControlBlockAllocationReleasesConstructedContext)
{
#if !USE_LIBFIU
    GTEST_SKIP() << "Fault injection is disabled";
#else
    auto source = Context::createCopy(getContext().context);
    source->setSetting("log_comment", String(256 * 1024, 'x'));
    FailPointInjection::enableFailPoint("query_context_control_block_allocation_failure");
    std::thread([&]
    {
        ThreadStatus thread;
        auto scope = QueryScope::createForQueryContext();
        auto * tracker = thread.memory_tracker.getParent();
        const auto before_copy = tracker->get();
        EXPECT_THROW(Context::createCopyForQuery(source), Exception);
        EXPECT_EQ(tracker->get(), before_copy);
    }).join();
#endif
}

}
