#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Interpreters/Context.h>

#include <gtest/gtest.h>
#include <thread>

using namespace DB;

TEST(QueryThreadMetadata, ReusedThreadReleasesQueryIdLogsAndCallback)
{
#if defined(SANITIZER)
    GTEST_SKIP() << "Requires ClickHouse allocation interceptors, which sanitizer builds replace";
#else
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();
    context->setCurrentQueryId(String(8192, 'i'));
    context->setSetting("max_untracked_memory", UInt64{0});
    context->setSetting("log_queries", false);
    context->setSetting("query_profiler_real_time_period_ns", UInt64{0});
    context->setSetting("query_profiler_cpu_time_period_ns", UInt64{0});
    const String query = "SELECT 1 /*" + String(8192, 'q') + "*/";

    std::thread([&]
    {
        ThreadStatus thread{ThreadStatus::NoOSThreadTag{}};
        thread.untracked_memory_limit = 0;
        MemoryTracker user(&total_memory_tracker, VariableContext::User);
        for (size_t iteration = 0; iteration < 3; ++iteration)
        {
            auto group = ThreadGroup::createForQuery(context, [capture = String(8192, 'c')]
            {
                std::ignore = capture;
            });
            group->memory_tracker.setParent(&user);
            CurrentThread::attachToGroup(group);
            CurrentThread::attachQueryForLog(query);
            CurrentThread::flushUntrackedMemory();
            const auto live_bytes = user.get();

            CurrentThread::detachFromGroupIfNotDetached();
            group.reset();
            CurrentThread::flushUntrackedMemory();
            EXPECT_GE(live_bytes, 4 * 8192);
            EXPECT_EQ(user.get(), 0) << "metadata remains charged after iteration " << iteration;
            EXPECT_TRUE(CurrentThread::getQueryId().empty());
        }
    }).join();
#endif
}
