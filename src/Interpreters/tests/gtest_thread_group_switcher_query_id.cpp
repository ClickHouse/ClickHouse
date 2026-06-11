#include <gtest/gtest.h>

#include <Common/CurrentThread.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Interpreters/Context.h>

using namespace DB;

/// A thread may have a query_id assigned directly without being attached to any thread group,
/// e.g. `BgSchPool::<uuid>` set by BackgroundSchedulePool. A temporary scope group must keep
/// this query_id while the thread is attached to it, and restore it after detaching - the
/// `system.part_log` rows produced by background streaming inserts (e.g. S3Queue to a
/// materialized view) rely on it for correlation.
TEST(ThreadGroupSwitcher, KeepsAndRestoresDirectlyAssignedQueryId)
{
    getContext();
    MainThreadStatus::getInstance();

    const std::string query_id = "BgSchPool::test-query-id";
    current_thread->setQueryId(std::string(query_id));

    {
        auto thread_group = ThreadGroup::createForScope();
        ThreadGroupSwitcher switcher(thread_group, ThreadName::MERGETREE_WRITE_PART, /*allow_existing_group*/ true);
        EXPECT_EQ(CurrentThread::getQueryId(), query_id);
    }

    EXPECT_EQ(CurrentThread::getQueryId(), query_id);

    current_thread->clearQueryId();
}
