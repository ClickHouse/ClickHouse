#include <memory>
#include <thread>

#include <gtest/gtest.h>

#include <Common/ThreadStatus.h>
#include <Common/ProfileEvents.h>

#include <Common/tests/gtest_global_context.h>
#include <Interpreters/Context.h>

namespace ProfileEvents
{
    extern const Event Query;
}


namespace DB
{
namespace
{

template <typename CreateChild>
void checkBorrowedChildRetainsParent(CreateChild && create_child)
{
    auto context = getContext().context;
    auto parent = std::make_shared<ThreadGroup>(context, 0);
    std::weak_ptr<ThreadGroup> weak_parent = parent;
    const auto initial_query_count = parent->performance_counters[ProfileEvents::Query];

    auto child = create_child(context, parent);
    parent.reset();

    ASSERT_FALSE(weak_parent.expired());

    child->performance_counters.increment(ProfileEvents::Query);
    static_cast<void>(child->memory_tracker.getResolvedSampleConfig());

    auto retained_parent = weak_parent.lock();
    ASSERT_NE(retained_parent, nullptr);
    EXPECT_EQ(retained_parent->performance_counters[ProfileEvents::Query], initial_query_count + 1);

    retained_parent.reset();
    child.reset();
    EXPECT_TRUE(weak_parent.expired());
}

TEST(ThreadGroup, BorrowedChildrenRetainParent)
{
    /// Use a dedicated thread so `CurrentThread` is independent of other tests in `unit_tests_dbms`.
    std::thread thread([]
    {
        ThreadStatus thread_status;

        checkBorrowedChildRetainsParent([](ContextPtr, const ThreadGroupPtr & parent)
        {
            return std::make_shared<ThreadGroup>(parent);
        });

        checkBorrowedChildRetainsParent([](ContextPtr context, const ThreadGroupPtr & parent)
        {
            return std::make_shared<ThreadGroup>(context, parent);
        });
    });
    thread.join();
}

} // namespace
} // namespace DB
