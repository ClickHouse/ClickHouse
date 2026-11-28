#include <Interpreters/QuerySchedulingGroup.h>

#include <gtest/gtest.h>

#include <barrier>
#include <thread>
#include <vector>

using namespace DB;

namespace
{

QuerySchedulingGroupPtr makeGroup()
{
    return std::make_shared<QuerySchedulingGroup>("all", ClassifierSettings{}, /*classifier_=*/ nullptr);
}

/// Joins the query whose group is registered under `group_id`, the way a task does on a worker.
QuerySchedulingGroupPtr join(DistributedQuerySchedulingGroups & groups, const String & group_id)
{
    return groups.join(group_id, [](const QuerySchedulingGroupPtr & registered) { return registered ? registered : makeGroup(); });
}

}

/// Parts of one query that join concurrently, with no group registered yet, get one group.
TEST(DistributedQuerySchedulingGroups, ConcurrentJoinGetsOneGroup)
{
    DistributedQuerySchedulingGroups groups;
    constexpr size_t num_threads = 16;
    std::vector<QuerySchedulingGroupPtr> joined(num_threads);
    std::barrier<> start(num_threads);

    std::vector<std::thread> threads;
    for (size_t i = 0; i < num_threads; ++i)
        threads.emplace_back([&, i]
        {
            start.arrive_and_wait();
            joined[i] = join(groups, "query");
        });
    for (auto & thread : threads)
        thread.join();

    for (const auto & group : joined)
        EXPECT_EQ(group, joined.front());
}

/// A part joins the group the initiator registered.
TEST(DistributedQuerySchedulingGroups, PartJoinsRegisteredGroup)
{
    DistributedQuerySchedulingGroups groups;
    auto initiator = makeGroup();
    groups.add("query", initiator);

    EXPECT_EQ(join(groups, "query"), initiator);
    EXPECT_NE(join(groups, "other query"), initiator);
}

/// A part that finds only an expired entry registers its own group.
TEST(DistributedQuerySchedulingGroups, ExpiredGroupIsReplaced)
{
    DistributedQuerySchedulingGroups groups;
    groups.add("query", makeGroup());

    auto group = join(groups, "query");
    ASSERT_NE(group, nullptr);
    EXPECT_EQ(join(groups, "query"), group);
}
