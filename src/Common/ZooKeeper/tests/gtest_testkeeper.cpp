#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/TestKeeper.h>
#include <Common/ZooKeeper/Types.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>

#include <atomic>
#include <future>

using namespace Coordination;
using namespace DB;

namespace
{

Coordination::TestKeeper makeKeeper(int32_t operation_timeout_ms = DEFAULT_OPERATION_TIMEOUT_MS, std::string chroot = "")
{
    zkutil::ZooKeeperArgs args;
    args.operation_timeout_ms = operation_timeout_ms;
    args.chroot = chroot;

    return Coordination::TestKeeper(args);
}

void create(TestKeeper & keeper, const String & path, const String & data, bool is_ephemeral)
{
    std::promise<CreateResponse> sink;
    std::future<CreateResponse> future = sink.get_future();
    keeper.create(path, data, is_ephemeral, /* is_sequential */ false, {},
        [&](const auto & response) { sink.set_value(std::move(response)); });

    CreateResponse response = future.get();
    ASSERT_EQ(response.error, Error::ZOK);
}

bool exists(TestKeeper & keeper, const String & path, WatchCallbackPtrOrEventPtr watch = {})
{
    std::promise<ExistsResponse> sink;
    std::future<ExistsResponse> future = sink.get_future();
    keeper.exists(path, [&](const auto & response) { sink.set_value(std::move(response)); }, std::move(watch));

    return future.get().error == Coordination::Error::ZOK;
}

ListResponse list(TestKeeper & keeper, const String & path, ListRequestType list_request_type, bool with_stat, bool with_data)
{
    std::promise<ListResponse> sink;
    std::future<ListResponse> future = sink.get_future();
    keeper.list(path, list_request_type,
        [&](const auto & response) { sink.set_value(std::move(response)); },
        WatchCallbackPtrOrEventPtr(), with_stat, with_data);

    return future.get();
}

ListWithOptionsResponse listWithOptions(TestKeeper & keeper, const String & path, const ListOptions & options)
{
    std::promise<ListWithOptionsResponse> sink;
    std::future<ListWithOptionsResponse> future = sink.get_future();
    keeper.listWithOptions(
        path,
        options,
        [&](const auto & response) { sink.set_value(response); },
        WatchCallbackPtrOrEventPtr());

    return future.get();
}

RemoveRecursiveResponse removeRecursive(TestKeeper & keeper, const String & path, uint32_t remove_nodes_limit)
{
    std::promise<RemoveRecursiveResponse> sink;
    std::future<RemoveRecursiveResponse> future = sink.get_future();
    keeper.removeRecursive(path, remove_nodes_limit, [&](const auto & response) { sink.set_value(response); });

    return future.get();
}

ListRecursiveResponse listRecursive(TestKeeper & keeper, const String & path)
{
    std::promise<ListRecursiveResponse> sink;
    std::future<ListRecursiveResponse> future = sink.get_future();
    keeper.listRecursive(path, /* get_children_recursive_nodes_limit */ 100, [&](const auto & response) { sink.set_value(response); });

    return future.get();
}

}

TEST(TestKeeperTest, JustWorks)
{
    TestKeeper keeper = makeKeeper();

    ASSERT_TRUE(exists(keeper, "/"));
    ASSERT_FALSE(exists(keeper, "/A"));

    create(keeper, "/A", "hello", /*is_ephemeral=*/false);
    ASSERT_TRUE(exists(keeper, "/A"));
}

TEST(TestKeeperTest, FilteredListWithStatsAndDataIsAligned)
{
    TestKeeper keeper = makeKeeper();

    create(keeper, "/parent", "", /* is_ephemeral */ false);
    create(keeper, "/parent/ephemeral", "ephemeral_data", /* is_ephemeral */ true);
    create(keeper, "/parent/persistent", "persistent_data", /* is_ephemeral */ false);

    {
        ListResponse response = list(keeper, "/parent", ListRequestType::PERSISTENT_ONLY, /* with_stat */ true, /* with_data */ true);

        ASSERT_EQ(response.error, Error::ZOK);
        ASSERT_EQ(response.names, std::vector<std::string>({"persistent"}));

        ASSERT_EQ(response.data.size(), 1u);
        EXPECT_EQ(response.data[0], "persistent_data");

        ASSERT_EQ(response.stats.size(), 1u);
        EXPECT_EQ(response.stats[0].ephemeralOwner, 0);
    }

    {
        ListResponse response = list(keeper, "/parent", ListRequestType::EPHEMERAL_ONLY, /* with_stat */ true, /* with_data */ true);

        ASSERT_EQ(response.error, Error::ZOK);
        EXPECT_EQ(response.names, std::vector<std::string>({"ephemeral"}));

        ASSERT_EQ(response.data.size(), 1u);
        EXPECT_EQ(response.data[0], "ephemeral_data");

        ASSERT_EQ(response.stats.size(), 1u);
        EXPECT_NE(response.stats[0].ephemeralOwner, 0);
    }

    {
        ListResponse response = list(keeper, "/parent", ListRequestType::ALL, /* with_stat */ true, /* with_data */ true);

        ASSERT_EQ(response.error, Error::ZOK);
        ASSERT_EQ(response.names.size(), 2u);
        ASSERT_EQ(response.data.size(), 2u);
        ASSERT_EQ(response.stats.size(), 2u);
    }
}

TEST(TestKeeperTest, FilteredListWithoutStatsAndData)
{
    TestKeeper keeper = makeKeeper();

    create(keeper, "/parent", "", /* is_ephemeral */ false);
    create(keeper, "/parent/ephemeral", "ephemeral_data", /* is_ephemeral */ true);
    create(keeper, "/parent/persistent", "persistent_data", /* is_ephemeral */ false);

    {
        ListResponse response = list(keeper, "/parent", ListRequestType::PERSISTENT_ONLY, /* with_stat */ false, /* with_data */ false);

        ASSERT_EQ(response.error, Error::ZOK);
        ASSERT_EQ(response.names, std::vector<std::string>({"persistent"}));

        EXPECT_TRUE(response.data.empty());
        EXPECT_TRUE(response.stats.empty());
    }
}

TEST(TestKeeperTest, ListWithOptionsShuffleLimitIsTruncated)
{
    TestKeeper keeper = makeKeeper();

    create(keeper, "/parent", "", /* is_ephemeral */ false);
    create(keeper, "/parent/a", "", /* is_ephemeral */ false);
    create(keeper, "/parent/b", "", /* is_ephemeral */ false);
    create(keeper, "/parent/c", "", /* is_ephemeral */ false);

    ListOptions options;
    options.max_results = 1;
    options.shuffle = true;
    const auto response = listWithOptions(keeper, "/parent", options);

    EXPECT_EQ(response.error, Error::ZOK);
    EXPECT_EQ(response.names.size(), 1);
    EXPECT_TRUE(response.truncated);
}

TEST(TestKeeperTest, RemoveRecursiveKeepsPrefixSiblings)
{
    TestKeeper keeper = makeKeeper();

    /// `/q-x` and `/q.x` sort between `/q` and `/q/a`, `/q2` sorts after the subtree.
    for (const auto * path : {"/q", "/q/a", "/q/a/b", "/q-x", "/q-x/y", "/q.x", "/q2", "/q2/z"})
        create(keeper, path, "", /* is_ephemeral */ false);

    ASSERT_EQ(removeRecursive(keeper, "/q", /* remove_nodes_limit */ 100).error, Error::ZOK);

    for (const auto * path : {"/q", "/q/a", "/q/a/b"})
        EXPECT_FALSE(exists(keeper, path)) << path;
    for (const auto * path : {"/q-x", "/q-x/y", "/q.x", "/q2", "/q2/z"})
        EXPECT_TRUE(exists(keeper, path)) << path;

    ListResponse root = list(keeper, "/", ListRequestType::ALL, /* with_stat */ false, /* with_data */ false);
    EXPECT_EQ(root.names, std::vector<std::string>({"q-x", "q.x", "q2"}));
    EXPECT_EQ(root.stat.numChildren, static_cast<int32_t>(root.names.size()));
}

TEST(TestKeeperTest, RemoveRecursiveLimitCountsOnlySubtree)
{
    TestKeeper keeper = makeKeeper();

    for (const auto * path : {"/q", "/q/a", "/q2", "/q2/z"})
        create(keeper, path, "", /* is_ephemeral */ false);

    EXPECT_EQ(removeRecursive(keeper, "/q", /* remove_nodes_limit */ 1).error, Error::ZNOTEMPTY);
    EXPECT_EQ(removeRecursive(keeper, "/q", /* remove_nodes_limit */ 2).error, Error::ZOK);
    EXPECT_FALSE(exists(keeper, "/q"));
    EXPECT_TRUE(exists(keeper, "/q2/z"));
}

TEST(TestKeeperTest, RemoveRecursiveTriggersOnlySubtreeWatches)
{
    TestKeeper keeper = makeKeeper();

    for (const auto * path : {"/q", "/q/a", "/q2"})
        create(keeper, path, "", /* is_ephemeral */ false);

    auto subtree_events = std::make_shared<std::atomic<size_t>>(0);
    auto sibling_events = std::make_shared<std::atomic<size_t>>(0);
    ASSERT_TRUE(exists(keeper, "/q/a", std::make_shared<WatchCallback>([subtree_events](const WatchResponse &) { ++*subtree_events; })));
    ASSERT_TRUE(exists(keeper, "/q2", std::make_shared<WatchCallback>([sibling_events](const WatchResponse &) { ++*sibling_events; })));

    ASSERT_EQ(removeRecursive(keeper, "/q", /* remove_nodes_limit */ 100).error, Error::ZOK);

    EXPECT_EQ(subtree_events->load(), 1u);
    EXPECT_EQ(sibling_events->load(), 0u);
}

TEST(TestKeeperTest, ListSubtreeWithSiblingSortedBeforeChildren)
{
    TestKeeper keeper = makeKeeper();

    /// `/q-x` sorts between `/q` and `/q/a`.
    for (const auto * path : {"/q", "/q/a", "/q/a/b", "/q-x"})
        create(keeper, path, "", /* is_ephemeral */ false);

    ListRecursiveResponse subtree = listRecursive(keeper, "/q");
    ASSERT_EQ(subtree.error, Error::ZOK);
    EXPECT_EQ(subtree.children, std::vector<std::string>({"/q/a", "/q/a/b"}));

    EXPECT_EQ(listWithOptions(keeper, "/q", ListOptions{}).names, std::vector<std::string>({"a"}));

    ListOptions recursive_options;
    recursive_options.recursive = true;
    EXPECT_EQ(listWithOptions(keeper, "/q", recursive_options).names, std::vector<std::string>({"a", "a/b"}));
}
