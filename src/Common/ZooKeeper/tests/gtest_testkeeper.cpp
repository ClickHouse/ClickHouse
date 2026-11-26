#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/TestKeeper.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
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

void set(TestKeeper & keeper, const String & path, const String & data)
{
    std::promise<SetResponse> sink;
    std::future<SetResponse> future = sink.get_future();
    keeper.set(path, data, /* version */ -1, [&](const auto & response) { sink.set_value(response); });

    ASSERT_EQ(future.get().error, Error::ZOK);
}

ListResponse list(
    TestKeeper & keeper, const String & path, ListRequestType list_request_type, bool with_stat, bool with_data,
    WatchCallbackPtrOrEventPtr watch = {})
{
    std::promise<ListResponse> sink;
    std::future<ListResponse> future = sink.get_future();
    keeper.list(path, list_request_type,
        [&](const auto & response) { sink.set_value(std::move(response)); },
        std::move(watch), with_stat, with_data);

    return future.get();
}

ListWithOptionsResponse listWithOptions(TestKeeper & keeper, const String & path, const ListOptions & options, WatchCallbackPtrOrEventPtr watch = {})
{
    std::promise<ListWithOptionsResponse> sink;
    std::future<ListWithOptionsResponse> future = sink.get_future();
    keeper.listWithOptions(
        path,
        options,
        [&](const auto & response) { sink.set_value(response); },
        std::move(watch));

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

/// Counts the events of every watch it makes and checks that each one carries the expected type and path.
struct WatchEvents
{
    std::vector<std::shared_ptr<std::atomic<size_t>>> counters;

    WatchCallbackPtr make(const String & path, int32_t type = CHILD)
    {
        auto counter = counters.emplace_back(std::make_shared<std::atomic<size_t>>(0));
        return std::make_shared<WatchCallback>([counter, path, type](const WatchResponse & response)
        {
            EXPECT_EQ(response.type, type);
            EXPECT_EQ(response.path, path);
            ++*counter;
        });
    }

    void expect(const std::vector<size_t> & expected) const
    {
        ASSERT_EQ(counters.size(), expected.size());
        for (size_t i = 0; i < counters.size(); ++i)
            EXPECT_EQ(counters[i]->load(), expected[i]) << "watch " << i;
    }
};

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

TEST(TestKeeperTest, ListWatchFiresOnChildSetOnlyWithStatOrData)
{
    TestKeeper keeper = makeKeeper();

    const std::vector<String> dirs{"/plain", "/stat", "/data", "/options"};
    for (const auto & dir : dirs)
    {
        create(keeper, dir, "", /* is_ephemeral */ false);
        create(keeper, dir + "/child", "", /* is_ephemeral */ false);
    }

    WatchEvents events;
    ListOptions with_data_options;
    with_data_options.with_data = true;

    /// 0, 1: plain list requests.
    ASSERT_EQ(list(keeper, "/plain", ListRequestType::ALL, false, false, events.make("/plain")).error, Error::ZOK);
    ASSERT_EQ(listWithOptions(keeper, "/plain", ListOptions{}, events.make("/plain")).error, Error::ZOK);
    /// 2, 3, 4: list requests with stats or data; 5: a plain one on the path of 4, which shares its watch.
    ASSERT_EQ(list(keeper, "/stat", ListRequestType::ALL, /* with_stat */ true, false, events.make("/stat")).error, Error::ZOK);
    ASSERT_EQ(list(keeper, "/data", ListRequestType::ALL, false, /* with_data */ true, events.make("/data")).error, Error::ZOK);
    ASSERT_EQ(listWithOptions(keeper, "/options", with_data_options, events.make("/options")).error, Error::ZOK);
    ASSERT_EQ(list(keeper, "/options", ListRequestType::ALL, false, false, events.make("/options")).error, Error::ZOK);

    for (const auto & dir : dirs)
        set(keeper, dir + "/child", "new");
    events.expect({0, 0, 1, 1, 1, 1});

    for (const auto & dir : dirs)
        create(keeper, dir + "/child2", "", /* is_ephemeral */ false);
    events.expect({1, 1, 1, 1, 1, 1});

    /// 6: a change of the listed node itself is not a change of its children.
    ASSERT_EQ(list(keeper, "/", ListRequestType::ALL, false, /* with_data */ true, events.make("/")).error, Error::ZOK);
    set(keeper, "/", "new");
    events.expect({1, 1, 1, 1, 1, 1, 0});
    create(keeper, "/new", "", /* is_ephemeral */ false);
    events.expect({1, 1, 1, 1, 1, 1, 1});

    /// 7: a callback set by both kinds of list request is called once.
    auto shared = events.make("/data");
    ASSERT_EQ(list(keeper, "/data", ListRequestType::ALL, false, false, shared).error, Error::ZOK);
    ASSERT_EQ(list(keeper, "/data", ListRequestType::ALL, false, /* with_data */ true, shared).error, Error::ZOK);
    create(keeper, "/data/child3", "", /* is_ephemeral */ false);
    events.expect({1, 1, 1, 1, 1, 1, 1, 1});

    /// 8: a node watch gets the type of the change.
    ASSERT_TRUE(exists(keeper, "/data/child", events.make("/data/child", CHANGED)));
    set(keeper, "/data/child", "newer");
    events.expect({1, 1, 1, 1, 1, 1, 1, 1, 1});
}

TEST(TestKeeperTest, ListWatchFiresOnSetOfChildOnlyIfFilterPassesIt)
{
    TestKeeper keeper = makeKeeper();

    create(keeper, "/dir", "", /* is_ephemeral */ false);
    create(keeper, "/dir/persistent", "", /* is_ephemeral */ false);
    create(keeper, "/dir/ephemeral", "", /* is_ephemeral */ true);

    WatchEvents events;
    ListOptions ephemeral_only;
    ephemeral_only.filter = ListRequestType::EPHEMERAL_ONLY;
    ephemeral_only.with_data = true;
    const auto persistent_only = [&]
    {
        return list(keeper, "/dir", ListRequestType::PERSISTENT_ONLY, false, /* with_data */ true, events.make("/dir")).error;
    };

    /// 0
    ASSERT_EQ(persistent_only(), Error::ZOK);
    set(keeper, "/dir/ephemeral", "new");
    events.expect({0});
    set(keeper, "/dir/persistent", "new");
    events.expect({1});

    /// 1
    ASSERT_EQ(listWithOptions(keeper, "/dir", ephemeral_only, events.make("/dir")).error, Error::ZOK);
    set(keeper, "/dir/persistent", "new");
    events.expect({1, 0});
    set(keeper, "/dir/ephemeral", "new");
    events.expect({1, 1});

    /// 2, 3 and 4, 5: requests with different filters on one path fire on a change of any child.
    ASSERT_EQ(persistent_only(), Error::ZOK);
    ASSERT_EQ(listWithOptions(keeper, "/dir", ephemeral_only, events.make("/dir")).error, Error::ZOK);
    set(keeper, "/dir/ephemeral", "new");
    events.expect({1, 1, 1, 1});
    ASSERT_EQ(persistent_only(), Error::ZOK);
    ASSERT_EQ(listWithOptions(keeper, "/dir", ephemeral_only, events.make("/dir")).error, Error::ZOK);
    set(keeper, "/dir/persistent", "new");
    events.expect({1, 1, 1, 1, 1, 1});

    /// 6: creating a child fires it whatever the filter.
    ASSERT_EQ(persistent_only(), Error::ZOK);
    create(keeper, "/dir/ephemeral2", "", /* is_ephemeral */ true);
    events.expect({1, 1, 1, 1, 1, 1, 1});

    /// 7: a filter value TestKeeper does not know leaves out no child, as in Keeper.
    const auto unknown_filter = static_cast<ListRequestType>(3); // NOLINT(clang-analyzer-optin.core.EnumCastOutOfRange)
    const auto response = list(keeper, "/dir", unknown_filter, false, /* with_data */ true, events.make("/dir"));
    ASSERT_EQ(response.error, Error::ZOK);
    EXPECT_EQ(response.names, std::vector<String>({"ephemeral", "ephemeral2", "persistent"}));
    set(keeper, "/dir/ephemeral", "new");
    events.expect({1, 1, 1, 1, 1, 1, 1, 1});
}

TEST(TestKeeperTest, ListWatchFiresOnRemoval)
{
    TestKeeper keeper = makeKeeper();

    for (const auto * path : {"/gone", "/dir", "/dir/sub", "/dir/sub/leaf"})
        create(keeper, path, "", /* is_ephemeral */ false);

    WatchEvents events;
    ASSERT_EQ(list(keeper, "/gone", ListRequestType::ALL, false, false, events.make("/gone", DELETED)).error, Error::ZOK);
    ASSERT_EQ(list(keeper, "/gone", ListRequestType::ALL, false, /* with_data */ true, events.make("/gone", DELETED)).error, Error::ZOK);
    ASSERT_EQ(list(keeper, "/dir", ListRequestType::ALL, false, false, events.make("/dir")).error, Error::ZOK);
    ASSERT_EQ(list(keeper, "/dir", ListRequestType::ALL, false, /* with_data */ true, events.make("/dir")).error, Error::ZOK);
    /// 4: removing `leaf` changes the children of `sub` before `sub` itself is removed.
    ASSERT_EQ(list(keeper, "/dir/sub", ListRequestType::ALL, false, false, events.make("/dir/sub")).error, Error::ZOK);
    /// 5: a node that does not exist is not removed.
    ASSERT_FALSE(exists(keeper, "/dir/sub/none", events.make("/dir/sub/none", CREATED)));

    std::promise<RemoveResponse> sink;
    keeper.remove("/gone", /* version */ -1, [&](const auto & response) { sink.set_value(response); });
    ASSERT_EQ(sink.get_future().get().error, Error::ZOK);
    ASSERT_EQ(removeRecursive(keeper, "/dir/sub", /* remove_nodes_limit */ 100).error, Error::ZOK);
    events.expect({1, 1, 1, 1, 1, 0});

    create(keeper, "/dir/sub", "", /* is_ephemeral */ false);
    create(keeper, "/dir/sub/none", "", /* is_ephemeral */ false);
    events.expect({1, 1, 1, 1, 1, 1});
}

TEST(TestKeeperTest, FinalizeExpiresListWatches)
{
    TestKeeper keeper = makeKeeper();

    create(keeper, "/dir", "", /* is_ephemeral */ false);

    std::vector<std::shared_ptr<std::atomic<size_t>>> counters;
    auto watch = [&]
    {
        auto counter = counters.emplace_back(std::make_shared<std::atomic<size_t>>(0));
        return std::make_shared<WatchCallback>([counter](const WatchResponse & response)
        {
            EXPECT_EQ(response.type, SESSION);
            EXPECT_EQ(response.state, EXPIRED_SESSION);
            ++*counter;
        });
    };

    ASSERT_TRUE(exists(keeper, "/dir", watch()));
    ASSERT_EQ(list(keeper, "/dir", ListRequestType::ALL, false, false, watch()).error, Error::ZOK);
    ASSERT_EQ(list(keeper, "/dir", ListRequestType::ALL, false, /* with_data */ true, watch()).error, Error::ZOK);
    auto shared = watch();
    ASSERT_EQ(list(keeper, "/dir", ListRequestType::ALL, false, false, shared).error, Error::ZOK);
    ASSERT_EQ(list(keeper, "/dir", ListRequestType::ALL, false, /* with_data */ true, shared).error, Error::ZOK);

    keeper.finalize("test");

    for (size_t i = 0; i < counters.size(); ++i)
        EXPECT_EQ(counters[i]->load(), 1u) << "watch " << i;
}

TEST(TestKeeperTest, NoOpWritesTriggerNoWatches)
{
    TestKeeper keeper = makeKeeper();

    create(keeper, "/dir", "", /* is_ephemeral */ false);
    create(keeper, "/dir/child", "", /* is_ephemeral */ false);

    WatchEvents events;
    ASSERT_EQ(list(keeper, "/dir", ListRequestType::ALL, false, false, events.make("/dir")).error, Error::ZOK);
    ASSERT_EQ(list(keeper, "/dir", ListRequestType::ALL, false, /* with_data */ true, events.make("/dir")).error, Error::ZOK);
    ASSERT_TRUE(exists(keeper, "/dir/child", events.make("/dir/child", CHANGED)));
    ASSERT_FALSE(exists(keeper, "/dir/missing", events.make("/dir/missing", CREATED)));

    std::promise<MultiResponse> sink;
    keeper.multi(
        Requests{
            zkutil::makeCreateRequest("/dir/child", "", zkutil::CreateMode::Persistent, /* ignore_if_exists */ true),
            zkutil::makeRemoveRequest("/dir/missing", /* version */ -1, /* try_remove */ true)},
        [&](const auto & response) { sink.set_value(response); });
    ASSERT_EQ(sink.get_future().get().error, Error::ZOK);
    ASSERT_EQ(removeRecursive(keeper, "/dir/missing", /* remove_nodes_limit */ 100).error, Error::ZOK);
    events.expect({0, 0, 0, 0});

    set(keeper, "/dir/child", "new");
    create(keeper, "/dir/missing", "", /* is_ephemeral */ false);
    events.expect({1, 1, 1, 1});
}
