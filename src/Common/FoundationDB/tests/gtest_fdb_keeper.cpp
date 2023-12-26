#include "config.h"

#if USE_FDB

#include <cstddef>
#include <exception>
#include <future>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <Core/BackgroundSchedulePool.h>
#include <Parsers/ASTLiteral.h>
#include <fmt/core.h>
#include <foundationdb/fdb_c.h>
#include <gtest/gtest.h>
#include "Common/FoundationDB/tests/gtest_fdb_common.h"
#include <Common/Exception.h>
#include <Common/FoundationDB/FDBKeeper.h>
#include <Common/FoundationDB/FoundationDBCommon.h>
#include <Common/FoundationDB/FoundationDBHelpers.h>
#include <Common/FoundationDB/fdb_error_definitions.h>
#include <Common/FoundationDB/internal/KeeperConst.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeperArgs.h>


#pragma clang diagnostic ignored "-Wunused-macros"

using namespace DB;
using namespace Coordination;
using namespace std::placeholders;
using namespace std::chrono_literals;

namespace CurrentMetrics
{
extern const Metric BackgroundCommonPoolTask;
extern const Metric BackgroundCommonPoolSize;
}

class FDBKeeperSuite : public FDBFixture
{
public:
    FDBKeeperSuite()
    {
        zookeeperArgs.fdb_prefix = key_prefix;
        zookeeperArgs.fdb_cluster = cluster_file;
    }

    void SetUp() override
    {
        SKIP_IF_NO_FDB();
        clear();
        keeper = createKeeper();
    }

    void TearDown() override { keeper.reset(); }

protected:
    std::shared_ptr<FDBKeeper> keeper;
    zkutil::ZooKeeperArgs zookeeperArgs;
    Poco::Logger * log = &Poco::Logger::get("GTestFDBKeeperSuite");

    std::shared_ptr<FDBKeeper> createKeeper() { return std::make_shared<FDBKeeper>(zookeeperArgs); }

    template <typename Response>
    auto bindPromise(std::promise<Response> && promise)
    {
        auto promise_ptr = std::make_shared<std::promise<Response>>(std::move(promise));
        return [promise_ptr](const Response resp) { promise_ptr->set_value(resp); };
    }

    template <typename Response>
    auto bindPromisePtr(std::promise<Response> && promise)
    {
        auto promise_ptr = std::make_shared<std::promise<Response>>(std::move(promise));
        return std::make_shared<std::function<void(const Response &)>>([promise_ptr](const Response resp)
                                                                       { promise_ptr->set_value(resp); });
    }

    template <typename R>
    R wait(std::future<R> & future)
    {
        return wait(future, 1s);
    }

    template <typename R, typename Duration>
    R wait(std::future<R> & future, Duration duration)
    {
        if (future.wait_for(duration) != std::future_status::ready)
            throw std::runtime_error("future timeout");
        return future.get();
    }
};

class FDBKeeperChrootSuite : public FDBKeeperSuite, public testing::WithParamInterface<std::string_view>
{
public:
    FDBKeeperChrootSuite() { zookeeperArgs.chroot = GetParam(); }

    void SetUp() override
    {
        SKIP_IF_NO_FDB();
        clear();
        initRoot();
        keeper = createKeeper();
    }

protected:
    void initRoot()
    {
        if (zookeeperArgs.chroot.empty())
            return;

        auto chroot = zookeeperArgs.chroot;
        if (chroot.back() == '/')
            chroot.pop_back();
        auto root_keeper_args = zookeeperArgs;
        root_keeper_args.chroot = "";
        auto root_keeper = std::make_shared<FDBKeeper>(root_keeper_args);
        std::promise<bool> promise;
        auto future = promise.get_future();
        root_keeper->create(
            chroot,
            "",
            false,
            false,
            {},
            [&](const CreateResponse & resp) { promise.set_value(resp.error == Error::ZOK || resp.error == Error::ZNODEEXISTS); });
        ASSERT_TRUE(wait(future)) << "Failed to create chroot";
    }
};

INSTANTIATE_TEST_SUITE_P(FDBKeeperChroots, FDBKeeperChrootSuite, testing::Values("", "/test"));
// INSTANTIATE_TEST_SUITE_P(FDBKeeperChroots, FDBKeeperChrootSuite, testing::Values("", "/", "/test", "/test/", "test", "test/"));

#define CALL_KEEPER(ACTION, RESPONSE, VAR, ...) \
    std::promise<Coordination::RESPONSE##Response> VAR##_promise; \
    auto VAR##_future = VAR##_promise.get_future(); \
    keeper->ACTION(__VA_ARGS__, bindPromise(std::move(VAR##_promise)))

#define CALL_KEEPER_NON_WATCH(ACTION, RESPONSE, VAR, ...) \
    std::promise<Coordination::RESPONSE##Response> VAR##_promise; \
    auto VAR##_future = VAR##_promise.get_future(); \
    keeper->ACTION(__VA_ARGS__, bindPromise(std::move(VAR##_promise)), {})

#define CALL_KEEPER_WATCH(ACTION, RESPONSE, VAR, ...) \
    std::promise<Coordination::RESPONSE##Response> VAR##_promise; \
    auto VAR##_future = VAR##_promise.get_future(); \
    std::promise<Coordination::WatchResponse> VAR##_watch_promise; \
    auto VAR##_watch_future = VAR##_watch_promise.get_future(); \
    keeper->ACTION(__VA_ARGS__, bindPromise(std::move(VAR##_promise)), bindPromisePtr(std::move(VAR##_watch_promise)))

#define KEEPER_CREATE(VAR, ...) CALL_KEEPER(create, Create, VAR, __VA_ARGS__)
#define KEEPER_SET(VAR, ...) CALL_KEEPER(set, Set, VAR, __VA_ARGS__)
#define KEEPER_CHECK(VAR, ...) CALL_KEEPER(check, Check, VAR, __VA_ARGS__)
#define KEEPER_EXISTS(VAR, ...) CALL_KEEPER_NON_WATCH(exists, Exists, VAR, __VA_ARGS__)
#define KEEPER_GET(VAR, ...) CALL_KEEPER_NON_WATCH(get, Get, VAR, __VA_ARGS__)
#define KEEPER_REMOVE(VAR, ...) CALL_KEEPER(remove, Remove, VAR, __VA_ARGS__)
#define KEEPER_LIST(VAR, ...) CALL_KEEPER_NON_WATCH(list, List, VAR, __VA_ARGS__)
#define KEEPER_MULTI(VAR, ...) CALL_KEEPER(multi, Multi, VAR, __VA_ARGS__)

#define KEEPER_EXISTS_WATCH(VAR, ...) CALL_KEEPER_WATCH(exists, Exists, VAR, __VA_ARGS__)
#define KEEPER_GET_WATCH(VAR, ...) CALL_KEEPER_WATCH(get, Get, VAR, __VA_ARGS__)
#define KEEPER_LIST_WATCH(VAR, ...) CALL_KEEPER_WATCH(list, List, VAR, __VA_ARGS__)

#define CONCATENATE_DETAIL(x, y) x##y
#define CONCATENATE(x, y) CONCATENATE_DETAIL(x, y)
#define EXTRACT_RESPONSE(TYPE, RESP, MULTI_RESP, I) auto RESP = std::dynamic_pointer_cast<CONCATENATE(TYPE, Response)>((MULTI_RESP).responses[(I)])

#define USE_KEEPER(KEEPER) keeper = std::move(KEEPER)

TEST_P(FDBKeeperChrootSuite, CreateShouldSuccess)
{
    KEEPER_CREATE(a, "/a", "abc", false, false, {});
    KEEPER_CREATE(b, "/b", "abc", false, false, {});

    EXPECT_EQ(wait(a_future).error, Error::ZOK);
    EXPECT_EQ(wait(b_future).error, Error::ZOK);

    KEEPER_CREATE(c, "/a/c", "abc", false, false, {});
    KEEPER_CREATE(d, "/a/d", "abc", false, false, {});
    EXPECT_EQ(wait(c_future).error, Error::ZOK);
    EXPECT_EQ(wait(d_future).error, Error::ZOK);
}

TEST_P(FDBKeeperChrootSuite, CreateChildShouldSuccess)
{
    KEEPER_CREATE(a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(a_future).error, Error::ZOK);

    KEEPER_CREATE(b, "/a/b", "abc", false, false, {});
    ASSERT_EQ(wait(b_future).error, Error::ZOK);
}

TEST_P(FDBKeeperChrootSuite, CreateShouldFailedIfParentNotExists)
{
    KEEPER_CREATE(a, "/a/b", "abc", false, false, {});
    ASSERT_EQ(wait(a_future).error, Error::ZNONODE);
}

TEST_P(FDBKeeperChrootSuite, CreateShouldFailedIfExists)
{
    KEEPER_CREATE(a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(a_future).error, Error::ZOK);

    KEEPER_CREATE(b, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(b_future).error, Error::ZNODEEXISTS);
}

TEST_P(FDBKeeperChrootSuite, CreateRootStatShouldCorrect)
{
    KEEPER_CREATE(create_b, "/b", "abc", false, false, {});
    KEEPER_CREATE(create_a, "/a", "abc", false, false, {});

    EXPECT_EQ(wait(create_a_future).error, Error::ZOK);
    ASSERT_EQ(wait(create_b_future).error, Error::ZOK);

    KEEPER_GET(root, "/");
    KEEPER_GET(a, "/a");
    auto resp_root = wait(root_future);
    auto resp_a = wait(a_future);

    ASSERT_EQ(resp_root.error, Error::ZOK);
    ASSERT_EQ(resp_a.error, Error::ZOK);

    EXPECT_EQ(resp_root.stat.cversion, 2);
    EXPECT_EQ(resp_root.stat.numChildren, 2);

    EXPECT_EQ(resp_a.stat.czxid, resp_root.stat.pzxid);
}

TEST_P(FDBKeeperChrootSuite, CreateParentStatShouldCorrect)
{
    KEEPER_CREATE(create_a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(create_a_future).error, Error::ZOK);

    KEEPER_EXISTS(a_old, "/a");
    auto a_old = wait(a_old_future);
    ASSERT_EQ(a_old.error, Error::ZOK);
    ASSERT_EQ(a_old.stat.cversion, 0);
    ASSERT_EQ(a_old.stat.version, 0);

    KEEPER_CREATE(create_b, "/a/b", "abc", false, false, {});
    ASSERT_EQ(wait(create_b_future).error, Error::ZOK);

    KEEPER_CREATE(create_c, "/a/c", "abc", false, false, {});
    ASSERT_EQ(wait(create_c_future).error, Error::ZOK);

    KEEPER_GET(a, "/a");
    KEEPER_GET(c, "/a/c");
    auto resp_a = wait(a_future);
    auto resp_c = wait(c_future);

    ASSERT_EQ(resp_a.error, Error::ZOK);
    ASSERT_EQ(resp_c.error, Error::ZOK);

    EXPECT_EQ(resp_a.stat.cversion, 2);
    EXPECT_EQ(resp_a.stat.numChildren, 2);

    EXPECT_EQ(resp_c.stat.czxid, resp_a.stat.pzxid);
}

TEST_P(FDBKeeperChrootSuite, CreateSequentialShouldSuccess)
{
    KEEPER_CREATE(create_a, "/a", "abc", false, true, {});
    auto create_a_resp = wait(create_a_future);
    ASSERT_EQ(create_a_resp.error, Error::ZOK);

    KEEPER_CREATE(create_b, "/b", "abc", false, false, {});
    auto create_b_resp = wait(create_b_future);
    ASSERT_EQ(create_b_resp.error, Error::ZOK);

    KEEPER_CREATE(create_c, "/c", "abc", false, true, {});
    auto create_c_resp = wait(create_c_future);
    ASSERT_EQ(create_c_resp.error, Error::ZOK);

    EXPECT_EQ(create_a_resp.path_created, "/a0000000000");
    EXPECT_EQ(create_b_resp.path_created, "/b");
    EXPECT_EQ(create_c_resp.path_created, "/c0000000002");
}

TEST_P(FDBKeeperChrootSuite, SequentialNumShouldNotIncreaseByDelete)
{
    KEEPER_CREATE(create_a, "/a", "abc", false, true, {});
    auto create_a_resp = wait(create_a_future);
    ASSERT_EQ(create_a_resp.error, Error::ZOK);
    ASSERT_EQ(create_a_resp.path_created, "/a0000000000");

    KEEPER_REMOVE(rm_a, "/a0000000000", -1);
    ASSERT_EQ(wait(rm_a_future).error, Error::ZOK);

    KEEPER_CREATE(create_b, "/b", "abc", false, false, {});
    auto create_b_resp = wait(create_b_future);
    ASSERT_EQ(create_b_resp.error, Error::ZOK);
    ASSERT_EQ(create_b_resp.path_created, "/b");

    KEEPER_REMOVE(rm_b, "/b", -1);
    ASSERT_EQ(wait(rm_b_future).error, Error::ZOK);

    KEEPER_CREATE(create_c, "/c", "abc", false, true, {});
    auto create_c_resp = wait(create_c_future);
    ASSERT_EQ(create_c_resp.error, Error::ZOK);
    ASSERT_EQ(create_c_resp.path_created, "/c0000000002");
}


TEST_P(FDBKeeperChrootSuite, Get)
{
    KEEPER_CREATE(a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(a_future).error, Error::ZOK);

    KEEPER_GET(get, "/a");
    auto resp = wait(get_future);
    ASSERT_EQ(resp.error, Error::ZOK);
    EXPECT_EQ(resp.data, "abc");
    EXPECT_EQ(resp.stat.version, 0);
    EXPECT_EQ(resp.stat.cversion, 0);
    EXPECT_EQ(resp.stat.dataLength, 3);
    EXPECT_EQ(resp.stat.numChildren, 0);
}

TEST_P(FDBKeeperChrootSuite, GetNotExists)
{
    KEEPER_GET(a, "/a");
    ASSERT_EQ(wait(a_future).error, Error::ZNONODE);

    KEEPER_GET(b, "/a/b");
    ASSERT_EQ(wait(b_future).error, Error::ZNONODE);
}

TEST_P(FDBKeeperChrootSuite, Exists)
{
    KEEPER_CREATE(a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(a_future).error, Error::ZOK);

    KEEPER_EXISTS(exists, "/a");
    ASSERT_EQ(wait(exists_future).error, Error::ZOK);
}

TEST_P(FDBKeeperChrootSuite, ExistsNotExists)
{
    KEEPER_EXISTS(exists, "/a");
    ASSERT_EQ(wait(exists_future).error, Error::ZNONODE);
}

TEST_P(FDBKeeperChrootSuite, Set)
{
    KEEPER_CREATE(create_a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(create_a_future).error, Error::ZOK);

    KEEPER_SET(set_a, "/a", "123456", 0);
    auto set_resp = wait(set_a_future);
    ASSERT_EQ(set_resp.error, Error::ZOK);
    EXPECT_EQ(set_resp.stat.version, 1);
    EXPECT_EQ(set_resp.stat.dataLength, 6);
    EXPECT_NE(set_resp.stat.czxid, 0);
    EXPECT_NE(set_resp.stat.mzxid, 0);
    EXPECT_NE(set_resp.stat.pzxid, 0);

    KEEPER_GET(get_a, "/a");
    auto get_resp = wait(get_a_future);
    ASSERT_EQ(get_resp.error, Error::ZOK);
    EXPECT_EQ(get_resp.stat.version, 1);
    EXPECT_EQ(get_resp.stat.dataLength, 6);
    EXPECT_EQ(get_resp.data, "123456");
}

TEST_P(FDBKeeperChrootSuite, SetBadVersion)
{
    KEEPER_CREATE(create_a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(create_a_future).error, Error::ZOK);

    KEEPER_SET(set_a, "/a", "123456", 1);
    auto set_resp = wait(set_a_future);
    ASSERT_EQ(set_resp.error, Error::ZBADVERSION);
}

TEST_P(FDBKeeperChrootSuite, SetNotExists)
{
    KEEPER_SET(set_a, "/a", "123456", 0);
    auto set_resp = wait(set_a_future);
    ASSERT_EQ(set_resp.error, Error::ZNONODE);
}

TEST_P(FDBKeeperChrootSuite, CheckAfterCreate)
{
    KEEPER_CREATE(create_a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(create_a_future).error, Error::ZOK);

    KEEPER_CHECK(check_a, "/a", 0);
    ASSERT_EQ(wait(check_a_future).error, Error::ZOK);
}

TEST_P(FDBKeeperChrootSuite, CheckAfterSet)
{
    KEEPER_CREATE(create_a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(create_a_future).error, Error::ZOK);

    KEEPER_SET(set_a, "/a", "123456", 0);
    ASSERT_EQ(wait(set_a_future).error, Error::ZOK);

    KEEPER_CHECK(check_a, "/a", 1);
    ASSERT_EQ(wait(check_a_future).error, Error::ZOK);
}

TEST_P(FDBKeeperChrootSuite, CheckBadVersion)
{
    KEEPER_CREATE(create_a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(create_a_future).error, Error::ZOK);

    KEEPER_SET(set_a, "/a", "123456", 0);
    ASSERT_EQ(wait(set_a_future).error, Error::ZOK);

    KEEPER_CHECK(check_a, "/a", 2);
    ASSERT_EQ(wait(check_a_future).error, Error::ZBADVERSION);
}

TEST_P(FDBKeeperChrootSuite, CheckAnyVersion)
{
    KEEPER_CREATE(create_a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(create_a_future).error, Error::ZOK);

    KEEPER_SET(set_a, "/a", "123456", 0);
    ASSERT_EQ(wait(set_a_future).error, Error::ZOK);

    KEEPER_CHECK(check_a, "/a", -1);
    ASSERT_EQ(wait(check_a_future).error, Error::ZOK);

    KEEPER_CHECK(check_b, "/b", -1);
    ASSERT_EQ(wait(check_b_future).error, Error::ZNONODE);
}

TEST_P(FDBKeeperChrootSuite, Remove)
{
    KEEPER_CREATE(a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(a_future).error, Error::ZOK);

    KEEPER_REMOVE(remove, "/a", -1);
    ASSERT_EQ(wait(remove_future).error, Error::ZOK);

    KEEPER_EXISTS(exists, "/a");
    ASSERT_EQ(wait(exists_future).error, Error::ZNONODE);
}

TEST_P(FDBKeeperChrootSuite, RemoveParentStat)
{
    KEEPER_CREATE(create_a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(create_a_future).error, Error::ZOK);

    KEEPER_CREATE(create_b, "/a/b", "abc", false, false, {});
    ASSERT_EQ(wait(create_b_future).error, Error::ZOK);

    KEEPER_REMOVE(remove, "/a/b", -1);
    ASSERT_EQ(wait(remove_future).error, Error::ZOK);

    KEEPER_GET(a, "/a");
    auto resp = wait(a_future);
    EXPECT_EQ(resp.stat.cversion, 2);
    EXPECT_EQ(resp.stat.numChildren, 0);
}

TEST_P(FDBKeeperChrootSuite, RemoveWithVersion)
{
    KEEPER_CREATE(a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(a_future).error, Error::ZOK);

    KEEPER_REMOVE(remove, "/a", 0);
    ASSERT_EQ(wait(remove_future).error, Error::ZOK);

    KEEPER_EXISTS(exists, "/a");
    ASSERT_EQ(wait(exists_future).error, Error::ZNONODE);
}

TEST_P(FDBKeeperChrootSuite, RemoveWithBadVersion)
{
    KEEPER_CREATE(a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(a_future).error, Error::ZOK);

    KEEPER_REMOVE(remove, "/a", 100);
    ASSERT_EQ(wait(remove_future).error, Error::ZBADVERSION);
}

TEST_P(FDBKeeperChrootSuite, RemoveWithChildren)
{
    KEEPER_CREATE(a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(a_future).error, Error::ZOK);

    KEEPER_CREATE(b, "/a/b", "abc", false, false, {});
    ASSERT_EQ(wait(b_future).error, Error::ZOK);

    KEEPER_REMOVE(remove, "/a", -1);
    ASSERT_EQ(wait(remove_future).error, Error::ZNOTEMPTY);
}

TEST_P(FDBKeeperChrootSuite, List)
{
    KEEPER_CREATE(create_a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(create_a_future).error, Error::ZOK);

    KEEPER_CREATE(create_b, "/a/b", "abc", false, false, {});
    ASSERT_EQ(wait(create_b_future).error, Error::ZOK);

    KEEPER_CREATE(create_c, "/a/c", "abc", false, false, {});
    ASSERT_EQ(wait(create_c_future).error, Error::ZOK);

    KEEPER_LIST(list, "/a", ListRequestType::ALL);
    auto resp = wait(list_future);
    ASSERT_EQ(resp.error, Error::ZOK);
    ASSERT_EQ(resp.names.size(), 2);
    ASSERT_EQ(resp.names[0], "b");
    ASSERT_EQ(resp.names[1], "c");
    ASSERT_EQ(resp.stat.numChildren, 2);
}

TEST_P(FDBKeeperChrootSuite, ListNotExists)
{
    KEEPER_LIST(list, "/a", ListRequestType::ALL);
    ASSERT_EQ(wait(list_future).error, Error::ZNONODE);
}

TEST_P(FDBKeeperChrootSuite, WatchExists)
{
    KEEPER_CREATE(a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(a_future).error, Error::ZOK);

    KEEPER_EXISTS_WATCH(exists, "/a");
    ASSERT_EQ(wait(exists_future).error, Error::ZOK);

    KEEPER_REMOVE(rm, "/a", -1);
    ASSERT_EQ(wait(rm_future).error, Error::ZOK);

    auto resp = wait(exists_watch_future);
    ASSERT_EQ(resp.type, Event::DELETED);
    ASSERT_EQ(resp.path, "/a");
}

TEST_P(FDBKeeperChrootSuite, WatchNonExists)
{
    KEEPER_EXISTS_WATCH(exists, "/a");
    ASSERT_EQ(wait(exists_future).error, Error::ZNONODE);

    KEEPER_CREATE(a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(a_future).error, Error::ZOK);

    auto resp = wait(exists_watch_future);
    ASSERT_EQ(resp.type, Event::CREATED);
    ASSERT_EQ(resp.path, "/a");
}

TEST_P(FDBKeeperChrootSuite, WatchListRootAddChild)
{
    KEEPER_LIST_WATCH(list, "/", ListRequestType::ALL);
    ASSERT_EQ(wait(list_future).error, Error::ZOK);

    KEEPER_CREATE(create, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(create_future).error, Error::ZOK);

    auto resp = wait(list_watch_future);
    ASSERT_EQ(resp.type, Event::CHILD);
    ASSERT_EQ(resp.path, "/");
}

TEST_P(FDBKeeperChrootSuite, WatchListAddChild)
{
    KEEPER_CREATE(create, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(create_future).error, Error::ZOK);

    KEEPER_LIST_WATCH(list, "/a", ListRequestType::ALL);
    ASSERT_EQ(wait(list_future).error, Error::ZOK);

    KEEPER_CREATE(create_b, "/a/b", "abc", false, false, {});
    ASSERT_EQ(wait(create_b_future).error, Error::ZOK);

    auto resp = wait(list_watch_future);
    ASSERT_EQ(resp.type, Event::CHILD);
    ASSERT_EQ(resp.path, "/a");
}

TEST_P(FDBKeeperChrootSuite, WatchListRootDelChild)
{
    KEEPER_CREATE(create, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(create_future).error, Error::ZOK);

    KEEPER_LIST_WATCH(list, "/", ListRequestType::ALL);
    ASSERT_EQ(wait(list_future).error, Error::ZOK);

    KEEPER_REMOVE(rm, "/a", -1);
    ASSERT_EQ(wait(rm_future).error, Error::ZOK);

    auto resp = wait(list_watch_future);
    ASSERT_EQ(resp.type, Event::CHILD);
    ASSERT_EQ(resp.path, "/");
}

TEST_P(FDBKeeperChrootSuite, WatchListDelChild)
{
    KEEPER_CREATE(create, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(create_future).error, Error::ZOK);

    KEEPER_CREATE(create_b, "/a/b", "abc", false, false, {});
    ASSERT_EQ(wait(create_b_future).error, Error::ZOK);

    KEEPER_LIST_WATCH(list, "/a", ListRequestType::ALL);
    ASSERT_EQ(wait(list_future).error, Error::ZOK);

    KEEPER_REMOVE(rm, "/a/b", -1);
    ASSERT_EQ(wait(rm_future).error, Error::ZOK);

    auto resp = wait(list_watch_future);
    ASSERT_EQ(resp.type, Event::CHILD);
    ASSERT_EQ(resp.path, "/a");
}

TEST_P(FDBKeeperChrootSuite, WatchListRemoveEmptyList)
{
    KEEPER_CREATE(create, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(create_future).error, Error::ZOK);

    KEEPER_LIST_WATCH(list, "/a", ListRequestType::ALL);
    ASSERT_EQ(wait(list_future).error, Error::ZOK);

    KEEPER_REMOVE(rm, "/a", -1);
    ASSERT_EQ(wait(rm_future).error, Error::ZOK);

    auto resp = wait(list_watch_future);
    ASSERT_EQ(resp.type, Event::CHILD);
    ASSERT_EQ(resp.path, "/a");
}

TEST_P(FDBKeeperChrootSuite, WatchGet)
{
    KEEPER_CREATE(a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(a_future).error, Error::ZOK);

    KEEPER_GET_WATCH(get, "/a");
    ASSERT_EQ(wait(get_future).error, Error::ZOK);

    KEEPER_SET(set, "/a", "efg", 0);
    ASSERT_EQ(wait(set_future).error, Error::ZOK);

    auto resp = wait(get_watch_future);
    ASSERT_EQ(resp.type, Event::CHANGED);
    ASSERT_EQ(resp.path, "/a");
}

TEST_P(FDBKeeperChrootSuite, WatchRemoved)
{
    KEEPER_CREATE(a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(a_future).error, Error::ZOK);

    KEEPER_GET_WATCH(get, "/a");
    ASSERT_EQ(wait(get_future).error, Error::ZOK);

    KEEPER_REMOVE(rm, "/a", -1);
    ASSERT_EQ(wait(rm_future).error, Error::ZOK);

    auto resp = wait(get_watch_future);
    ASSERT_EQ(resp.type, Event::CHANGED);
    ASSERT_EQ(resp.path, "/a");
}

TEST_P(FDBKeeperChrootSuite, ListHugeChildren)
{
    const size_t children_size = 10'000;
    const size_t create_batch = 1'000;

    std::vector<std::future<CreateResponse>> futures;
    futures.reserve(children_size);

    for (size_t i = 0; i < children_size;)
    {
        size_t batch_upper = std::min(i + (i > 0 ? create_batch : 100), children_size);
        LOG_TRACE(log, "creating {} to {}", i, batch_upper);
        for (; i < batch_upper; i++)
        {
            KEEPER_CREATE(create, "/very_long_name_node_" + std::to_string(i), "abc", false, false, {});
            futures.emplace_back(std::move(create_future));
        }
        for (auto & future : futures)
            ASSERT_EQ(wait(future, 5s).error, Error::ZOK);
        futures.clear();
    }
    LOG_TRACE(log, "created {} node(s)", children_size);

    KEEPER_LIST(list, "/", ListRequestType::ALL);
    auto resp = wait(list_future, 5s);
    ASSERT_EQ(resp.names.size(), children_size);
}

TEST_P(FDBKeeperChrootSuite, MultiRemainResponsesShouldSetErrorWhenFail)
{
    using namespace zkutil;

    Requests reqs = {
        makeCreateRequest("/a", "abc", CreateMode::Persistent),
        makeCreateRequest("/a", "abc", CreateMode::Persistent),
        makeCreateRequest("/b", "abc", CreateMode::Persistent),
        makeCreateRequest("/c", "abc", CreateMode::Persistent),
    };

    KEEPER_MULTI(multi, reqs);
    auto resp = wait(multi_future);

    ASSERT_EQ(resp.error, Error::ZNODEEXISTS);
    ASSERT_EQ(resp.responses.size(), 4);

    EXTRACT_RESPONSE(Create, resp_b, resp, 1);
    EXTRACT_RESPONSE(Create, resp_c, resp, 2);
    EXTRACT_RESPONSE(Create, resp_d, resp, 3);
    ASSERT_EQ(resp_b->error, Error::ZNODEEXISTS);
    ASSERT_EQ(resp_c->error, Error::ZRUNTIMEINCONSISTENCY);
    ASSERT_EQ(resp_d->error, Error::ZRUNTIMEINCONSISTENCY);
};

TEST_P(FDBKeeperChrootSuite, MultiCreateMultiNode)
{
    using namespace zkutil;

    Requests reqs = {
        makeCreateRequest("/a", "abc", CreateMode::Persistent),
        makeCreateRequest("/b", "abc", CreateMode::Persistent),
    };

    KEEPER_MULTI(multi, reqs);
    auto resp = wait(multi_future);

    ASSERT_EQ(resp.error, Error::ZOK);
    ASSERT_EQ(resp.responses.size(), 2);

    EXTRACT_RESPONSE(Create, resp_a, resp, 0);
    EXTRACT_RESPONSE(Create, resp_b, resp, 1);
    ASSERT_EQ(resp_a->error, Error::ZOK);
    ASSERT_EQ(resp_a->path_created, "/a");
    ASSERT_EQ(resp_b->error, Error::ZOK);
    ASSERT_EQ(resp_b->path_created, "/b");

    KEEPER_GET(get_root, "/");
    auto stat_root = wait(get_root_future);
    ASSERT_EQ(stat_root.stat.numChildren, 2);
};

TEST_P(FDBKeeperChrootSuite, MultiCreateMultiSequentialNode)
{
    using namespace zkutil;

    Requests reqs = {
        makeCreateRequest("/a", "abc", CreateMode::PersistentSequential),
        makeCreateRequest("/a", "abc", CreateMode::PersistentSequential),
        makeCreateRequest("/a", "abc", CreateMode::PersistentSequential),
    };

    KEEPER_MULTI(multi, reqs);
    auto resp = wait(multi_future);

    ASSERT_EQ(resp.error, Error::ZOK);
    ASSERT_EQ(resp.responses.size(), 3);
};

TEST_P(FDBKeeperChrootSuite, MultiCreateMultiMixSequentialNode)
{
    using namespace zkutil;

    Requests reqs = {
        makeCreateRequest("/a", "abc", CreateMode::Persistent),
        makeCreateRequest("/b", "abc", CreateMode::Persistent),
        makeCreateRequest("/c", "abc", CreateMode::PersistentSequential),
    };

    KEEPER_MULTI(multi, reqs);
    auto resp = wait(multi_future);

    ASSERT_EQ(resp.error, Error::ZOK);
    ASSERT_EQ(resp.responses.size(), 3);
};

TEST_P(FDBKeeperChrootSuite, MultiCreateSameZNode)
{
    using namespace zkutil;

    Requests reqs = {
        makeCreateRequest("/a", "abc", CreateMode::Persistent),
        makeCreateRequest("/a", "abc", CreateMode::Persistent),
    };

    KEEPER_MULTI(multi, reqs);
    auto resp = wait(multi_future);

    ASSERT_EQ(resp.error, Error::ZNODEEXISTS);
    ASSERT_EQ(resp.responses.size(), 2);

    EXTRACT_RESPONSE(Create, resp_a, resp, 0);
    EXTRACT_RESPONSE(Create, resp_b, resp, 1);
    ASSERT_EQ(resp_a->error, Error::ZOK);
    ASSERT_EQ(resp_a->path_created, "/a");
    ASSERT_EQ(resp_b->error, Error::ZNODEEXISTS);
};

TEST_P(FDBKeeperChrootSuite, MultiCreateAndRemove)
{
    using namespace zkutil;

    Requests reqs = {
        makeCreateRequest("/a", "abc", CreateMode::Persistent),
        makeRemoveRequest("/a", -1),
    };

    KEEPER_MULTI(multi, reqs);
    auto resp = wait(multi_future);

    ASSERT_EQ(resp.error, Error::ZOK);
    ASSERT_EQ(resp.responses.size(), 2);
};

TEST_P(FDBKeeperChrootSuite, MultiCreateAndCheck)
{
    using namespace zkutil;

    Requests reqs = {
        makeCreateRequest("/a", "abc", CreateMode::Persistent),
        makeCheckRequest("/a", 0),
    };

    KEEPER_MULTI(multi, reqs);
    auto resp = wait(multi_future);

    ASSERT_EQ(resp.error, Error::ZOK);
    ASSERT_EQ(resp.responses.size(), 2);
};

TEST_P(FDBKeeperChrootSuite, MultiCreateAndSet)
{
    using namespace zkutil;

    Requests reqs = {
        makeCreateRequest("/a", "abc", CreateMode::Persistent),
        makeSetRequest("/a", "efg", 0),
    };

    KEEPER_MULTI(multi, reqs);
    auto resp = wait(multi_future);

    ASSERT_EQ(resp.error, Error::ZOK);
    ASSERT_EQ(resp.responses.size(), 2);

    EXTRACT_RESPONSE(Set, resp_set, resp, 1);
    EXPECT_NE(resp_set->stat.czxid, 0);
    EXPECT_NE(resp_set->stat.mzxid, 0);
    EXPECT_NE(resp_set->stat.pzxid, 0);
};

TEST_P(FDBKeeperChrootSuite, MultiSet)
{
    using namespace zkutil;

    KEEPER_CREATE(create_a, "/a", "abc", false, false, {});
    KEEPER_CREATE(create_b, "/b", "abc", false, false, {});
    ASSERT_EQ(wait(create_a_future).error, Error::ZOK);
    ASSERT_EQ(wait(create_b_future).error, Error::ZOK);

    Requests reqs = {
        makeSetRequest("/a", "efg", 0),
        makeSetRequest("/b", "hij", 0),
    };

    KEEPER_MULTI(multi, reqs);
    auto resp = wait(multi_future);

    ASSERT_EQ(resp.error, Error::ZOK);
    ASSERT_EQ(resp.responses.size(), 2);

    EXTRACT_RESPONSE(Set, resp_set, resp, 0);
    EXPECT_NE(resp_set->stat.czxid, 0);
    EXPECT_NE(resp_set->stat.mzxid, 0);
    EXPECT_NE(resp_set->stat.pzxid, 0);
    EXPECT_NE(resp_set->stat.mzxid, resp_set->stat.czxid);

    KEEPER_GET(get_a, "/a");
    KEEPER_GET(get_b, "/b");
    auto a = wait(get_a_future);
    auto b = wait(get_b_future);
    ASSERT_EQ(a.error, Error::ZOK);
    ASSERT_EQ(b.error, Error::ZOK);
    ASSERT_EQ(a.data, "efg");
    ASSERT_EQ(b.data, "hij");
};

TEST_P(FDBKeeperChrootSuite, MultiCreateChildrenAndSet)
{
    using namespace zkutil;

    KEEPER_CREATE(create_a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(create_a_future).error, Error::ZOK);

    Requests reqs = {
        makeCreateRequest("/a/b", "abc", CreateMode::Persistent),
        makeSetRequest("/a", "efg", 0),
    };

    KEEPER_MULTI(multi, reqs);
    auto resp = wait(multi_future);

    ASSERT_EQ(resp.error, Error::ZOK);
    ASSERT_EQ(resp.responses.size(), 2);

    EXTRACT_RESPONSE(Set, resp_set, resp, 1);
    EXPECT_NE(resp_set->stat.czxid, 0);
    EXPECT_NE(resp_set->stat.mzxid, 0);
    EXPECT_NE(resp_set->stat.pzxid, 0);
};

TEST_F(FDBKeeperSuite, KeeperShouldExpiredWhenSessionWasCleared)
{
    using namespace zkutil;

    LOG_TRACE(log, "Waiting session id...");
    std::this_thread::sleep_for(std::chrono::milliseconds(KEEPER_HEARTBEAT_INTERVAL_MS) * 2);
    auto session_id_old = keeper->getSessionID();
    ASSERT_NE(session_id_old, 0);

    clear();
    LOG_TRACE(log, "Waiting session id...");
    std::this_thread::sleep_for(std::chrono::milliseconds(KEEPER_HEARTBEAT_INTERVAL_MS) * 2);
    ASSERT_TRUE(keeper->isExpired());
};

TEST_P(FDBKeeperChrootSuite, CreateEphemeral)
{
    using namespace zkutil;

    KEEPER_CREATE(create_a, "/a", "abc", true, false, {});
    ASSERT_EQ(wait(create_a_future, 5s).error, Error::ZOK);

    KEEPER_GET(get_a, "/a");
    auto a = wait(get_a_future);
    ASSERT_EQ(a.error, Error::ZOK);
    ASSERT_EQ(a.stat.ephemeralOwner, keeper->getSessionID());
};

TEST_P(FDBKeeperChrootSuite, MultiCreateEphemeral)
{
    using namespace zkutil;

    Requests reqs = {
        makeCreateRequest("/a", "abc", CreateMode::Ephemeral),
        makeCreateRequest("/b", "abc", CreateMode::EphemeralSequential),
    };

    KEEPER_MULTI(multi, reqs);
    auto resp = wait(multi_future);

    ASSERT_EQ(resp.error, Error::ZOK);
    ASSERT_EQ(resp.responses.size(), 2);

    KEEPER_GET(get_a, "/a");
    auto a = wait(get_a_future);
    ASSERT_EQ(a.error, Error::ZOK);
    ASSERT_EQ(a.stat.ephemeralOwner, keeper->getSessionID());

    KEEPER_GET(get_b, "/b0000000001");
    auto b = wait(get_b_future);
    ASSERT_EQ(b.error, Error::ZOK);
    ASSERT_EQ(b.stat.ephemeralOwner, keeper->getSessionID());
};

TEST_P(FDBKeeperChrootSuite, CreateOnEphemeralParentShouldFailed)
{
    using namespace zkutil;

    KEEPER_CREATE(create_a, "/a", "abc", true, false, {});
    ASSERT_EQ(wait(create_a_future, 5s).error, Error::ZOK);

    KEEPER_CREATE(create_b, "/a/b", "abc", false, false, {});
    ASSERT_EQ(wait(create_b_future, 5s).error, Error::ZNOCHILDRENFOREPHEMERALS);
};

TEST_P(FDBKeeperChrootSuite, RemoveEphemeral)
{
    using namespace zkutil;

    KEEPER_CREATE(create_a, "/a", "abc", true, false, {});
    ASSERT_EQ(wait(create_a_future, 5s).error, Error::ZOK);

    KEEPER_REMOVE(rm_a, "/a", -1);
    ASSERT_EQ(wait(rm_a_future).error, Error::ZOK);
};

TEST_P(FDBKeeperChrootSuite, CleanerShouldWork)
{
    using namespace zkutil;

    auto keeper2 = createKeeper();

    KEEPER_CREATE(create_a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(create_a_future).error, Error::ZOK);
    KEEPER_CREATE(create_b, "/a/b", "abc", true, false, {});
    ASSERT_EQ(wait(create_b_future).error, Error::ZOK);
    KEEPER_CREATE(create_c, "/c", "abc", true, false, {});
    ASSERT_EQ(wait(create_c_future).error, Error::ZOK);

    keeper.reset();
    std::this_thread::sleep_for(std::chrono::milliseconds(KEEPER_SESSION_EXPIRE_AT_MS + KEEPER_CLEANER_ROUND_DURATION_MS) + 1s);

    USE_KEEPER(keeper2);
    KEEPER_GET(get_b, "/a/b");
    ASSERT_EQ(wait(get_b_future).error, Error::ZNONODE);
    KEEPER_GET(get_c, "/c");
    ASSERT_EQ(wait(get_c_future).error, Error::ZNONODE);
};

TEST_P(FDBKeeperChrootSuite, CleanerManyExpired)
{
    using namespace zkutil;
    const int keepers_size = 50;
    const int nodes_size = 50;
    const int future_batch_size = 1000;

    auto orig_keeper = keeper;
    std::vector<std::shared_ptr<Coordination::FDBKeeper>> new_keepers;
    new_keepers.reserve(keepers_size);
    std::vector<std::future<CreateResponse>> futures;
    auto wait_futures = [&]()
    {
        for (auto & f : futures)
            ASSERT_EQ(wait(f, 5s).error, Error::ZOK);
        futures.clear();
    };

    for (int i = 0; i < keepers_size; i++)
    {
        LOG_TRACE(log, "Creating {} nodes on keeper {}", nodes_size, i);
        auto new_keeper = createKeeper();
        new_keepers.push_back(new_keeper);
        USE_KEEPER(new_keeper);

        for (int j = 0; j < nodes_size; j++)
        {
            KEEPER_CREATE(create_ephe, fmt::format("/{}_{}", i, j), "", true, false, {});
            futures.emplace_back(std::move(create_ephe_future));
        }

        if (futures.size() >= future_batch_size)
            wait_futures();
    }
    wait_futures();

    USE_KEEPER(orig_keeper);
    new_keepers.clear();

    int remain_size = keepers_size * nodes_size;
    for (int i = 0; remain_size > 0 && i < 5; i++)
    {
        LOG_TRACE(log, "Remain nodes: {}", remain_size);
        std::this_thread::sleep_for(std::chrono::milliseconds{KEEPER_SESSION_EXPIRE_AT_MS});
        KEEPER_EXISTS(list, "/");
        auto list_resp = wait(list_future);
        ASSERT_EQ(list_resp.error, Error::ZOK);
        remain_size = list_resp.stat.numChildren;
    }
    ASSERT_EQ(remain_size, 0);
};

String genLageData(size_t size)
{
    std::mt19937 gen(std::random_device{}());

    auto size_chars = size;
    auto size_uint64s = size_chars / sizeof(uint64_t);

    String res;
    res.resize(size_chars);

    std::uniform_int_distribution<uint64_t> dis;
    for (size_t i = 0; i < size_uint64s; ++i)
        *(reinterpret_cast<uint64_t *>(res.data()) + i) = dis(gen);

    return res;
}

TEST_P(FDBKeeperChrootSuite, Create1MDataShouldSuccess)
{
    auto data = genLageData(1024 * 1024);

    KEEPER_CREATE(a, "/a", data, false, false, {});
    ASSERT_EQ(wait(a_future).error, Error::ZOK);

    KEEPER_GET(get, "/a");
    auto get_resp = wait(get_future);
    ASSERT_EQ(get_resp.error, Error::ZOK);
    ASSERT_EQ(get_resp.data, data);
}

TEST_P(FDBKeeperChrootSuite, CreateOverflowDataShouldFailed)
{
    auto data = genLageData(1024 * 1024 + 1);

    KEEPER_CREATE(a, "/a", data, false, false, {});
    ASSERT_EQ(wait(a_future).error, Error::ZBADARGUMENTS);
}

TEST_P(FDBKeeperChrootSuite, Set1MDataShouldSuccess)
{
    auto data = genLageData(1024 * 1024);

    KEEPER_CREATE(a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(a_future).error, Error::ZOK);

    KEEPER_SET(set, "/a", data, -1);
    ASSERT_EQ(wait(set_future).error, Error::ZOK);

    KEEPER_GET(get, "/a");
    auto get_resp = wait(get_future);
    ASSERT_EQ(get_resp.error, Error::ZOK);
    ASSERT_EQ(get_resp.data, data);
}

TEST_P(FDBKeeperChrootSuite, SetOverflowDataShouldFailed)
{
    auto data = genLageData(1024 * 1024 + 1);

    KEEPER_CREATE(a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(a_future).error, Error::ZOK);

    KEEPER_SET(set, "/a", data, -1);
    ASSERT_EQ(wait(set_future).error, Error::ZBADARGUMENTS);
}

TEST_P(FDBKeeperChrootSuite, Remove1MDataShouldSuccess)
{
    auto data = genLageData(1024 * 1024);

    KEEPER_CREATE(a, "/a", data, false, false, {});
    ASSERT_EQ(wait(a_future).error, Error::ZOK);

    KEEPER_REMOVE(rm, "/a", -1);
    ASSERT_EQ(wait(rm_future).error, Error::ZOK);
}

TEST_P(FDBKeeperChrootSuite, LinearizableCreateShouldWork)
{
    KEEPER_CREATE(a, "/a", "", false, false, {});
    KEEPER_CREATE(b, "/a/b", "", false, false, {});
    KEEPER_CREATE(c, "/a/b/c", "", false, false, {});

    ASSERT_EQ(wait(a_future).error, Error::ZOK);
    ASSERT_EQ(wait(b_future).error, Error::ZOK);
    ASSERT_EQ(wait(c_future).error, Error::ZOK);
}

TEST_P(FDBKeeperChrootSuite, LinearizableCreateRemoveShouldWork)
{
    KEEPER_CREATE(create, "/a", "", false, false, {});
    KEEPER_REMOVE(rm, "/a", -1);

    ASSERT_EQ(wait(create_future).error, Error::ZOK);
    ASSERT_EQ(wait(rm_future).error, Error::ZOK);
}

TEST_P(FDBKeeperChrootSuite, LinearizableCreateSetShouldWork)
{
    KEEPER_CREATE(create, "/a", "", false, false, {});
    KEEPER_SET(set, "/a", "1", -1);

    ASSERT_EQ(wait(create_future).error, Error::ZOK);
    ASSERT_EQ(wait(set_future).error, Error::ZOK);
}

TEST_P(FDBKeeperChrootSuite, LinearizableSetShouldWork)
{
    for (int i = 0; i < 10; i++)
    {
        keeper.reset();
        clear();
        initRoot();
        /// Recreate keeper is required because old keeper may be expired
        /// by clear() or tmp root keeper in initRoot()
        keeper = createKeeper();

        KEEPER_CREATE(create, "/a", "", false, false, {});
        ASSERT_EQ(wait(create_future).error, Error::ZOK);

        KEEPER_SET(set1, "/a", "1", -1);
        KEEPER_SET(set2, "/a", "2", -1);
        KEEPER_SET(set3, "/a", "3", -1);

        ASSERT_EQ(wait(set1_future).error, Error::ZOK);
        ASSERT_EQ(wait(set2_future).error, Error::ZOK);
        ASSERT_EQ(wait(set3_future).error, Error::ZOK);

        KEEPER_GET(get, "/a");
        auto get_resp = wait(get_future);
        ASSERT_EQ(get_resp.error, Error::ZOK);
        ASSERT_EQ(get_resp.data, "3");
    }
}

TEST_P(FDBKeeperChrootSuite, LinearizableRemoveRemoveShouldWork)
{
    KEEPER_CREATE(a, "/a", "", false, false, {});
    ASSERT_EQ(wait(a_future).error, Error::ZOK);
    KEEPER_CREATE(b, "/a/b", "", false, false, {});
    ASSERT_EQ(wait(b_future).error, Error::ZOK);

    KEEPER_REMOVE(rm_b, "/a/b", -1);
    KEEPER_REMOVE(rm_a, "/a", -1);
    ASSERT_EQ(wait(rm_b_future).error, Error::ZOK);
    ASSERT_EQ(wait(rm_a_future).error, Error::ZOK);
}

TEST_P(FDBKeeperChrootSuite, LinearizableMultiRequestShouldWork)
{
    using namespace zkutil;

    Requests reqs1 = {
        makeCreateRequest("/a", "abc", CreateMode::Persistent),
        makeCreateRequest("/b", "abc", CreateMode::Persistent),
        makeCreateRequest("/b/c", "abc", CreateMode::Persistent),
    };
    Requests reqs2 = {
        makeCreateRequest("/a/b", "abc", CreateMode::Persistent),
        makeCreateRequest("/b/c/d", "abc", CreateMode::Persistent),
        makeSetRequest("/a", "edf", -1),
    };
    Requests reqs3 = {
        makeRemoveRequest("/a/b", -1),
        makeRemoveRequest("/b/c/d", -1),
    };
    Requests reqs4 = {
        makeRemoveRequest("/a", -1),
        makeRemoveRequest("/b/c", -1),
        makeRemoveRequest("/b", -1),
    };

    KEEPER_MULTI(multi1, reqs1);
    KEEPER_MULTI(multi2, reqs2);
    KEEPER_MULTI(multi3, reqs3);
    KEEPER_MULTI(multi4, reqs4);

    EXPECT_EQ(wait(multi1_future).error, Error::ZOK);
    EXPECT_EQ(wait(multi2_future).error, Error::ZOK);
    EXPECT_EQ(wait(multi3_future).error, Error::ZOK);
    EXPECT_EQ(wait(multi4_future).error, Error::ZOK);
}

// FIXME: Unstable test. Request maybe processed before reset();
TEST_P(FDBKeeperChrootSuite, DISABLED_CreateEphemeralAbortShouldPassASan)
{
    KEEPER_CREATE(create, "/a", "", true, false, {});
    keeper.reset();
    ASSERT_EQ(wait(create_future).error, Error::ZCONNECTIONLOSS);
}

// FIXME: Unstable test. Request maybe processed before reset();
TEST_P(FDBKeeperChrootSuite, DISABLED_CreateEphemeralInMultiRequestAbortShouldPassASan)
{
    using namespace zkutil;

    Requests reqs = {
        makeCreateRequest("/a", "abc", CreateMode::Ephemeral),
        makeCreateRequest("/b", "abc", CreateMode::Ephemeral),
        makeCreateRequest("/b/c", "abc", CreateMode::Ephemeral),
    };
    KEEPER_MULTI(multi, reqs);
    keeper.reset();

    auto resp = wait(multi_future);
    ASSERT_EQ(resp.error, Error::ZCONNECTIONLOSS);
    ASSERT_EQ(resp.responses[0]->error, Error::ZCONNECTIONLOSS);
    ASSERT_EQ(resp.responses[1]->error, Error::ZRUNTIMEINCONSISTENCY);
    ASSERT_EQ(resp.responses[2]->error, Error::ZRUNTIMEINCONSISTENCY);
}

TEST_P(FDBKeeperChrootSuite, CTimeAndMTimeShouldCorrect)
{
    // *time is measured in milliseconds
    auto now = []()
    { return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count(); };

    auto create_time = now();
    KEEPER_CREATE(a, "/a", "abc", false, false, {});
    ASSERT_EQ(wait(a_future).error, Error::ZOK);

    std::this_thread::sleep_for(1s);

    auto modify_time = now();
    KEEPER_SET(set, "/a", "efg", -1);
    ASSERT_EQ(wait(set_future).error, Error::ZOK);

    KEEPER_GET(get, "/a");
    auto get_resp = wait(get_future);
    ASSERT_EQ(get_resp.error, Error::ZOK);

    LOG_TRACE(log, "{} {}", create_time, get_resp.stat.ctime);
    ASSERT_LT(std::abs(create_time - get_resp.stat.ctime), 1'000);
    ASSERT_LT(std::abs(modify_time - get_resp.stat.mtime), 1'000);
}

TEST_P(FDBKeeperChrootSuite, ManualCleanerShouldWork)
{
    LOG_TRACE(log, "Waiting session id...");
    std::this_thread::sleep_for(std::chrono::milliseconds(KEEPER_HEARTBEAT_INTERVAL_MS) * 2);
    auto old_session_id = keeper->getSessionID();

    keeper->cleanSession(old_session_id).get();

    LOG_TRACE(log, "Waiting session id...");
    std::this_thread::sleep_for(std::chrono::milliseconds(KEEPER_HEARTBEAT_INTERVAL_MS) * 2);
    ASSERT_TRUE(keeper->isExpired());
}

TEST_P(FDBKeeperChrootSuite, ManualCleanerShouldFaildedIfSessionIDInvalid)
{
    try
    {
        keeper->cleanSession(0).get();
        FAIL() << "no exception occurred";
    }
    catch (DB::Exception & e)
    {
        ASSERT_TRUE(e.message().starts_with("No such session"));
    }
}

TEST_P(FDBKeeperChrootSuite, SessionShouldBeReadyInCtor)
{
    ASSERT_NE(keeper->getSessionID(), 0);
}

TEST_F(FDBKeeperSuite, TrxShouldBeCanceledByFinalize)
{
    KEEPER_CREATE(a, "/a", "", false, false, {});
    keeper->finalize("test");

    ASSERT_EQ(wait(a_future).error, Error::ZSESSIONEXPIRED);
}

TEST_F(FDBKeeperSuite, WatchShouldBeCanceledByFinalize)
{
    KEEPER_CREATE(create_a, "/a", "", false, false, {});
    ASSERT_EQ(wait(create_a_future).error, Error::ZOK);

    KEEPER_GET_WATCH(get_a, "/a");
    KEEPER_LIST_WATCH(list, "/", ListRequestType::ALL);
    KEEPER_EXISTS_WATCH(exists_a, "/a");

    ASSERT_EQ(wait(get_a_future).error, Error::ZOK);
    ASSERT_EQ(wait(list_future).error, Error::ZOK);
    ASSERT_EQ(wait(exists_a_future).error, Error::ZOK);

    keeper->finalize("test");

    ASSERT_EQ(wait(get_a_watch_future, 0s).error, Error::ZSESSIONEXPIRED);
    ASSERT_EQ(wait(list_watch_future, 0s).error, Error::ZSESSIONEXPIRED);
    ASSERT_EQ(wait(exists_a_watch_future, 0s).error, Error::ZSESSIONEXPIRED);
}


TEST_P(FDBKeeperChrootSuite, MultiTrxCanceledImmediately)
{
    using namespace zkutil;

    Requests reqs = {
        makeCreateRequest("/a", "abc", CreateMode::Persistent),
        makeCheckRequest("/a", 0),
    };
    KEEPER_MULTI(multi, reqs);
    keeper->finalize("test");
    auto resp = wait(multi_future);

    ASSERT_EQ(resp.error, Error::ZSESSIONEXPIRED);
    ASSERT_EQ(resp.responses.size(), 2);
};

TEST_P(FDBKeeperChrootSuite, CheckNotExists)
{
    ASSERT_TRUE(keeper->isFeatureEnabled(KeeperFeatureFlag::CHECK_NOT_EXISTS));

    {
        KEEPER_CREATE(create, "/a", "0", false, false, {});
        wait(create_future);
        KEEPER_SET(set, "/a", "1", -1);
        wait(set_future);
    }

    auto request = std::make_shared<Coordination::CheckRequest>();
    request->not_exists = true;

    {
        SCOPED_TRACE("CheckNotExists returns ZOK if not exists");
        request->path = "/b";
        request->version = -1;
        KEEPER_MULTI(multi, {request});
        auto resp = wait(multi_future);
        ASSERT_EQ(resp.error, Error::ZOK);
    }

    {
        SCOPED_TRACE("CheckNotExists returns ZNODEEXISTS if exists");
        request->path = "/a";
        request->version = -1;
        KEEPER_MULTI(multi, {request});
        auto resp = wait(multi_future);
        ASSERT_EQ(resp.error, Error::ZNODEEXISTS);
    }

    {
        SCOPED_TRACE("CheckNotExists returns ZNODEEXISTS for same version");
        request->path = "/a";
        request->version = 1;
        KEEPER_MULTI(multi, {request});
        auto resp = wait(multi_future);
        ASSERT_EQ(resp.error, Error::ZNODEEXISTS);
    }

    {
        SCOPED_TRACE("CheckNotExists returns ZOK for different version");
        request->path = "/a";
        request->version = 2;
        KEEPER_MULTI(multi, {request});
        auto resp = wait(multi_future);
        ASSERT_EQ(resp.error, Error::ZOK);
    }
};

TEST_P(FDBKeeperChrootSuite, ListWithTypeFilter)
{
    ASSERT_TRUE(keeper->isFeatureEnabled(KeeperFeatureFlag::FILTERED_LIST));

    {
        KEEPER_CREATE(create, "/a", "abc", false, false, {});
        ASSERT_EQ(wait(create_future).error, Error::ZOK);
    }

    auto create_node = [&](const std::string & name, bool emphemeral)
    {
        KEEPER_CREATE(create, "/a/" + name, "abc", emphemeral, false, {});
        ASSERT_EQ(wait(create_future).error, Error::ZOK);
    };
    create_node("a", false);
    create_node("b", false);
    create_node("c", true);
    create_node("d", true);

    {
        SCOPED_TRACE("List PERSISTENT_ONLY");
        KEEPER_LIST(list, "/a", ListRequestType::PERSISTENT_ONLY);
        auto resp = wait(list_future);
        ASSERT_EQ(resp.error, Error::ZOK);
        ASSERT_EQ(resp.names.size(), 2);
        ASSERT_EQ(resp.names[0], "a");
        ASSERT_EQ(resp.names[1], "b");
        ASSERT_EQ(resp.stat.numChildren, 4);
    }

    {
        SCOPED_TRACE("List EPHEMERAL_ONLY");
        KEEPER_LIST(list, "/a", ListRequestType::EPHEMERAL_ONLY);
        auto resp = wait(list_future);
        ASSERT_EQ(resp.error, Error::ZOK);
        ASSERT_EQ(resp.names.size(), 2);
        ASSERT_EQ(resp.names[0], "c");
        ASSERT_EQ(resp.names[1], "d");
        ASSERT_EQ(resp.stat.numChildren, 4);
    }
}

TEST_P(FDBKeeperChrootSuite, MultiRead)
{
    ASSERT_TRUE(keeper->isFeatureEnabled(KeeperFeatureFlag::MULTI_READ));

    {
        KEEPER_CREATE(create, "/a", "aaa", false, false, {});
        ASSERT_EQ(wait(create_future).error, Error::ZOK);
    }

    auto create_node = [&](const std::string & name, bool emphemeral)
    {
        KEEPER_CREATE(create, "/a/" + name, "abc", emphemeral, false, {});
        ASSERT_EQ(wait(create_future).error, Error::ZOK);
    };
    create_node("a", false);
    create_node("b", false);
    create_node("c", true);
    create_node("d", true);

    using namespace zkutil;
    {
        SCOPED_TRACE("Multi success request");
        KEEPER_MULTI(
            multi,
            {
                makeGetRequest("/a"),
                makeGetRequest("/a/c"),
                makeSimpleListRequest("/a"),
                makeListRequest("/a", ListRequestType::EPHEMERAL_ONLY),
                makeExistsRequest("/a/a"),
            });
        auto resps = wait(multi_future);
        ASSERT_EQ(resps.error, Error::ZOK);
        ASSERT_EQ(resps.responses.size(), 5);

        auto resp0 = dynamic_cast<const Coordination::GetResponse &>(*resps.responses[0]);
        ASSERT_EQ(resp0.data, "aaa");

        auto resp1 = dynamic_cast<const Coordination::GetResponse &>(*resps.responses[1]);
        ASSERT_EQ(resp1.data, "abc");

        auto resp2 = dynamic_cast<const Coordination::ListResponse &>(*resps.responses[2]);
        ASSERT_EQ(resp2.names.size(), 4);

        auto resp3 = dynamic_cast<const Coordination::ListResponse &>(*resps.responses[3]);
        ASSERT_EQ(resp3.names.size(), 2);

        auto resp4 = dynamic_cast<const Coordination::ExistsResponse &>(*resps.responses[4]);
        ASSERT_EQ(resp4.stat.dataLength, 3);
    }

    {
        SCOPED_TRACE("Multi failed request");
        KEEPER_MULTI(
            multi,
            {
                makeGetRequest("/a"),
                makeGetRequest("/b"),
                makeGetRequest("/c"),
            });
        auto resps = wait(multi_future);
        ASSERT_EQ(resps.error, Error::ZNONODE);
        ASSERT_EQ(resps.responses.size(), 3);

        auto resp0 = dynamic_cast<const Coordination::GetResponse &>(*resps.responses[0]);
        ASSERT_EQ(resp0.data, "aaa");

        auto resp1 = dynamic_cast<const Coordination::GetResponse &>(*resps.responses[1]);
        ASSERT_EQ(resp1.error, Error::ZNONODE);

        auto resp2 = dynamic_cast<const Coordination::GetResponse &>(*resps.responses[2]);
        ASSERT_EQ(resp2.error, Error::ZRUNTIMEINCONSISTENCY);
    }
}

TEST_P(FDBKeeperChrootSuite, MultiExists)
{
    ASSERT_TRUE(keeper->isFeatureEnabled(KeeperFeatureFlag::MULTI_READ));

    auto create_node = [&](const std::string & name)
    {
        KEEPER_CREATE(create, name, "abc", false, false, {});
        ASSERT_EQ(wait(create_future).error, Error::ZOK);
    };
    create_node("/a");
    create_node("/b");
    create_node("/d");

    using namespace zkutil;
    KEEPER_MULTI(
        multi,
        {
            makeExistsRequest("/a"),
            makeExistsRequest("/b"),
            makeExistsRequest("/c"),
            makeExistsRequest("/d"),
        });
    auto resps = wait(multi_future);
    ASSERT_EQ(resps.error, Error::ZOK);
    ASSERT_EQ(resps.responses.size(), 4);

    std::vector<Error> actual_results;
    for (auto & resp : resps.responses)
    {
        auto resp_exists = dynamic_cast<const Coordination::ExistsResponse &>(*resp);
        actual_results.emplace_back(resp_exists.error);
    }
    std::vector<Error> expect_results = {
        Error::ZOK,
        Error::ZOK,
        Error::ZNONODE,
        Error::ZOK,
    };
    ASSERT_EQ(actual_results, expect_results);
}

TEST_P(FDBKeeperChrootSuite, CreateIgnoreExists)
{
    ASSERT_TRUE(keeper->isFeatureEnabled(KeeperFeatureFlag::CREATE_IF_NOT_EXISTS));

    {
        KEEPER_CREATE(create, "/a", "aaa", false, false, {});
        ASSERT_EQ(wait(create_future).error, Error::ZOK);
    }

    using namespace zkutil;
    KEEPER_MULTI(create_a, {makeCreateRequest("/a", "abc", zkutil::CreateMode::Persistent, true)});
    auto resps = wait(create_a_future);

    ASSERT_EQ(resps.responses.size(), 1);
    const auto & create_resp = dynamic_cast<const CreateResponse &>(*resps.responses[0]);

    ASSERT_EQ(create_resp.path_created, "/a");
    ASSERT_EQ(create_resp.error, Error::ZOK);
}
#endif
