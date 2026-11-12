#include <Interpreters/DDLIdempotencyKey.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperArgs.h>

#include <Poco/Timestamp.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

constexpr auto KEYS_PATH = "/idempotency";
constexpr auto KEY_PATH = "/idempotency/key";

zkutil::ZooKeeperPtr makeZooKeeper()
{
    zkutil::ZooKeeperArgs args;
    args.implementation = "testkeeper";
    auto zookeeper = zkutil::ZooKeeper::createWithoutKillingPreviousSessions(args);
    zookeeper->create(KEYS_PATH, "", zkutil::CreateMode::Persistent);
    return zookeeper;
}

void commit(const zkutil::ZooKeeperPtr & zookeeper, const String & entry, std::optional<int32_t> expired_version)
{
    Coordination::Requests ops;
    addDDLIdempotencyKeyCommitRequests(ops, KEY_PATH, DDLIdempotencyKeyNode{.entry = entry, .digest = "d", .ttl_sec = 0}, expired_version);
    zookeeper->multi(ops);
}

String committedEntry(const zkutil::ZooKeeperPtr & zookeeper)
{
    return DDLIdempotencyKeyNode::parse(zookeeper->get(KEY_PATH)).entry;
}

/// Every node committed with `ttl_sec = 0` has expired by then.
Int64 later()
{
    return Poco::Timestamp().epochTime() + 10;
}

}

TEST(DDLIdempotencyKey, PruneRemovesExpiredKey)
{
    auto zookeeper = makeZooKeeper();
    commit(zookeeper, "query-0000000001", {});

    EXPECT_FALSE(tryGetExpiredDDLIdempotencyKeyNode(zookeeper, KEY_PATH, Poco::Timestamp().epochTime() - 10));
    auto expired = tryGetExpiredDDLIdempotencyKeyNode(zookeeper, KEY_PATH, later());
    ASSERT_TRUE(expired);
    EXPECT_TRUE(tryRemoveExpiredDDLIdempotencyKeyNode(zookeeper, KEY_PATH, *expired));
    EXPECT_FALSE(zookeeper->exists(KEY_PATH));
}

TEST(DDLIdempotencyKey, PruneKeepsReplacedKey)
{
    auto zookeeper = makeZooKeeper();
    commit(zookeeper, "query-0000000001", {});

    auto expired = tryGetExpiredDDLIdempotencyKeyNode(zookeeper, KEY_PATH, later());
    ASSERT_TRUE(expired);
    commit(zookeeper, "query-0000000002", expired->version);

    EXPECT_FALSE(tryRemoveExpiredDDLIdempotencyKeyNode(zookeeper, KEY_PATH, *expired));
    EXPECT_EQ(committedEntry(zookeeper), "query-0000000002");
}

/// Two replicas prune the same key: after one removes it, a statement commits the key again, which creates the node
/// with the version the other replica has read.
TEST(DDLIdempotencyKey, PruneKeepsKeyCreatedAgain)
{
    auto zookeeper = makeZooKeeper();
    commit(zookeeper, "query-0000000001", {});

    auto first = tryGetExpiredDDLIdempotencyKeyNode(zookeeper, KEY_PATH, later());
    auto second = tryGetExpiredDDLIdempotencyKeyNode(zookeeper, KEY_PATH, later());
    ASSERT_TRUE(first && second);
    ASSERT_TRUE(tryRemoveExpiredDDLIdempotencyKeyNode(zookeeper, KEY_PATH, *second));
    commit(zookeeper, "query-0000000002", {});
    Coordination::Stat stat;
    zookeeper->get(KEY_PATH, &stat);
    ASSERT_EQ(stat.version, first->version);

    EXPECT_FALSE(tryRemoveExpiredDDLIdempotencyKeyNode(zookeeper, KEY_PATH, *first));
    EXPECT_EQ(committedEntry(zookeeper), "query-0000000002");
}
