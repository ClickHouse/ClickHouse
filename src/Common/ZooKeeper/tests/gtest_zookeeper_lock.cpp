#include <Common/ZooKeeper/TestKeeper.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperArgs.h>
#include <Common/ZooKeeper/ZooKeeperLock.h>

#include <base/defines.h>

#include <gtest/gtest.h>

#include <memory>

namespace
{

/// `TestKeeper::getSessionID` returns 0 while `TestKeeperCreateRequest::process` stamps
/// `ephemeralOwner = 1` on every ephemeral node, and `ZooKeeper::getClientID` forwards to the
/// former. So a lock node taken through `TestKeeper` always looks like it belongs to somebody
/// else, which makes the foreign-owner branch of `ZooKeeperLock::unlock` reachable with no second
/// session and no stealing of the node. Do not "simplify" that away: it is the only reason these
/// tests need no Keeper.
zkutil::ZooKeeperPtr makeTestKeeperClient()
{
    zkutil::ZooKeeperArgs args;
    /// `createWithoutKillingPreviousSessions` reaches the `ZooKeeper(std::unique_ptr<IKeeper>)`
    /// constructor, which does not call `initSession` and therefore needs no global context.
    return zkutil::ZooKeeper::createWithoutKillingPreviousSessions(std::make_unique<Coordination::TestKeeper>(args));
}

}

#ifdef DEBUG_OR_SANITIZER_BUILD

TEST(ZooKeeperLockDeathTest, ForeignOwnerIsFatalEvenWhenLostIsTolerated)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";

    /// A foreign owner is a bug regardless of `throw_if_lost`: the caller only tolerates the node
    /// being gone, not somebody else holding the lock it believes it holds.
    auto zookeeper = makeTestKeeperClient();
    auto lock = zkutil::createSimpleZooKeeperLock(zookeeper, "/zk_lock_foreign_tolerant", "lock", "msg", /*throw_if_lost=*/false);
    ASSERT_TRUE(lock->tryLock());

    EXPECT_DEATH(lock->unlock(), "Lock is lost, it has another owner");

    /// Only the forked child died, so this process still believes it holds the lock and
    /// `~ZooKeeperLock` would reach the same branch. Expire the session so `unlock` early-returns.
    zookeeper->finalize("test finished");
}

TEST(ZooKeeperLockDeathTest, ForeignOwnerIsFatalForTheDefaultStrictCaller)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";

    /// Regression guard for the two callers that keep the default strict `throw_if_lost`.
    auto zookeeper = makeTestKeeperClient();
    auto lock = zkutil::createSimpleZooKeeperLock(zookeeper, "/zk_lock_foreign_strict", "lock", "msg");
    ASSERT_TRUE(lock->tryLock());

    EXPECT_DEATH(lock->unlock(), "Lock is lost, it has another owner");

    zookeeper->finalize("test finished");
}

#endif

TEST(ZooKeeperLockTest, MissingNodeIsToleratedWhenLostIsTolerated)
{
    /// A caller that tolerates a lost lock must survive the node being removed under it, which is
    /// what DDL queue cleanup does to the shard lock when it drops the whole entry subtree.
    auto zookeeper = makeTestKeeperClient();
    auto lock = zkutil::createSimpleZooKeeperLock(zookeeper, "/zk_lock_missing_tolerant", "lock", "msg", /*throw_if_lost=*/false);
    ASSERT_TRUE(lock->tryLock());

    zookeeper->remove(lock->getLockPath(), -1);

    EXPECT_NO_THROW(lock->unlock());
}
