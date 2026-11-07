#include <gtest/gtest.h>

#include <Storages/ObjectStorageQueue/ObjectStorageQueueIFileMetadata.h>

using DB::ObjectStorageQueueLocalActiveNodes;

TEST(ObjectStorageQueueLocalActiveNodes, RegistrationBlocksRemovalLock)
{
    ObjectStorageQueueLocalActiveNodes nodes;

    ASSERT_TRUE(nodes.tryAdd("/queue/processing/a"));
    ASSERT_TRUE(nodes.contains("/queue/processing/a"));

    /// A registered (live) path cannot be locked for removal.
    ASSERT_FALSE(nodes.tryLockForRemoval("/queue/processing/a"));

    nodes.remove("/queue/processing/a");
    ASSERT_FALSE(nodes.contains("/queue/processing/a"));
    ASSERT_TRUE(nodes.tryLockForRemoval("/queue/processing/a"));
    nodes.unlockRemoval("/queue/processing/a");
}

TEST(ObjectStorageQueueLocalActiveNodes, RemovalLockBlocksRegistration)
{
    ObjectStorageQueueLocalActiveNodes nodes;

    ASSERT_TRUE(nodes.tryLockForRemoval("/queue/processing/a"));

    /// A creator must back off while the cleanup inspects the path,
    /// so its create cannot land inside the revalidate+remove window.
    ASSERT_FALSE(nodes.tryAdd("/queue/processing/a"));
    /// Other paths are unaffected.
    ASSERT_TRUE(nodes.tryAdd("/queue/processing/b"));

    nodes.unlockRemoval("/queue/processing/a");
    ASSERT_TRUE(nodes.tryAdd("/queue/processing/a"));

    nodes.remove("/queue/processing/a");
    nodes.remove("/queue/processing/b");
}

TEST(ObjectStorageQueueLocalActiveNodes, ReferenceCounting)
{
    ObjectStorageQueueLocalActiveNodes nodes;

    /// Overlapping owners of the same path (e.g. an uncertain-commit straggler
    /// and a retry): one owner unregistering must not drop the protection.
    ASSERT_TRUE(nodes.tryAdd("/queue/processing/a"));
    ASSERT_TRUE(nodes.tryAdd("/queue/processing/a"));

    nodes.remove("/queue/processing/a");
    ASSERT_TRUE(nodes.contains("/queue/processing/a"));
    ASSERT_FALSE(nodes.tryLockForRemoval("/queue/processing/a"));

    nodes.remove("/queue/processing/a");
    ASSERT_FALSE(nodes.contains("/queue/processing/a"));
    ASSERT_TRUE(nodes.tryLockForRemoval("/queue/processing/a"));
    nodes.unlockRemoval("/queue/processing/a");
}

TEST(ObjectStorageQueueLocalActiveNodes, RemovalLockIsExclusive)
{
    ObjectStorageQueueLocalActiveNodes nodes;

    ASSERT_TRUE(nodes.tryLockForRemoval("/queue/processing/a"));
    ASSERT_FALSE(nodes.tryLockForRemoval("/queue/processing/a"));
    nodes.unlockRemoval("/queue/processing/a");
    ASSERT_TRUE(nodes.tryLockForRemoval("/queue/processing/a"));
    nodes.unlockRemoval("/queue/processing/a");
}
