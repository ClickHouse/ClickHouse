#include <gtest/gtest.h>

#include <Storages/ObjectStorageQueue/ObjectStorageQueueIFileMetadata.h>

using DB::ObjectStorageQueueLocalActiveNodes;

TEST(ObjectStorageQueueLocalActiveNodes, AddRemoveContains)
{
    ObjectStorageQueueLocalActiveNodes nodes;

    ASSERT_FALSE(nodes.contains("/queue/processing/a"));
    nodes.add("/queue/processing/a");
    ASSERT_TRUE(nodes.contains("/queue/processing/a"));
    ASSERT_FALSE(nodes.contains("/queue/processing/b"));

    nodes.remove("/queue/processing/a");
    ASSERT_FALSE(nodes.contains("/queue/processing/a"));
}

TEST(ObjectStorageQueueLocalActiveNodes, ReferenceCounting)
{
    ObjectStorageQueueLocalActiveNodes nodes;

    /// Overlapping owners of the same path (e.g. an uncertain-commit straggler
    /// and a retry): one owner unregistering must not drop the protection.
    nodes.add("/queue/processing/a");
    nodes.add("/queue/processing/a");

    nodes.remove("/queue/processing/a");
    ASSERT_TRUE(nodes.contains("/queue/processing/a"));

    nodes.remove("/queue/processing/a");
    ASSERT_FALSE(nodes.contains("/queue/processing/a"));
}
