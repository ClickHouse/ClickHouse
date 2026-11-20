#include <gtest/gtest.h>

#include <Disks/DiskLocal.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PersistentRemovalQueue.h>
#include <Common/FailPoint.h>
#include <Common/getRandomASCIIString.h>

#include <filesystem>
#include <fstream>

namespace fs = std::filesystem;

namespace DB::FailPoints
{
    extern const char persistent_removal_queue_fail_append[];
}

namespace
{

class PersistentRemovalQueueTest : public testing::Test
{
public:
    void SetUp() override
    {
        path = "./test-removal-queue-dir." + DB::getRandomASCIIString(6);
        fs::create_directories(path);
        disk = std::make_shared<DB::DiskLocal>("test-removal-queue", path);
    }

    void TearDown() override { fs::remove_all(path); }

    std::unique_ptr<DB::PersistentRemovalQueue> open(size_t compaction_threshold = 1000) const
    {
        auto queue = std::make_unique<DB::PersistentRemovalQueue>(disk, ".metadata", compaction_threshold);
        queue->load();
        return queue;
    }

    std::string logPath() const { return fs::path(path) / ".metadata" / "blobs_to_remove.log"; }

    std::string path;
    DB::DiskPtr disk;
};

DB::StoredObjects blobs(const std::vector<std::string> & keys)
{
    DB::StoredObjects result;
    for (const auto & key : keys)
        result.emplace_back(key, "file_" + key, 100);
    return result;
}

std::vector<std::string> keys(const DB::IRemovalQueue & queue)
{
    std::vector<std::string> result;
    for (const auto & blob : queue.takeFirst(0))
        result.push_back(blob.remote_path);
    return result;
}

using Keys = std::vector<std::string>;

}

TEST_F(PersistentRemovalQueueTest, RestoredAfterRestart)
{
    {
        auto queue = open();
        queue->submitForRemoval(blobs({"key1", "key2", "key3"}));
        EXPECT_EQ(queue->markAsRemoved(blobs({"key1"})), 1);
    }

    auto queue = open();
    EXPECT_EQ(keys(*queue), (Keys{"key2", "key3"}));

    const auto restored = queue->takeFirst(0);
    EXPECT_EQ(restored[0].local_path, "file_key2");
    EXPECT_EQ(restored[0].bytes_size, 100);
}

TEST_F(PersistentRemovalQueueTest, CompactionKeepsOnlyPendingBlobs)
{
    {
        auto queue = open(/*compaction_threshold=*/2);
        queue->submitForRemoval(blobs({"key1", "key2", "key3", "key4"}));
        queue->markAsRemoved(blobs({"key1", "key2"}));
    }

    {
        auto queue = open(/*compaction_threshold=*/2);
        EXPECT_EQ(keys(*queue), (Keys{"key3", "key4"}));
        queue->markAsRemoved(blobs({"key3"}));
    }

    auto queue = open(/*compaction_threshold=*/2);
    EXPECT_EQ(keys(*queue), (Keys{"key4"}));
}

TEST_F(PersistentRemovalQueueTest, TornLastEntryIsDropped)
{
    open()->submitForRemoval(blobs({"key1", "key2"}));

    {
        std::ofstream file(logPath(), std::ios::binary | std::ios::app);
        file.put(0);
        file.put(10);
    }

    {
        auto queue = open();
        EXPECT_EQ(keys(*queue), (Keys{"key1", "key2"}));
        queue->submitForRemoval(blobs({"key3"}));
    }

    auto queue = open();
    EXPECT_EQ(keys(*queue), (Keys{"key1", "key2", "key3"}));
}

TEST_F(PersistentRemovalQueueTest, LogIsRepairedAfterFailedAppend)
{
    {
        auto queue = open();
        DB::FailPointInjection::enableFailPoint(DB::FailPoints::persistent_removal_queue_fail_append);
        queue->submitForRemoval(blobs({"key1"}));
        DB::FailPointInjection::disableFailPoint(DB::FailPoints::persistent_removal_queue_fail_append);
        EXPECT_TRUE(fs::exists(logPath()));
        EXPECT_EQ(fs::file_size(logPath()), 0);

        queue->submitForRemoval(blobs({"key2"}));
    }

    auto queue = open();
    EXPECT_EQ(keys(*queue), (Keys{"key1", "key2"}));
}
