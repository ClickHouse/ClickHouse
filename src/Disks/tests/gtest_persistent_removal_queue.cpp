#include <gtest/gtest.h>

#include <Disks/DiskLocal.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PersistentRemovalQueue.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Common/Exception.h>
#include <Common/getRandomASCIIString.h>

#include <filesystem>
#include <fstream>
#include <utility>

namespace fs = std::filesystem;

namespace DB::ErrorCodes
{
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
}

namespace
{

/// Fails a write after the file is opened, as a write error such as `ENOSPC` does: the file is created but stays empty.
class FailingDisk : public DB::DiskLocal
{
public:
    using DiskLocal::DiskLocal;

    bool fail_next_write = false;

    std::unique_ptr<DB::WriteBufferFromFileBase> writeFile(
        const String & path, size_t buf_size, DB::WriteMode mode, const DB::WriteSettings & settings) override
    {
        auto buf = DiskLocal::writeFile(path, buf_size, mode, settings);
        if (std::exchange(fail_next_write, false))
            throw DB::Exception(DB::ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Injected write failure");
        return buf;
    }
};

class PersistentRemovalQueueTest : public testing::Test
{
public:
    void SetUp() override
    {
        path = "./test-removal-queue-dir." + DB::getRandomASCIIString(6);
        fs::create_directories(path);
        disk = std::make_shared<FailingDisk>("test-removal-queue", path);
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
    std::shared_ptr<FailingDisk> disk;
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
        disk->fail_next_write = true;
        queue->submitForRemoval(blobs({"key1"}));
        EXPECT_TRUE(fs::exists(logPath()));
        EXPECT_EQ(fs::file_size(logPath()), 0);

        queue->submitForRemoval(blobs({"key2"}));
    }

    auto queue = open();
    EXPECT_EQ(keys(*queue), (Keys{"key1", "key2"}));
}
