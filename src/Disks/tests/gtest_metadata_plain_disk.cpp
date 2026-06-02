
#include <Disks/DiskObjectStorage/MetadataStorages/Plain/MetadataStorageFromPlainObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Disks/WriteMode.h>

#include <Common/ObjectStorageKey.h>

#include <gtest/gtest.h>

#include <filesystem>

namespace fs = std::filesystem;
using namespace DB;

class MetadataPlainDiskTest : public testing::Test
{
public:
    void SetUp() override
    {
        test_dir = "./test_metadata_plain_disk_" + std::to_string(reinterpret_cast<uintptr_t>(this));
        fs::remove_all(test_dir);
    }

    void TearDown() override
    {
        fs::remove_all(test_dir);
    }

    /// Create a MetadataStorageFromPlainObjectStorage backed by LocalObjectStorage.
    /// key_prefix simulates the S3 common key prefix (e.g. "data").
    std::pair<std::shared_ptr<IMetadataStorage>, std::shared_ptr<IObjectStorage>>
    createWithPrefix(const std::string & key_prefix) const
    {
        std::string full_prefix = key_prefix.empty()
            ? test_dir
            : (fs::path(test_dir) / key_prefix).string();
        LocalObjectStorageSettings settings("test", full_prefix, /*read_only_=*/false);
        auto object_storage = std::make_shared<LocalObjectStorage>(std::move(settings));
        auto metadata_storage = std::make_shared<MetadataStorageFromPlainObjectStorage>(
            object_storage, /*storage_path_prefix_=*/"", /*object_metadata_cache_size=*/0);
        return {metadata_storage, object_storage};
    }

    /// Write a file using getKeyForPath to construct the correct full key.
    void writeFile(const std::shared_ptr<IObjectStorage> & storage, const std::string & relative_path, const std::string & content)
    {
        auto key = ObjectStorageKey::createAsRelative(storage->getCommonKeyPrefix(), relative_path);
        StoredObject object(key.serialize());
        auto buf = storage->writeObject(object, WriteMode::Rewrite);
        buf->write(content.data(), content.size());
        buf->preFinalize();
        buf->finalize();
    }

    bool fileExists(const std::shared_ptr<IObjectStorage> & storage, const std::string & relative_path)
    {
        auto key = ObjectStorageKey::createAsRelative(storage->getCommonKeyPrefix(), relative_path);
        return fs::exists(key.serialize());
    }

    std::string test_dir;
};

/// Verify that removeDirectory correctly removes objects when a non-empty
/// common key prefix is configured. This is a regression test for the bug where
/// removeDirectory used iterator paths directly without prepending the key prefix.
TEST_F(MetadataPlainDiskTest, RemoveDirectoryWithKeyPrefix)
{
    auto [metadata, object_storage] = createWithPrefix("data");

    // Create files under "part_id/" directory (flat, no subdirectories)
    writeFile(object_storage, "part_id/file1.txt", "content1");
    writeFile(object_storage, "part_id/file2.bin", "content2");
    writeFile(object_storage, "part_id/file3.dat", "content3");

    ASSERT_TRUE(fileExists(object_storage, "part_id/file1.txt"));
    ASSERT_TRUE(fileExists(object_storage, "part_id/file2.bin"));
    ASSERT_TRUE(fileExists(object_storage, "part_id/file3.dat"));

    auto tx = metadata->createTransaction();
    tx->removeDirectory("part_id");

    EXPECT_FALSE(fileExists(object_storage, "part_id/file1.txt"));
    EXPECT_FALSE(fileExists(object_storage, "part_id/file2.bin"));
    EXPECT_FALSE(fileExists(object_storage, "part_id/file3.dat"));
}

/// Verify that removeRecursive correctly removes objects with a non-empty key prefix.
TEST_F(MetadataPlainDiskTest, RemoveRecursiveWithKeyPrefix)
{
    auto [metadata, object_storage] = createWithPrefix("data");

    // Create files under "uuid-12345/" directory
    writeFile(object_storage, "uuid-12345/checksums.txt", "abc");
    writeFile(object_storage, "uuid-12345/columns.txt", "def");
    writeFile(object_storage, "uuid-12345/data.bin", "ghi");

    ASSERT_TRUE(fileExists(object_storage, "uuid-12345/checksums.txt"));
    ASSERT_TRUE(fileExists(object_storage, "uuid-12345/columns.txt"));
    ASSERT_TRUE(fileExists(object_storage, "uuid-12345/data.bin"));

    auto tx = metadata->createTransaction();
    tx->removeRecursive("uuid-12345", {});

    EXPECT_FALSE(fileExists(object_storage, "uuid-12345/checksums.txt"));
    EXPECT_FALSE(fileExists(object_storage, "uuid-12345/columns.txt"));
    EXPECT_FALSE(fileExists(object_storage, "uuid-12345/data.bin"));
}

/// Verify that unlinkFile also works correctly with a non-empty key prefix.
TEST_F(MetadataPlainDiskTest, UnlinkFileWithKeyPrefix)
{
    auto [metadata, object_storage] = createWithPrefix("data");

    writeFile(object_storage, "some/path/file.txt", "hello");
    ASSERT_TRUE(fileExists(object_storage, "some/path/file.txt"));

    auto tx = metadata->createTransaction();
    tx->unlinkFile("some/path/file.txt", /*if_exists=*/false, /*should_remove_objects=*/true);

    EXPECT_FALSE(fileExists(object_storage, "some/path/file.txt"));
}

/// Verify that removeDirectory is a no-op when the directory doesn't exist
/// (no crash, no exception).
TEST_F(MetadataPlainDiskTest, RemoveDirectoryNonExistent)
{
    auto [metadata, object_storage] = createWithPrefix("data");

    auto tx = metadata->createTransaction();
    EXPECT_NO_THROW(tx->removeDirectory("nonexistent_dir"));
}

/// Verify behavior with an empty key prefix (should still work correctly).
TEST_F(MetadataPlainDiskTest, RemoveDirectoryWithEmptyPrefix)
{
    auto [metadata, object_storage] = createWithPrefix("");

    writeFile(object_storage, "part/file1.txt", "a");
    writeFile(object_storage, "part/file2.txt", "b");

    ASSERT_TRUE(fileExists(object_storage, "part/file1.txt"));
    ASSERT_TRUE(fileExists(object_storage, "part/file2.txt"));

    auto tx = metadata->createTransaction();
    tx->removeDirectory("part");

    EXPECT_FALSE(fileExists(object_storage, "part/file1.txt"));
    EXPECT_FALSE(fileExists(object_storage, "part/file2.txt"));
}
