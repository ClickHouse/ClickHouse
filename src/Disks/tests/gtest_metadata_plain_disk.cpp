#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Plain/MetadataStorageFromPlainObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
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
    std::shared_ptr<IMetadataStorage> getMetadataStorage(const std::string & key_prefix, size_t object_metadata_cache_size = 0)
    {
        if (!active_metadatas.contains(key_prefix))
            createStorage(key_prefix, object_metadata_cache_size);

        return active_metadatas.at(key_prefix);
    }
    std::shared_ptr<IObjectStorage> getObjectStorage(const std::string & key_prefix)
    {
        if (!active_object_storages.contains(key_prefix))
            createStorage(key_prefix, /*object_metadata_cache_size=*/0);

        return active_object_storages.at(key_prefix);
    }
    void TearDown() override
    {
        for (const auto & [_, metadata] : active_metadatas)
            metadata->shutdown();

        for (const auto & [_, object_storage] : active_object_storages)
        {
            object_storage->shutdown();
            fs::remove_all(object_storage->getCommonKeyPrefix());
        }
    }
private:
    void createStorage(const std::string & key_prefix, size_t object_metadata_cache_size)
    {
        fs::remove_all("./" + key_prefix);
        LocalObjectStorageSettings settings("test", "./" + key_prefix, /*read_only_=*/false);
        auto object_storage = std::make_shared<LocalObjectStorage>(std::move(settings));
        auto metadata_storage = std::make_shared<MetadataStorageFromPlainObjectStorage>(object_storage, /*storage_path_prefix_=*/"", object_metadata_cache_size);
        active_metadatas.emplace(key_prefix, metadata_storage);
        active_object_storages.emplace(key_prefix, object_storage);
    }

    std::unordered_map<std::string, std::shared_ptr<IMetadataStorage>> active_metadatas;
    std::unordered_map<std::string, std::shared_ptr<IObjectStorage>> active_object_storages;
};

static void writeFile(
    const std::shared_ptr<IMetadataStorage> & metadata,
    const std::shared_ptr<IObjectStorage> & object_storage,
    const std::string & path,
    const std::string & data)
{
    auto tx = metadata->createTransaction();
    StoredObject object(tx->generateObjectKeyForPath(path).serialize());
    auto buffer = object_storage->writeObject(object, WriteMode::Rewrite);
    buffer->write(data.data(), data.size());
    buffer->preFinalize();
    buffer->finalize();
}

static std::vector<std::string> listAllBlobs(const std::shared_ptr<IObjectStorage> & object_storage)
{
    std::vector<std::string> result;

    const auto & prefix = object_storage->getCommonKeyPrefix();
    if (!fs::exists(prefix))
        return result;

    for (const auto & entry : fs::recursive_directory_iterator(prefix))
        if (entry.is_regular_file())
            result.push_back(entry.path().string());

    std::sort(result.begin(), result.end());
    return result;
}

TEST_F(MetadataPlainDiskTest, RemoveDirectory)
{
    auto metadata = getMetadataStorage("RemoveDirectory");
    auto object_storage = getObjectStorage("RemoveDirectory");

    writeFile(metadata, object_storage, "part_id/file1.txt", "content1");
    writeFile(metadata, object_storage, "part_id/file2.bin", "content2");
    writeFile(metadata, object_storage, "part_id/file3.dat", "content3");
    ASSERT_EQ(listAllBlobs(object_storage).size(), 3u);
    ASSERT_TRUE(metadata->existsFile("part_id/file1.txt"));

    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("part_id");
    }

    EXPECT_FALSE(metadata->existsFile("part_id/file1.txt"));
    EXPECT_FALSE(metadata->existsFile("part_id/file2.bin"));
    EXPECT_FALSE(metadata->existsFile("part_id/file3.dat"));
    EXPECT_TRUE(listAllBlobs(object_storage).empty());
}

TEST_F(MetadataPlainDiskTest, RemoveRecursive)
{
    auto metadata = getMetadataStorage("RemoveRecursive");
    auto object_storage = getObjectStorage("RemoveRecursive");

    writeFile(metadata, object_storage, "uuid-12345/checksums.txt", "abc");
    writeFile(metadata, object_storage, "uuid-12345/columns.txt", "def");
    writeFile(metadata, object_storage, "uuid-12345/data.bin", "ghi");
    ASSERT_EQ(listAllBlobs(object_storage).size(), 3u);

    {
        auto tx = metadata->createTransaction();
        tx->removeRecursive("uuid-12345", {});
    }

    EXPECT_FALSE(metadata->existsFile("uuid-12345/checksums.txt"));
    EXPECT_FALSE(metadata->existsFile("uuid-12345/columns.txt"));
    EXPECT_FALSE(metadata->existsFile("uuid-12345/data.bin"));
    EXPECT_TRUE(listAllBlobs(object_storage).empty());
}

TEST_F(MetadataPlainDiskTest, UnlinkFile)
{
    auto metadata = getMetadataStorage("UnlinkFile");
    auto object_storage = getObjectStorage("UnlinkFile");
    
    writeFile(metadata, object_storage, "some/path/file.txt", "hello");
    ASSERT_TRUE(metadata->existsFile("some/path/file.txt"));
    
    {
        auto tx = metadata->createTransaction();
        tx->unlinkFile("some/path/file.txt", /*if_exists=*/false, /*should_remove_objects=*/true);
    }
    
    EXPECT_FALSE(metadata->existsFile("some/path/file.txt"));
    EXPECT_TRUE(listAllBlobs(object_storage).empty());
}

TEST_F(MetadataPlainDiskTest, RemoveDirectoryNonExistent)
{
    auto metadata = getMetadataStorage("RemoveDirectoryNonExistent");
    auto tx = metadata->createTransaction();
    EXPECT_NO_THROW(tx->removeDirectory("nonexistent_dir"));
}

TEST_F(MetadataPlainDiskTest, RemoveDirectoryInvalidatesObjectMetadataCache)
{
    auto metadata = getMetadataStorage("RemoveDirectoryInvalidatesObjectMetadataCache", /*object_metadata_cache_size=*/1024 * 1024);
    auto object_storage = getObjectStorage("RemoveDirectoryInvalidatesObjectMetadataCache");
    
    writeFile(metadata, object_storage, "part_id/file.txt", "12345");
    ASSERT_EQ(metadata->getFileSize("part_id/file.txt"), 5u);

    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("part_id");
    }

    EXPECT_FALSE(metadata->getFileSizeIfExists("part_id/file.txt").has_value());
    EXPECT_FALSE(metadata->existsFile("part_id/file.txt"));
    EXPECT_TRUE(listAllBlobs(object_storage).empty());
}
