#include <gtest/gtest.h>

#include <Disks/DiskLocal.h>
#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Cache/MetadataStorageFromCacheObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/InMemoryRemovalQueue.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Local/MetadataStorageFromDisk.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Cached/CachedObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/DiskObjectStorage/Replication/ClusterConfiguration.h>
#include <Disks/DiskObjectStorage/Replication/ObjectStorageRouter.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/FileCacheSettings.h>
#include <Interpreters/FileCache/FileSegment.h>
#include <Core/ServerUUID.h>
#include <Common/ObjectStorageKeyGenerator.h>
#include <Common/getRandomASCIIString.h>
#include <Common/tests/gtest_global_context.h>

#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB::FileCacheSetting
{
    extern const FileCacheSettingsString path;
    extern const FileCacheSettingsUInt64 max_size;
    extern const FileCacheSettingsUInt64 max_elements;
    extern const FileCacheSettingsBool load_metadata_asynchronously;
}

class CachedMetadataStorageTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        getContext();
        DB::ServerUUID::setRandomForUnitTests();

        fs::create_directories(path + "/metadata");

        DB::FileCacheSettings cache_settings;
        cache_settings[DB::FileCacheSetting::path] = path + "/cache/";
        cache_settings[DB::FileCacheSetting::max_size] = 1024;
        cache_settings[DB::FileCacheSetting::max_elements] = 10;
        cache_settings[DB::FileCacheSetting::load_metadata_asynchronously] = false;
        cache = std::make_shared<DB::FileCache>("test-cached-metadata-storage", cache_settings);

        object_storage = std::make_shared<DB::LocalObjectStorage>(DB::LocalObjectStorageSettings("test", path + "/blobs/", /*read_only_=*/false));
        auto cached_object_storage = std::make_shared<DB::CachedObjectStorage>(object_storage, cache, cache_settings, "test-cached-metadata-storage");

        auto metadata_disk = std::make_shared<DB::DiskLocal>("test-cached-metadata-storage", path + "/metadata");
        auto underlying = std::make_shared<DB::MetadataStorageFromDisk>(
            metadata_disk, "", DB::createObjectStorageKeyGeneratorByTemplate("[a-z]{32}"), std::make_unique<DB::InMemoryRemovalQueue>());
        metadata = std::make_shared<DB::MetadataStorageFromCacheObjectStorage>(underlying, object_storage, cached_object_storage);

        std::unordered_map<DB::Location, DB::LocationInfo> cluster_registry = {{"main", {true, true, ""}}};
        std::unordered_map<DB::Location, DB::ObjectStoragePtr> object_storage_registry = {{"main", cached_object_storage}};
        Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration());
        disk = std::make_shared<DB::DiskObjectStorage>(
            "test-cached-metadata-storage",
            std::make_shared<DB::ClusterConfiguration>("test-cached-metadata-storage", std::move(cluster_registry)),
            metadata,
            std::make_shared<DB::ObjectStorageRouter>(std::move(object_storage_registry)),
            nullptr,
            *config,
            "");
    }

    void TearDown() override
    {
        disk.reset();
        metadata.reset();
        cache.reset();
        fs::remove_all(path);
    }

    /// Downloads the whole blob into the cache on behalf of `origin`.
    void cacheBlob(const DB::StoredObject & blob, const DB::FileCacheOriginInfo & origin) const
    {
        const auto key = DB::FileCacheKey::fromPath(blob.remote_path);
        auto holder = cache->set(key, 0, blob.bytes_size, DB::CreateFileSegmentSettings{}, origin);
        auto & segment = holder->front();
        ASSERT_EQ(segment.getOrSetDownloader(), DB::FileSegment::getCallerId());
        std::string failure_reason;
        ASSERT_TRUE(segment.reserve(blob.bytes_size, 1000, failure_reason));
        fs::create_directories(cache->getKeyPath(key, origin));
        std::string data(blob.bytes_size, 'x');
        segment.write(data.data(), data.size(), 0);
        holder->completeAndPopFront(/*allow_background_download=*/false, /*force_shrink_to_downloaded_size=*/false);
    }

    void writeFile(const std::string & file, const std::string & data) const
    {
        auto buf = disk->writeFile(file, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteMode::Rewrite, DB::WriteSettings{});
        writeString(data, *buf);
        buf->finalize();
    }

    [[nodiscard]] std::string readFile(const std::string & file) const
    {
        auto buf = disk->readFile(file, DB::ReadSettings{});
        std::string data;
        readStringUntilEOF(data, *buf);
        return data;
    }

    const std::string path = "./test-cached-metadata-storage." + DB::getRandomASCIIString(6);
    DB::FileCachePtr cache;
    std::shared_ptr<DB::LocalObjectStorage> object_storage;
    std::shared_ptr<DB::MetadataStorageFromCacheObjectStorage> metadata;
    std::shared_ptr<DB::DiskObjectStorage> disk;
};

/// A new file may get the key of a removed blob before the blob killer of the cache disk runs, so the cache entries
/// of the removed blobs must be dropped by the commit itself, not only by the killer.
TEST_F(CachedMetadataStorageTest, CommitDropsCacheOfRemovedBlobs)
{
    const DB::StoredObject blob("blob", "f.txt", 3);
    {
        auto transaction = metadata->createTransaction();
        transaction->createMetadataFile("f.txt", {blob});
        transaction->commit(DB::NoCommitOptions{});
    }

    cacheBlob(blob, DB::FileCache::getCommonOrigin());
    ASSERT_EQ(cache->getFileSegmentsNum(), 1);

    {
        auto transaction = metadata->createTransaction();
        transaction->unlinkFile("f.txt", /*if_exists=*/false, /*should_remove_objects=*/true);
        transaction->commit(DB::NoCommitOptions{});
    }

    EXPECT_EQ(cache->getFileSegmentsNum(), 0);
}

/// The cache entries are dropped after the metadata is committed, so a failure to drop them must not make
/// the disk transaction roll back the blobs it has written: the committed metadata already refers to them.
TEST_F(CachedMetadataStorageTest, FailedCacheDropKeepsCommittedBlobs)
{
    writeFile("f.txt", "OLD");
    const auto old_blobs = disk->getStorageObjects("f.txt");
    ASSERT_EQ(old_blobs.size(), 1);

    /// The entry belongs to another user of the cache, so dropping it on behalf of the disk throws `FILECACHE_ACCESS_DENIED`.
    cacheBlob(old_blobs.front(), DB::FileCacheOriginInfo("another_user", /*weight_=*/0));
    ASSERT_EQ(cache->getFileSegmentsNum(), 1);

    EXPECT_NO_THROW(writeFile("f.txt", "NEW"));

    const auto new_blobs = disk->getStorageObjects("f.txt");
    ASSERT_EQ(new_blobs.size(), 1);
    ASSERT_TRUE(object_storage->exists(new_blobs.front()));
    EXPECT_EQ(readFile("f.txt"), "NEW");
}
