#include <gtest/gtest.h>

#include <Disks/DiskLocal.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Cache/MetadataStorageFromCacheObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/InMemoryRemovalQueue.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Local/MetadataStorageFromDisk.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Cached/CachedObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/FileCacheSettings.h>
#include <Interpreters/FileCache/FileSegment.h>
#include <Core/ServerUUID.h>
#include <Common/ObjectStorageKeyGenerator.h>
#include <Common/getRandomASCIIString.h>
#include <Common/tests/gtest_global_context.h>
#include <base/scope_guard.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB::FileCacheSetting
{
    extern const FileCacheSettingsString path;
    extern const FileCacheSettingsUInt64 max_size;
    extern const FileCacheSettingsUInt64 max_elements;
    extern const FileCacheSettingsBool load_metadata_asynchronously;
}

/// A new file may get the key of a removed blob before the blob killer of the cache disk runs, so the cache entries
/// of the removed blobs must be dropped by the commit itself, not only by the killer.
TEST(CachedMetadataStorage, CommitDropsCacheOfRemovedBlobs)
{
    getContext();
    DB::ServerUUID::setRandomForUnitTests();

    const std::string path = "./test-cached-metadata-storage." + DB::getRandomASCIIString(6);
    fs::create_directories(path + "/metadata");
    SCOPE_EXIT({ fs::remove_all(path); });

    DB::FileCacheSettings cache_settings;
    cache_settings[DB::FileCacheSetting::path] = path + "/cache/";
    cache_settings[DB::FileCacheSetting::max_size] = 1024;
    cache_settings[DB::FileCacheSetting::max_elements] = 10;
    cache_settings[DB::FileCacheSetting::load_metadata_asynchronously] = false;
    auto cache = std::make_shared<DB::FileCache>("test-cached-metadata-storage", cache_settings);

    auto object_storage = std::make_shared<DB::LocalObjectStorage>(DB::LocalObjectStorageSettings("test", path + "/blobs/", /*read_only_=*/false));
    auto cached_object_storage = std::make_shared<DB::CachedObjectStorage>(object_storage, cache, cache_settings, "test-cached-metadata-storage");

    auto disk = std::make_shared<DB::DiskLocal>("test-cached-metadata-storage", path + "/metadata");
    auto underlying = std::make_shared<DB::MetadataStorageFromDisk>(
        disk, "", DB::createObjectStorageKeyGeneratorByTemplate("[a-z]{32}"), std::make_unique<DB::InMemoryRemovalQueue>());
    auto metadata = std::make_shared<DB::MetadataStorageFromCacheObjectStorage>(underlying, object_storage, cached_object_storage);

    const DB::StoredObject blob("blob", "f.txt", 3);
    {
        auto transaction = metadata->createTransaction();
        transaction->createMetadataFile("f.txt", {blob});
        transaction->commit(DB::NoCommitOptions{});
    }

    const auto key = DB::FileCacheKey::fromPath(blob.remote_path);
    const auto & origin = DB::FileCache::getCommonOrigin();
    {
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
    ASSERT_EQ(cache->getFileSegmentsNum(), 1);

    {
        auto transaction = metadata->createTransaction();
        transaction->unlinkFile("f.txt", /*if_exists=*/false, /*should_remove_objects=*/true);
        transaction->commit(DB::NoCommitOptions{});
    }

    EXPECT_EQ(cache->getFileSegmentsNum(), 0);
}
