#pragma once

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Interpreters/FileCache/FileCache_fwd.h>
#include <Interpreters/FileCache/FileSegment.h>

#include <mutex>
#include <unordered_map>

namespace DB
{

/// Object storage that borrows space from a filesystem cache.
/// Each stored object is backed by an ephemeral FileSegment in the cache.
/// When the holder is released (e.g. on shutdown), the cache can reclaim the space.
/// Intended for temporary tables where data loss on restart is acceptable.
class BorrowFromCacheObjectStorage : public IObjectStorage
{
public:
    BorrowFromCacheObjectStorage(const std::string & name_, FileCachePtr file_cache_);

    std::string getName() const override { return "BorrowFromCache"; }

    std::string getDiskName() const override { return name; }

    ObjectStorageType getType() const override { return ObjectStorageType::BorrowFromCache; }

    std::string getCommonKeyPrefix() const override { return ""; }

    std::string getDescription() const override { return "borrow_from_cache:" + name; }

    bool exists(const StoredObject & object) const override;

    std::unique_ptr<ReadBufferFromFileBase> readObject( /// NOLINT
        const StoredObject & object,
        const ReadSettings & read_settings,
        std::optional<size_t> read_hint = {},
        bool use_external_buffer = false,
        bool restrict_seek = false) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const StoredObject & object,
        WriteMode mode,
        std::optional<ObjectAttributes> attributes,
        size_t buf_size,
        const WriteSettings & write_settings) override;

    void removeObjectIfExists(const StoredObject & object) override;

    void removeObjectsIfExist(const StoredObjects & objects) override;

    ObjectMetadata getObjectMetadata(const std::string & path, bool with_tags) const override;

    std::optional<ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool with_tags) const override;

    void copyObject( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::optional<ObjectAttributes> object_to_attributes) override;

    void shutdown() override;

    void startup() override;

    String getObjectsNamespace() const override { return ""; }

    bool isRemote() const override { return false; }

    ObjectStorageKeyGeneratorPtr createKeyGenerator() const override;

    ReadSettings patchSettings(const ReadSettings & read_settings) const override;

private:
    struct SegmentEntry
    {
        /// Shared with the active `WriteBufferToFileSegment` decorator, so the holder
        /// (and the cache reservation it pins) outlives both the entry and the buffer
        /// regardless of which one is destroyed first.
        std::shared_ptr<FileSegmentsHolder> holder;
        std::string cache_path; /// Path on the local filesystem inside the cache
    };

    SegmentEntry * findEntry(const std::string & remote_path) const;

    std::string name;
    FileCachePtr file_cache;

    mutable std::mutex mutex;
    mutable std::unordered_map<std::string, SegmentEntry> entries;

    LoggerPtr log;
};

}
