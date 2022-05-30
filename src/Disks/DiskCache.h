#pragma once

#include <unordered_map>
#include <Common/logger_useful.h>
#include "DiskDecorator.h"
#include <Common/FileCache_fwd.h>

namespace Poco
{
class Logger;
}

namespace DB
{
/**
 * DiskCache implements a disk interface which adds cache to the underkying disk.
 */
class DiskCache : public DiskDecorator
{
friend class CachedWriteBuffer;
public:
    DiskCache(const String & disk_name_, const String & path_, std::shared_ptr<IDisk> delegate_, FileCachePtr cache_);

    const String & getName() const override { return cache_disk_name; }

    DiskType getType() const override { return DiskType::Cache; }

    bool isCached() const override { return true; }

    const String & getCacheBasePath() const override { return cache_base_path; }

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings &) override;

    void clearDirectory(const String & path) override;

    bool removeFile(const String & path) override;

    bool removeFileIfExists(const String & path) override;

    bool removeSharedFile(const String & path, bool keep_in_fs) override;

    bool removeSharedFileIfExists(const String & path, bool keep_in_fs) override;

    void removeSharedFiles(
        const RemoveBatchRequest & requests,
        bool keep_all_batch_data,
        const NameSet & file_names_remove_metadata_only) override;

    void removeDirectory(const String & path) override;

    void removeRecursive(const String & path) override;

    void removeSharedRecursive(
        const String & path,
        bool keep_all_batch_data,
        const NameSet & file_names_remove_metadata_only) override;

    ReservationPtr reserve(UInt64 bytes) override;

private:
    void removeCacheIfExists(const String & path);
    void removeCacheIfExistsRecursive(const String & path);

    String cache_disk_name;
    String cache_base_path;

    FileCachePtr cache;

    Poco::Logger * log;
};

}
