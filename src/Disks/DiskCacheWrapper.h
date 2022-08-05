#pragma once

#include <unordered_map>
#include <Common/logger_useful.h>
#include "DiskDecorator.h"
#include "DiskLocal.h"

namespace DB
{
struct FileDownloadMetadata;

/**
 * Simple cache wrapper.
 * Tries to cache files matched by predicate to given local disk (cache disk).
 *
 * When writeFile() is invoked wrapper firstly writes file to cache.
 * After write buffer is finalized actual file is stored to underlying disk.
 *
 * When readFile() is invoked and file exists in cache wrapper reads this file from cache.
 * If file doesn't exist wrapper downloads this file from underlying disk to cache.
 * readFile() invocation is thread-safe.
 */
class DiskCacheWrapper : public DiskDecorator
{
public:
    DiskCacheWrapper(
        std::shared_ptr<IDisk> delegate_,
        std::shared_ptr<DiskLocal> cache_disk_,
        std::function<bool(const String &)> cache_file_predicate_);
    void createDirectory(const String & path) override;
    void createDirectories(const String & path) override;
    void clearDirectory(const String & path) override;
    void moveDirectory(const String & from_path, const String & to_path) override;
    void moveFile(const String & from_path, const String & to_path) override;
    void replaceFile(const String & from_path, const String & to_path) override;

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String & path, size_t buf_size, WriteMode mode, const WriteSettings &) override;

    void removeFile(const String & path) override;
    void removeFileIfExists(const String & path) override;
    void removeDirectory(const String & path) override;
    void removeRecursive(const String & path) override;
    void removeSharedFile(const String & path, bool keep_s3) override;

    void removeSharedRecursive(const String & path, bool keep_all, const NameSet & files_to_keep) override;
    void removeSharedFiles(const RemoveBatchRequest & files, bool keep_all, const NameSet & files_to_keep) override;
    void createHardLink(const String & src_path, const String & dst_path) override;
    bool isFilesHardLinked(const String & src_path, const String & dst_path) const override;
    uint32_t getFileHardLinkCount(const String &) const override;
    ReservationPtr reserve(UInt64 bytes) override;

private:
    std::shared_ptr<FileDownloadMetadata> acquireDownloadMetadata(const String & path) const;

    /// Disk to cache files.
    std::shared_ptr<DiskLocal> cache_disk;
    /// Cache only files satisfies predicate.
    const std::function<bool(const String &)> cache_file_predicate;
    /// Contains information about currently running file downloads to cache.
    mutable std::unordered_map<String, std::weak_ptr<FileDownloadMetadata>> file_downloads;
    /// Protects concurrent downloading files to cache.
    mutable std::mutex mutex;

    Poco::Logger * log = &Poco::Logger::get("DiskCache");
};

}
