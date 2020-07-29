#pragma once

#include <unordered_map>
#include "DiskDecorator.h"
#include "DiskLocal.h"

namespace DB
{
/**
 * Write buffer with possibility to set and invoke callback when buffer is finalized.
 */
class CompletionAwareWriteBuffer : public WriteBufferFromFileBase
{
public:
    CompletionAwareWriteBuffer(
        std::unique_ptr<WriteBufferFromFileBase> impl_, std::function<void()> completion_callback_, size_t buf_size_);
    ~CompletionAwareWriteBuffer() override;
    void finalize() override;
    void sync() override;
    std::string getFileName() const override;

private:
    void nextImpl() override;

    /// Actual write buffer.
    std::unique_ptr<WriteBufferFromFileBase> impl;
    /// Callback is called when finalize is completed.
    const std::function<void()> completion_callback;
    bool finalized = false;
};

enum FileDownloadStatus
{
    NONE,
    DOWNLOADING,
    DOWNLOADED,
    ERROR
};

struct FileDownloadMetadata
{
    /// Thread waits on this condition if download process is in progress.
    std::condition_variable condition;
    FileDownloadStatus status = NONE;
};

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
    void clearDirectory(const String & path) override;
    void moveDirectory(const String & from_path, const String & to_path) override;
    void moveFile(const String & from_path, const String & to_path) override;
    void replaceFile(const String & from_path, const String & to_path) override;
    void copyFile(const String & from_path, const String & to_path) override;
    std::unique_ptr<ReadBufferFromFileBase>
    readFile(const String & path, size_t buf_size, size_t estimated_size, size_t aio_threshold, size_t mmap_threshold) const override;
    std::unique_ptr<WriteBufferFromFileBase>
    writeFile(const String & path, size_t buf_size, WriteMode mode, size_t estimated_size, size_t aio_threshold) override;
    void remove(const String & path) override;
    void removeRecursive(const String & path) override;
    void createHardLink(const String & src_path, const String & dst_path) override;

private:
    std::shared_ptr<FileDownloadMetadata> acquireDownloadMetadata(const String & path) const;

    std::shared_ptr<DiskLocal> cache_disk;
    const std::function<bool(const String &)> cache_file_predicate;
    /// Contains information about currently running file downloads to cache.
    mutable std::unordered_map<String, std::weak_ptr<FileDownloadMetadata>> file_downloads;
    /// Protects concurrent downloading files to cache.
    mutable std::mutex mutex;
};

}
