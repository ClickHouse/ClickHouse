#pragma once

#include "DiskDecorator.h"

#include <common/logger_useful.h>
#include <shared_mutex>

namespace DB
{
using ReadLock = std::shared_lock<std::shared_timed_mutex>;
using WriteLock = std::unique_lock<std::shared_timed_mutex>;

class RestartAwareReadBuffer;
class RestartAwareWriteBuffer;

/**
 * Gives possibility to change underlying disk settings at runtime calling 'restart' method.
 * All disk methods are protected by read-lock. Read/Write buffers produced by disk holds read-lock till buffer is finalized/destructed.
 * When 'restart' method is called write-lock is acquired to make sure that no operations are running on that disk.
 */
class DiskRestartProxy : public DiskDecorator
{
public:
    explicit DiskRestartProxy(DiskPtr & delegate_);

    ReservationPtr reserve(UInt64 bytes) override;
    const String & getPath() const override;
    UInt64 getTotalSpace() const override;
    UInt64 getAvailableSpace() const override;
    UInt64 getUnreservedSpace() const override;
    UInt64 getKeepingFreeSpace() const override;
    bool exists(const String & path) const override;
    bool isFile(const String & path) const override;
    bool isDirectory(const String & path) const override;
    size_t getFileSize(const String & path) const override;
    void createDirectory(const String & path) override;
    void createDirectories(const String & path) override;
    void clearDirectory(const String & path) override;
    void moveDirectory(const String & from_path, const String & to_path) override;
    DiskDirectoryIteratorPtr iterateDirectory(const String & path) override;
    void createFile(const String & path) override;
    void moveFile(const String & from_path, const String & to_path) override;
    void replaceFile(const String & from_path, const String & to_path) override;
    void copy(const String & from_path, const DiskPtr & to_disk, const String & to_path) override;
    void listFiles(const String & path, std::vector<String> & file_names) override;
    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        size_t buf_size,
        size_t estimated_size,
        size_t aio_threshold,
        size_t mmap_threshold,
        MMappedFileCache * mmap_cache) const override;
    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String & path, size_t buf_size, WriteMode mode) override;
    void removeFile(const String & path) override;
    void removeFileIfExists(const String & path) override;
    void removeDirectory(const String & path) override;
    void removeRecursive(const String & path) override;
    void removeSharedFile(const String & path, bool keep_s3) override;
    void removeSharedRecursive(const String & path, bool keep_s3) override;
    void setLastModified(const String & path, const Poco::Timestamp & timestamp) override;
    Poco::Timestamp getLastModified(const String & path) override;
    void setReadOnly(const String & path) override;
    void createHardLink(const String & src_path, const String & dst_path) override;
    void truncateFile(const String & path, size_t size) override;
    String getUniqueId(const String & path) const override;
    bool checkUniqueId(const String & id) const override;

    void restart();

private:
    friend class RestartAwareReadBuffer;
    friend class RestartAwareWriteBuffer;

    /// Mutex to protect RW access.
    mutable std::shared_timed_mutex mutex;

    Poco::Logger * log = &Poco::Logger::get("DiskRestartProxy");
};

}
