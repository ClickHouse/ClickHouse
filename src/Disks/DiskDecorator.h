#pragma once

#include "Disks/IDisk.h"

namespace DB
{

/** Forwards all methods to another disk.
  * Methods can be overridden by descendants.
  */
class DiskDecorator : public IDisk
{
public:
    explicit DiskDecorator(const DiskPtr & delegate_);
    const String & getName() const override;
    ReservationPtr reserve(UInt64 bytes) override;
    ~DiskDecorator() override = default;
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
    void copyFile(const String & from_path, const String & to_path) override;
    void copy(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path) override;
    void listFiles(const String & path, std::vector<String> & file_names) override;
    std::unique_ptr<ReadBufferFromFileBase>
    readFile(const String & path, size_t buf_size, size_t estimated_size, size_t aio_threshold, size_t mmap_threshold) const override;
    std::unique_ptr<WriteBufferFromFileBase>
    writeFile(const String & path, size_t buf_size, WriteMode mode, size_t estimated_size, size_t aio_threshold) override;
    void remove(const String & path) override;
    void removeRecursive(const String & path) override;
    void setLastModified(const String & path, const Poco::Timestamp & timestamp) override;
    Poco::Timestamp getLastModified(const String & path) override;
    void setReadOnly(const String & path) override;
    void createHardLink(const String & src_path, const String & dst_path) override;
    void truncateFile(const String & path, size_t size) override;
    int open(const String & path, mode_t mode) const override;
    void close(int fd) const override;
    void sync(int fd) const override;
    const String getType() const override { return delegate->getType(); }
    Executor & getExecutor() override;

protected:
    DiskPtr delegate;
};

}
