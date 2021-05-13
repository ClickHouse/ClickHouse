#pragma once

#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>


namespace DB
{

class DiskEncrypted : public IDisk
{
public:
    DiskEncrypted(const String & name_, DiskPtr disk_, const String & key_)
        : name(name_), wrapped_disk(std::move(disk_)), key(key_)
    {
    }

    const String & getName() const override { return name; }

    const String & getPath() const override { return wrapped_disk->getPath(); }

    ReservationPtr reserve(UInt64 bytes) override;

    UInt64 getTotalSpace() const override { return wrapped_disk->getTotalSpace(); }

    UInt64 getAvailableSpace() const override { return wrapped_disk->getAvailableSpace(); }

    UInt64 getUnreservedSpace() const override { return wrapped_disk->getUnreservedSpace(); }

    UInt64 getKeepingFreeSpace() const override { return wrapped_disk->getKeepingFreeSpace(); }

    bool exists(const String & path) const override { return wrapped_disk->exists(path); }

    bool isFile(const String & path) const override { return wrapped_disk->isFile(path); }

    bool isDirectory(const String & path) const override { return wrapped_disk->isDirectory(path); }

    size_t getFileSize(const String & path) const override { return wrapped_disk->getFileSize(path); }

    void createDirectory(const String & path) override { wrapped_disk->createDirectory(path); }

    void createDirectories(const String & path) override { wrapped_disk->createDirectories(path); }

    void clearDirectory(const String & path) override { wrapped_disk->clearDirectory(path); }

    void moveDirectory(const String & from_path, const String & to_path) override { wrapped_disk->moveDirectory(from_path, to_path); }

    DiskDirectoryIteratorPtr iterateDirectory(const String & path) override { return wrapped_disk->iterateDirectory(path); }

    void createFile(const String & path) override { wrapped_disk->createFile(path); }

    void moveFile(const String & from_path, const String & to_path) override { wrapped_disk->moveFile(from_path, to_path); }

    void replaceFile(const String & from_path, const String & to_path) override { wrapped_disk->replaceFile(from_path, to_path); }

    void listFiles(const String & path, std::vector<String> & file_names) override { wrapped_disk->listFiles(path, file_names); }

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        size_t buf_size,
        size_t estimated_size,
        size_t aio_threshold,
        size_t mmap_threshold,
        MMappedFileCache * mmap_cache) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & path,
        size_t buf_size,
        WriteMode mode) override;

    void removeFile(const String & path) override { wrapped_disk->removeFile(path); }

    void removeFileIfExists(const String & path) override { wrapped_disk->removeFileIfExists(path); }

    void removeDirectory(const String & path) override { wrapped_disk->removeDirectory(path); }

    void removeRecursive(const String & path) override { wrapped_disk->removeRecursive(path); }

    void setLastModified(const String & path, const Poco::Timestamp & timestamp) override { wrapped_disk->setLastModified(path, timestamp); }

    Poco::Timestamp getLastModified(const String & path) override { return wrapped_disk->getLastModified(path); }

    void setReadOnly(const String & path) override { wrapped_disk->setReadOnly(path); }

    void createHardLink(const String & src_path, const String & dst_path) override { wrapped_disk->createHardLink(src_path, dst_path); }

    void truncateFile(const String & path, size_t size) override;

    DiskType::Type getType() const override { return DiskType::Type::Encrypted; }

    SyncGuardPtr getDirectorySyncGuard(const String & path) const override;


private:
    String name;
    DiskPtr wrapped_disk;
    String key;
};

}
