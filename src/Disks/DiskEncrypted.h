#pragma once

#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class DiskEncrypted : public IDisk
{
public:
    DiskEncrypted(const String & name_, DiskPtr disk_, const String & key_, const String & path_)
        : name(name_), wrapped_disk(std::move(disk_)), key(key_), disk_path(path_)
        , disk_absolute_path(wrapped_disk->getPath() + disk_path)
    {
        // use wrapped_disk as an EncryptedDisk store
        if (disk_path.empty())
            return;

        if (disk_path.back() != '/')
            throw Exception("Disk path must ends with '/', but '" + disk_path + "' doesn't.", ErrorCodes::LOGICAL_ERROR);

        wrapped_disk->createDirectory(disk_path);
    }

    const String & getName() const override { return name; }

    const String & getPath() const override { return disk_absolute_path; }

    ReservationPtr reserve(UInt64 bytes) override;

    UInt64 getTotalSpace() const override { return wrapped_disk->getTotalSpace(); }

    UInt64 getAvailableSpace() const override { return wrapped_disk->getAvailableSpace(); }

    UInt64 getUnreservedSpace() const override { return wrapped_disk->getUnreservedSpace(); }

    UInt64 getKeepingFreeSpace() const override { return wrapped_disk->getKeepingFreeSpace(); }

    bool exists(const String & path) const override
    {
        auto wrapped_path = wrappedPath(path);
        return wrapped_disk->exists(wrapped_path);
    }

    bool isFile(const String & path) const override
    {
        auto wrapped_path = wrappedPath(path);
        return wrapped_disk->isFile(wrapped_path);
    }

    bool isDirectory(const String & path) const override
    {
        auto wrapped_path = wrappedPath(path);
        return wrapped_disk->isDirectory(wrapped_path);
    }

    size_t getFileSize(const String & path) const override
    {
        auto wrapped_path = wrappedPath(path);
        return wrapped_disk->getFileSize(wrapped_path);
    }

    void createDirectory(const String & path) override
    {
        auto wrapped_path = wrappedPath(path);
        wrapped_disk->createDirectory(wrapped_path);
    }

    void createDirectories(const String & path) override
    {
        auto wrapped_path = wrappedPath(path);
        wrapped_disk->createDirectories(wrapped_path);
    }


    void clearDirectory(const String & path) override
    {
        auto wrapped_path = wrappedPath(path);
        wrapped_disk->clearDirectory(wrapped_path);
    }

    void moveDirectory(const String & from_path, const String & to_path) override
    {
        auto wrapped_from_path = wrappedPath(from_path);
        auto wrapped_to_path = wrappedPath(to_path);
        wrapped_disk->moveDirectory(wrapped_from_path, wrapped_to_path);
    }

    DiskDirectoryIteratorPtr iterateDirectory(const String & path) override
    {
        auto wrapped_path = wrappedPath(path);
        return wrapped_disk->iterateDirectory(wrapped_path);
    }

    void createFile(const String & path) override {
        auto wrapped_path = wrappedPath(path);
        wrapped_disk->createFile(wrapped_path);
    }

    void moveFile(const String & from_path, const String & to_path) override
    {
        auto wrapped_from_path = wrappedPath(from_path);
        auto wrapped_to_path = wrappedPath(to_path);
        wrapped_disk->moveFile(wrapped_from_path, wrapped_to_path);
    }

    void replaceFile(const String & from_path, const String & to_path) override
    {
        auto wrapped_from_path = wrappedPath(from_path);
        auto wrapped_to_path = wrappedPath(to_path);
        wrapped_disk->replaceFile(wrapped_from_path, wrapped_to_path);
    }

    void listFiles(const String & path, std::vector<String> & file_names) override
    {
        auto wrapped_path = wrappedPath(path);
        wrapped_disk->listFiles(wrapped_path, file_names);
    }

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

    void removeFile(const String & path) override
    {
        auto wrapped_path = wrappedPath(path);
        wrapped_disk->removeFile(wrapped_path);
    }

    void removeFileIfExists(const String & path) override
    {
        auto wrapped_path = wrappedPath(path);
        wrapped_disk->removeFileIfExists(wrapped_path);
    }

    void removeDirectory(const String & path) override
    {
        auto wrapped_path = wrappedPath(path);
        wrapped_disk->removeDirectory(wrapped_path);
    }

    void removeRecursive(const String & path) override
    {
        auto wrapped_path = wrappedPath(path);
        wrapped_disk->removeRecursive(wrapped_path);
    }

    void setLastModified(const String & path, const Poco::Timestamp & timestamp) override
    {
        auto wrapped_path = wrappedPath(path);
        wrapped_disk->setLastModified(wrapped_path, timestamp);
    }

    Poco::Timestamp getLastModified(const String & path) override
    {
        auto wrapped_path = wrappedPath(path);
        return wrapped_disk->getLastModified(wrapped_path);
    }

    void setReadOnly(const String & path) override
    {
        auto wrapped_path = wrappedPath(path);
        wrapped_disk->setReadOnly(wrapped_path);
    }

    void createHardLink(const String & src_path, const String & dst_path) override
    {
        auto wrapped_src_path = wrappedPath(src_path);
        auto wrapped_dst_path = wrappedPath(dst_path);
        wrapped_disk->createHardLink(wrapped_src_path, wrapped_dst_path);
    }

    void truncateFile(const String & path, size_t size) override;

    DiskType::Type getType() const override { return DiskType::Type::Encrypted; }

    SyncGuardPtr getDirectorySyncGuard(const String & path) const override;


private:
    String wrappedPath(const String & path) const { return disk_path + path; }

    String name;
    DiskPtr wrapped_disk;
    String key;
    String disk_path;
    String disk_absolute_path;
};

}
