#pragma once

#include <Disks/IDisk.h>
#include <Disks/DiskDecorator.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class DiskEncrypted : public DiskDecorator
{
public:
    DiskEncrypted(const String & name_, DiskPtr disk_, const String & key_, const String & path_)
        : DiskDecorator(disk_)
        , name(name_), key(key_), disk_path(path_)
        , disk_absolute_path(delegate->getPath() + disk_path)
    {
        initialize();
    }

    const String & getName() const override { return name; }

    const String & getPath() const override { return disk_absolute_path; }

    ReservationPtr reserve(UInt64 bytes) override;

    bool exists(const String & path) const override
    {
        auto wrapped_path = wrappedPath(path);
        return delegate->exists(wrapped_path);
    }

    bool isFile(const String & path) const override
    {
        auto wrapped_path = wrappedPath(path);
        return delegate->isFile(wrapped_path);
    }

    bool isDirectory(const String & path) const override
    {
        auto wrapped_path = wrappedPath(path);
        return delegate->isDirectory(wrapped_path);
    }

    size_t getFileSize(const String & path) const override;

    void createDirectory(const String & path) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate->createDirectory(wrapped_path);
    }

    void createDirectories(const String & path) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate->createDirectories(wrapped_path);
    }


    void clearDirectory(const String & path) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate->clearDirectory(wrapped_path);
    }

    void moveDirectory(const String & from_path, const String & to_path) override
    {
        auto wrapped_from_path = wrappedPath(from_path);
        auto wrapped_to_path = wrappedPath(to_path);
        delegate->moveDirectory(wrapped_from_path, wrapped_to_path);
    }

    DiskDirectoryIteratorPtr iterateDirectory(const String & path) override
    {
        auto wrapped_path = wrappedPath(path);
        return delegate->iterateDirectory(wrapped_path);
    }

    void createFile(const String & path) override {
        auto wrapped_path = wrappedPath(path);
        delegate->createFile(wrapped_path);
    }

    void moveFile(const String & from_path, const String & to_path) override
    {
        auto wrapped_from_path = wrappedPath(from_path);
        auto wrapped_to_path = wrappedPath(to_path);
        delegate->moveFile(wrapped_from_path, wrapped_to_path);
    }

    void replaceFile(const String & from_path, const String & to_path) override
    {
        auto wrapped_from_path = wrappedPath(from_path);
        auto wrapped_to_path = wrappedPath(to_path);
        delegate->replaceFile(wrapped_from_path, wrapped_to_path);
    }

    void listFiles(const String & path, std::vector<String> & file_names) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate->listFiles(wrapped_path, file_names);
    }

    void copy(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path) override
    {
	IDisk::copy(from_path, to_disk, to_path);
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
        delegate->removeFile(wrapped_path);
    }

    void removeFileIfExists(const String & path) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate->removeFileIfExists(wrapped_path);
    }

    void removeDirectory(const String & path) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate->removeDirectory(wrapped_path);
    }

    void removeRecursive(const String & path) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate->removeRecursive(wrapped_path);
    }

    void removeSharedFile(const String & path, bool flag) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate->removeSharedFile(wrapped_path, flag);
    }

    void removeSharedRecursive(const String & path, bool flag) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate->removeSharedRecursive(wrapped_path, flag);
    }

    void removeSharedFileIfExists(const String & path, bool flag) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate->removeSharedFileIfExists(wrapped_path, flag);
    }

    void setLastModified(const String & path, const Poco::Timestamp & timestamp) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate->setLastModified(wrapped_path, timestamp);
    }

    Poco::Timestamp getLastModified(const String & path) override
    {
        auto wrapped_path = wrappedPath(path);
        return delegate->getLastModified(wrapped_path);
    }

    void setReadOnly(const String & path) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate->setReadOnly(wrapped_path);
    }

    void createHardLink(const String & src_path, const String & dst_path) override
    {
        auto wrapped_src_path = wrappedPath(src_path);
        auto wrapped_dst_path = wrappedPath(dst_path);
        delegate->createHardLink(wrapped_src_path, wrapped_dst_path);
    }

    void truncateFile(const String & path, size_t size) override;

    String getUniqueId(const String & path) const override
    {
        auto wrapped_path = wrappedPath(path);
        return delegate->getUniqueId(wrapped_path);
    }

    void onFreeze(const String & path) override
    {
        auto wrapped_path = wrappedPath(path);
	delegate->onFreeze(wrapped_path);
    }

    void applyNewSettings(const Poco::Util::AbstractConfiguration & config, ContextConstPtr context, const String & config_prefix, const DisksMap & map) override;

    DiskType::Type getType() const override { return DiskType::Type::Encrypted; }

    SyncGuardPtr getDirectorySyncGuard(const String & path) const override;


private:
    String wrappedPath(const String & path) const
    {
        // if path starts_with disk_path -> got already wrapped path
        if (!disk_path.empty() && path.rfind(disk_path, 0) == 0)
            return path;
        return disk_path + path;
    }

    void initialize()
    {
        // use wrapped_disk as an EncryptedDisk store
        if (disk_path.empty())
            return;

        if (disk_path.back() != '/')
            throw Exception("Disk path must ends with '/', but '" + disk_path + "' doesn't.", ErrorCodes::LOGICAL_ERROR);

        delegate->createDirectory(disk_path);
    }

    String name;
    String key;
    String disk_path;
    String disk_absolute_path;
};

}
