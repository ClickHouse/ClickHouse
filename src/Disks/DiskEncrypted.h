#pragma once

#include <Common/config.h>

#if USE_SSL
#include <Disks/IDisk.h>
#include <Disks/DiskDecorator.h>
#include <Common/MultiVersion.h>


namespace DB
{
class ReadBufferFromFileBase;
class WriteBufferFromFileBase;
namespace FileEncryption { enum class Algorithm; }

struct DiskEncryptedSettings
{
    DiskPtr wrapped_disk;
    String disk_path;
    std::unordered_map<UInt64, String> keys;
    UInt64 current_key_id;
    FileEncryption::Algorithm current_algorithm;
};

/// Encrypted disk ciphers all written files on the fly and writes the encrypted files to an underlying (normal) disk.
/// And when we read files from an encrypted disk it deciphers them automatically,
/// so we can work with a encrypted disk like it's a normal disk.
class DiskEncrypted : public DiskDecorator
{
public:
    DiskEncrypted(const String & name_, const Poco::Util::AbstractConfiguration & config_, const String & config_prefix_, const DisksMap & map_);
    DiskEncrypted(const String & name_, std::unique_ptr<const DiskEncryptedSettings> settings_);

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

    void createFile(const String & path) override
    {
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

    void copy(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path) override;

    void copyDirectoryContent(const String & from_dir, const std::shared_ptr<IDisk> & to_disk, const String & to_dir) override;

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) override;

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

    void removeSharedRecursive(const String & path, bool keep_all_batch_data, const NameSet & file_names_remove_metadata_only) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate->removeSharedRecursive(wrapped_path, keep_all_batch_data, file_names_remove_metadata_only);
    }

    void removeSharedFiles(const RemoveBatchRequest & files, bool keep_all_batch_data, const NameSet & file_names_remove_metadata_only) override
    {
        for (const auto & file : files)
        {
            auto wrapped_path = wrappedPath(file.path);
            bool keep = keep_all_batch_data || file_names_remove_metadata_only.contains(fs::path(file.path).filename());
            if (file.if_exists)
                delegate->removeSharedFileIfExists(wrapped_path, keep);
            else
                delegate->removeSharedFile(wrapped_path, keep);
        }
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

    void applyNewSettings(const Poco::Util::AbstractConfiguration & config, ContextPtr context, const String & config_prefix, const DisksMap & map) override;

    DiskType getType() const override { return DiskType::Encrypted; }
    bool isRemote() const override { return delegate->isRemote(); }

    SyncGuardPtr getDirectorySyncGuard(const String & path) const override;

private:
    String wrappedPath(const String & path) const
    {
        // if path starts_with disk_path -> got already wrapped path
        if (!disk_path.empty() && path.starts_with(disk_path))
            return path;
        return disk_path + path;
    }

    const String name;
    const String disk_path;
    const String disk_absolute_path;
    MultiVersion<DiskEncryptedSettings> current_settings;
};

}

#endif
