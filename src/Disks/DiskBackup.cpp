#include <Disks/DiskBackup.h>

#include <Common/logger_useful.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Backups/BackupFactory.h>

#include <Disks/DiskFactory.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_METHOD;
}


namespace
{

class DiskBackupDirectoryIterator final : public IDirectoryIterator
{
public:
    explicit DiskBackupDirectoryIterator(std::string dir_path_, std::vector<std::string> files_)
        : dir_path(std::move(dir_path_)), files(std::move(files_))
    {
    }

    void next() override { ++index; }

    bool isValid() const override { return index < files.size(); }

    String path() const override
    {
        return fmt::format("{}/{}", dir_path, files[index]);
    }

    String name() const override { return files[index]; }

private:
    std::string dir_path;
    std::vector<std::string> files;
    size_t index = 0;
};

}

DiskBackup::DiskBackup(std::shared_ptr<IBackup> backup_,
    PathPrefixReplacement path_prefix_replacement_,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix)
    : IDisk("backup", config, config_prefix)
    , backup(std::move(backup_))
    , path_prefix_replacement(std::move(path_prefix_replacement_))
    , logger(getLogger("DiskBackup"))
{
}

ReservationPtr DiskBackup::reserve(UInt64)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "DiskBackup does not support reserve method");
}

std::optional<UInt64> DiskBackup::getTotalSpace() const
{
    return backup->getTotalSize();
}

std::optional<UInt64> DiskBackup::getAvailableSpace() const
{
    return 0;
}

std::optional<UInt64> DiskBackup::getUnreservedSpace() const
{
    return 0;
}

bool DiskBackup::existsFileOrDirectory(const String & path) const
{
    std::string replaced_path = replacePathPrefix(path);
    return backup->fileExists(replaced_path) || backup->directoryExists(replaced_path);
}

bool DiskBackup::existsFile(const String & path) const
{
    std::string replaced_path = replacePathPrefix(path);
    return backup->fileExists(replaced_path);
}

bool DiskBackup::existsDirectory(const String & path) const
{
    std::string replaced_path = replacePathPrefix(path);
    return backup->directoryExists(replaced_path);
}

size_t DiskBackup::getFileSize(const String & path) const
{
    std::string replaced_path = replacePathPrefix(path);
    return backup->getFileSize(replaced_path);
}

void DiskBackup::createDirectory(const String & path)
{
    std::string replaced_path = replacePathPrefix(path);
    if (!backup->directoryExists(replaced_path))
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "DiskBackup does not support createDirectory method");
}

void DiskBackup::createDirectories(const String & path)
{
    std::string replaced_path = replacePathPrefix(path);
    if (!backup->directoryExists(replaced_path))
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "DiskBackup does not support createDirectories method");
}

void DiskBackup::clearDirectory(const String &)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "DiskBackup does not support clearDirectory method");
}

void DiskBackup::moveDirectory(const String &, const String &)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "DiskBackup does not support moveDirectory method");
}

DirectoryIteratorPtr DiskBackup::iterateDirectory(const String & path) const
{
    std::string replaced_path = replacePathPrefix(path);
    return std::make_unique<DiskBackupDirectoryIterator>(replaced_path, backup->listFiles(replaced_path, false /*recursive*/));
}

void DiskBackup::moveFile(const String &, const String &)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "DiskBackup does not support moveFile method");
}

void DiskBackup::replaceFile(const String &, const String &)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "DiskBackup does not support replaceFile method");
}

std::unique_ptr<ReadBufferFromFileBase>
DiskBackup::readFile(const String & path, const ReadSettings &, std::optional<size_t>, std::optional<size_t>) const
{
    std::string replaced_path = replacePathPrefix(path);
    return backup->readFile(replaced_path);
}

std::unique_ptr<WriteBufferFromFileBase> DiskBackup::writeFile(const String &, size_t, WriteMode, const WriteSettings &)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "DiskBackup does not support writeFile method");
}

std::vector<String> DiskBackup::getBlobPath(const String &) const
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "DiskBackup does not support getBlobPath method");
}

void DiskBackup::writeFileUsingBlobWritingFunction(const String &, WriteMode, WriteBlobFunction &&)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "DiskBackup does not support writeFileUsingBlobWritingFunction method");
}

void DiskBackup::removeFile(const String &)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "DiskBackup does not support removeFile method");
}

void DiskBackup::removeFileIfExists(const String &)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "DiskBackup does not support removeFileIfExists method");
}

void DiskBackup::removeDirectory(const String &)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "DiskBackup does not support removeDirectory method");
}

void DiskBackup::removeRecursive(const String &)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "DiskBackup does not support removeRecursive method");
}

void DiskBackup::listFiles(const String & path, std::vector<String> & file_names) const
{
    std::string replaced_path = replacePathPrefix(path);
    file_names = backup->listFiles(replaced_path, false /*recursive*/);
}

void DiskBackup::setLastModified(const String & path, const Poco::Timestamp & timestamp)
{
    std::string replaced_path = replacePathPrefix(path);

    std::lock_guard lock(mutex);
    last_modified[replaced_path] = timestamp;
}

Poco::Timestamp DiskBackup::getLastModified(const String & path) const
{
    std::string replaced_path = replacePathPrefix(path);

    {
        std::lock_guard lock(mutex);
        auto it = last_modified.find(replaced_path);
        if (it != last_modified.end())
            return it->second;
    }

    return backup->getTimestamp();
}

time_t DiskBackup::getLastChanged(const String &) const
{
    return backup->getTimestamp();
}

void DiskBackup::createHardLink(const String &, const String &)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "DiskBackup does not support createHardLink method");
}

void DiskBackup::truncateFile(const String &, size_t)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "DiskBackup does not support truncateFile method");
}

void DiskBackup::createFile(const String &)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "DiskBackup does not support createFile method");
}

void DiskBackup::setReadOnly(const String &)
{
}

void DiskBackup::copyDirectoryContent(
    const String & /*from_dir*/,
    const std::shared_ptr<IDisk> & /*to_disk*/,
    const String & /*to_dir*/,
    const ReadSettings & /*read_settings*/,
    const WriteSettings & /*write_settings*/,
    const std::function<void()> & /*cancellation_hook*/)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "DiskBackup does not support copyDirectoryContent method");
}

SyncGuardPtr DiskBackup::getDirectorySyncGuard(const String &) const
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "DiskBackup does not support getDirectorySyncGuard method");
}

void DiskBackup::applyNewSettings(const Poco::Util::AbstractConfiguration &, ContextPtr, const String &, const DisksMap &)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "DiskBackup does not support applyNewSettings method");
}

DataSourceDescription DiskBackup::getDataSourceDescription() const
{
    DataSourceDescription description;
    description.type = DataSourceType::ObjectStorage;
    description.object_storage_type = ObjectStorageType::None;
    description.metadata_type = MetadataStorageType::None;
    description.description = "DiskBackup";

    return description;
}

void DiskBackup::shutdown()
{
}

String DiskBackup::replacePathPrefix(const String & path) const
{
    if (path.starts_with(path_prefix_replacement.from))
        return path_prefix_replacement.to + path.substr(path_prefix_replacement.from.size());

    return path;
}

}
