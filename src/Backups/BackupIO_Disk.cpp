#include <Backups/BackupIO_Disk.h>
#include <Common/checkStackSize.h>
#include <Common/logger_useful.h>
#include <Disks/IDisk.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

BackupReaderDisk::BackupReaderDisk(const DiskPtr & disk_, const String & root_path_, const ReadSettings & read_settings_, const WriteSettings & write_settings_)
    : BackupReaderDefault(read_settings_, write_settings_, getLogger("BackupReaderDisk"))
    , disk(disk_)
    , root_path(root_path_)
    , data_source_description(disk->getDataSourceDescription())
{
}

BackupReaderDisk::~BackupReaderDisk() = default;

bool BackupReaderDisk::fileExists(const String & file_name)
{
    return disk->existsFile(root_path / file_name);
}

UInt64 BackupReaderDisk::getFileSize(const String & file_name)
{
    return disk->getFileSize(root_path / file_name);
}

std::unique_ptr<ReadBufferFromFileBase> BackupReaderDisk::readFile(const String & file_name)
{
    return disk->readFile(root_path / file_name, read_settings);
}

void BackupReaderDisk::copyFileToDisk(const String & path_in_backup, size_t file_size, bool encrypted_in_backup,
                                      DiskPtr destination_disk, const String & destination_path, WriteMode write_mode)
{
    /// Use `IDisk::copyFile` as a more optimal way to copy a file if it's possible.
    /// However `IDisk::copyFile` can't use throttling for reading, and can't copy an encrypted file or do appending.
    bool has_throttling = disk->isRemote() ? static_cast<bool>(read_settings.remote_throttler) : static_cast<bool>(read_settings.local_throttler);
    if (!has_throttling && (write_mode == WriteMode::Rewrite) && !encrypted_in_backup)
    {
        auto destination_data_source_description = destination_disk->getDataSourceDescription();
        if (destination_data_source_description.sameKind(data_source_description) && !data_source_description.is_encrypted)
        {
            /// Use more optimal way.
            LOG_TRACE(log, "Copying file {} from disk {} to disk {}", path_in_backup, disk->getName(), destination_disk->getName());
            disk->copyFile(root_path / path_in_backup, *destination_disk, destination_path, read_settings, write_settings);
            return; /// copied!
        }
    }

    /// Fallback to copy through buffers.
    BackupReaderDefault::copyFileToDisk(path_in_backup, file_size, encrypted_in_backup, destination_disk, destination_path, write_mode);
}


/// For an object-storage disk that keeps its metadata in local files (`metadata_type = local`), the
/// absolute directory under which the metadata file of a disk-relative path lives; nullopt for every
/// other disk. Those metadata files are written like any other local file, without a sync, and a
/// backup on such a disk is reached only through them: after a power loss the uploaded objects can
/// survive while the metadata pointing at them is gone.
static std::optional<fs::path> getLocalMetadataRoot(const DiskPtr & disk)
{
    const auto & description = disk->getDataSourceDescription();
    if (description.type != DataSourceType::ObjectStorage || description.metadata_type != MetadataStorageType::Local)
        return std::nullopt;

    /// An encrypting wrapper's own metadata storage view reports a path that is not a location in the
    /// filesystem, so the root is the one of the disk that stores the files.
    DiskPtr inner = disk;
    while (auto delegate = inner->getDelegateDiskIfExists())
        inner = delegate;

    return fs::path(inner->getMetadataStorage()->getPath());
}

/// A disk-relative path as the innermost delegate disk sees it: each encrypting wrapper on the way
/// prepends the prefix it keeps its files under, except when the path already starts with that
/// prefix, which the wrapper treats as an already wrapped path (`DiskEncryptedTransaction::wrappedPath`).
static fs::path getPathInInnermostDelegate(const DiskPtr & disk, const fs::path & path)
{
    String result = path;
    DiskPtr inner = disk;
    while (auto delegate = inner->getDelegateDiskIfExists())
    {
        const String & inner_path = inner->getPath();
        const String & delegate_path = delegate->getPath();
        if (!inner_path.starts_with(delegate_path))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Disk {} at {} is not located inside its delegate disk {} at {}",
                inner->getName(), inner_path, delegate->getName(), delegate_path);
        const String prefix = inner_path.substr(delegate_path.size());
        if (!result.starts_with(prefix))
            result = prefix + result;
        inner = delegate;
    }

    return result;
}

BackupWriterDisk::BackupWriterDisk(const DiskPtr & disk_, const String & root_path_, const ReadSettings & read_settings_, const WriteSettings & write_settings_)
    : BackupWriterDefault(read_settings_, write_settings_, getLogger("BackupWriterDisk"))
    , disk(disk_)
    , root_path(root_path_)
    , data_source_description(disk->getDataSourceDescription())
    /// Only a destination that keeps the backup as plain local files can be made durable by
    /// fsyncing them, and only there does `getBlobPath` map a path to the file holding it.
    /// Not `isRemote`: DiskObjectStorage reports true even over LocalObjectStorage, and
    /// DiskBackup reports false yet throws from `getBlobPath`.
    , destination_is_plain_local_files(data_source_description.type == DataSourceType::Local)
    , local_metadata_root(getLocalMetadataRoot(disk))
{
    if (data_source_description.object_storage_type == ObjectStorageType::Local)
    {
        /// The blobs are local files written without a sync, and `getBlobPath` does not map a path
        /// to them, so even with its metadata synced a backup on this disk is not durable.
        LOG_WARNING(
            log,
            "Disk {} ({}) stores data locally but not as plain files, so fsync_backup_files cannot make a backup on it durable",
            disk->getName(),
            data_source_description.name());
    }
}

BackupWriterDisk::~BackupWriterDisk() = default;

bool BackupWriterDisk::fileExists(const String & file_name)
{
    return disk->existsFile(root_path / file_name);
}

UInt64 BackupWriterDisk::getFileSize(const String & file_name)
{
    return disk->getFileSize(root_path / file_name);
}

std::unique_ptr<ReadBuffer> BackupWriterDisk::readFile(const String & file_name, size_t expected_file_size)
{
    return disk->readFile(root_path / file_name, read_settings.adjustBufferSize(expected_file_size));
}

std::unique_ptr<WriteBuffer> BackupWriterDisk::writeFile(const String & file_name)
{
    auto file_path = root_path / file_name;
    disk->createDirectories(file_path.parent_path());
    return disk->writeFile(file_path, write_buffer_size, WriteMode::Rewrite, write_settings);
}

void BackupWriterDisk::removeFile(const String & file_name)
{
    disk->removeFileIfExists(root_path / file_name);
}

void BackupWriterDisk::removeEmptyDirectories()
{
    /// When using archive-based backups, root_path is the parent of the archive filename.
    /// For single-component paths like "backup1.tzst", this becomes empty (the disk root).
    /// We must not traverse and remove directories starting from the disk root,
    /// as that would affect the entire disk, not just the backup's directories.
    if (root_path.empty())
        return;

    removeEmptyDirectoriesImpl(root_path);
}

void BackupWriterDisk::removeEmptyDirectoriesImpl(const fs::path & current_dir)
{
    checkStackSize();

    if (!disk->existsDirectory(current_dir))
        return;

    if (disk->isDirectoryEmpty(current_dir))
    {
        disk->removeDirectory(current_dir);
        return;
    }

    for (auto it = disk->iterateDirectory(current_dir); it->isValid(); it->next())
        removeEmptyDirectoriesImpl(current_dir / it->name());

    if (disk->isDirectoryEmpty(current_dir))
        disk->removeDirectory(current_dir);
}

void BackupWriterDisk::copyFileFromDisk(
    const String & path_in_backup, DiskPtr src_disk, const String & src_path, bool copy_encrypted, UInt64 start_pos, UInt64 length)
{
    /// Use `IDisk::copyFile` as a more optimal way to copy a file if it's possible.
    /// However `IDisk::copyFile` can't use throttling for reading, and can't copy an encrypted file or copy a part of the file.
    bool has_throttling = src_disk->isRemote() ? static_cast<bool>(read_settings.remote_throttler) : static_cast<bool>(read_settings.local_throttler);
    if (!has_throttling && !start_pos && !copy_encrypted)
    {
        auto source_data_source_description = src_disk->getDataSourceDescription();
        if (source_data_source_description.sameKind(data_source_description) && !source_data_source_description.is_encrypted
            && (length == src_disk->getFileSize(src_path)))
        {
            /// Use more optimal way.
            LOG_TRACE(log, "Copying file {} from disk {} to disk {}", src_path, src_disk->getName(), disk->getName());
            auto dest_file_path = root_path / path_in_backup;
            disk->createDirectories(dest_file_path.parent_path());
            src_disk->copyFile(src_path, *disk, dest_file_path, read_settings, write_settings);
            return; /// copied!
        }
    }

    /// Fallback to copy through buffers.
    BackupWriterDefault::copyFileFromDisk(path_in_backup, src_disk, src_path, copy_encrypted, start_pos, length);
}

void BackupWriterDisk::copyFile(const String & destination, const String & source, size_t /*size*/)
{
    LOG_TRACE(log, "Copying file inside backup from {} to {} ", source, destination);
    auto dest_file_path = root_path / destination;
    auto src_file_path = root_path / source;
    disk->createDirectories(dest_file_path.parent_path());
    disk->copyFile(src_file_path, *disk, dest_file_path, read_settings, write_settings);
}

/// `getBlobPath` returns a disk-type-dependent representation, so a plain-local disk resolving a
/// path to anything but a single filesystem path breaks the assumption durability relies on. Fail
/// instead of skipping the fsync, which would report success without persisting anything.
static String getLocalBlobPath(const IDisk & disk, const fs::path & path)
{
    auto blob_path = disk.getBlobPath(path);
    if (blob_path.size() != 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected a single blob path for {} on local disk {}, got {}",
            path.string(), disk.getName(), blob_path.size());
    return blob_path[0];
}

/// The absolute path of the local file that has to be fsynced for `path` on this disk to be durable:
/// the file itself on a plain-local disk, its metadata file on an object-storage disk with local
/// metadata (the uploaded object is already durable), and nothing anywhere else.
std::optional<fs::path> BackupWriterDisk::getLocalPathToSync(const fs::path & path) const
{
    if (destination_is_plain_local_files)
        return getLocalBlobPath(*disk, path);
    if (local_metadata_root)
        return *local_metadata_root / getPathInInnermostDelegate(disk, path);
    return std::nullopt;
}

void BackupWriterDisk::syncFileToDisk(const String & file_name)
{
    auto file_path = root_path / file_name;
    auto local_path = getLocalPathToSync(file_path);
    if (!local_path)
        return;

    fsyncBackupFileContents(*local_path);

    /// The directories holding this file, so `syncDirectoriesToDisk` can persist their entries: one
    /// per component of the disk-relative path, which reaches the disk's own root. Taken from the
    /// resolved path, as a wrapper does not map the parent of `<prefix>/x` to the directory holding it.
    std::lock_guard lock{dirs_to_sync_mutex};
    auto local_dir = local_path->parent_path();
    for (auto dir = file_path; !dir.empty(); dir = dir.parent_path())
    {
        if (!dirs_to_sync.emplace(local_dir).second)
            break; /// this dir and all its ancestors are already recorded
        local_dir = local_dir.parent_path();
    }
}

void BackupWriterDisk::syncDirectoriesToDisk()
{
    std::set<fs::path> dirs;
    {
        std::lock_guard lock{dirs_to_sync_mutex};
        dirs = dirs_to_sync;
    }
    /// Sync deepest-first: a child directory entry is durable only once its parent is fsynced.
    for (auto it = dirs.rbegin(); it != dirs.rend(); ++it)
        fsyncBackupDirectory(*it);
}

}
