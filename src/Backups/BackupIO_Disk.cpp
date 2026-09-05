#include <Backups/BackupIO_Disk.h>
#include <Common/checkStackSize.h>
#include <Common/logger_useful.h>
#include <Disks/IDisk.h>
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
{
    if (data_source_description.object_storage_type == ObjectStorageType::Local)
    {
        /// The blobs are local files, but the backup is reached through the disk's own metadata,
        /// whose durability belongs to the metadata storage rather than to the backup.
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

void BackupWriterDisk::syncFileToDisk(const String & file_name)
{
    /// A completed upload to object storage is already durable, and only a plain-local destination
    /// can be made durable by fsyncing files: there `getBlobPath` resolves a disk-relative path to
    /// the absolute filesystem path of the file holding it.
    if (!destination_is_plain_local_files)
        return;

    auto file_path = root_path / file_name;
    fsyncBackupFileContents(getLocalBlobPath(*disk, file_path));

    /// Remember the disk-relative ancestor directories of this file (down to the disk root ""),
    /// so `syncDirectoriesToDisk` can persist their entries.
    std::lock_guard lock{dirs_to_sync_mutex};
    for (auto dir = file_path.parent_path(); ; dir = dir.parent_path())
    {
        if (!dirs_to_sync.emplace(dir).second)
            break; /// this dir and all its ancestors are already recorded
        if (dir.empty())
            break; /// reached the disk root
    }
}

void BackupWriterDisk::syncDirectoriesToDisk()
{
    if (!destination_is_plain_local_files)
        return;

    std::set<fs::path> dirs;
    {
        std::lock_guard lock{dirs_to_sync_mutex};
        dirs = dirs_to_sync;
    }
    if (dirs.empty())
        return;

    /// Sync deepest-first: a child directory entry is durable only once its parent is fsynced.
    /// `getBlobPath` resolves the disk-relative path (including the disk root "") to the
    /// absolute filesystem path for a local disk.
    for (auto it = dirs.rbegin(); it != dirs.rend(); ++it)
        fsyncBackupDirectory(getLocalBlobPath(*disk, *it));
}

}
