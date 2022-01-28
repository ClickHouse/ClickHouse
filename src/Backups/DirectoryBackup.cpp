#include <Backups/DirectoryBackup.h>
#include <Common/quoteString.h>
#include <Disks/IDisk.h>
#include <Disks/DiskLocal.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


DirectoryBackup::DirectoryBackup(
    const String & backup_name_,
    const DiskPtr & disk_,
    const String & path_,
    const ContextPtr & context_,
    const std::optional<BackupInfo> & base_backup_info_)
    : BackupImpl(backup_name_, context_, base_backup_info_)
    , disk(disk_), path(path_)
{
    /// Path to backup must end with '/'
    if (!path.ends_with("/"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Backup {}: Path to backup must end with '/', but {} doesn't.", getName(), quoteString(path));
    dir_path = fs::path(path).parent_path(); /// get path without terminating slash

    /// If `disk` is not specified, we create an internal instance of `DiskLocal` here.
    if (!disk)
    {
        auto fspath = fs::path{dir_path};
        if (!fspath.has_filename())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Backup {}: Path to a backup must be a directory path.", getName(), quoteString(path));
        path = fspath.filename() / "";
        dir_path = fs::path(path).parent_path(); /// get path without terminating slash
        String disk_path = fspath.remove_filename();
        disk = std::make_shared<DiskLocal>(disk_path, disk_path, 0);
    }
}


DirectoryBackup::~DirectoryBackup()
{
    close();
}

bool DirectoryBackup::backupExists() const
{
    return disk->isDirectory(dir_path);
}

void DirectoryBackup::openImpl(OpenMode open_mode_)
{
    if (open_mode_ == OpenMode::WRITE)
        disk->createDirectories(dir_path);
}

void DirectoryBackup::closeImpl(bool writing_finalized_)
{
    if ((getOpenModeNoLock() == OpenMode::WRITE) && !writing_finalized_ && disk->isDirectory(dir_path))
    {
        /// Creating of the backup wasn't finished correctly,
        /// so the backup cannot be used and it's better to remove its files.
        disk->removeRecursive(dir_path);
    }
}

std::unique_ptr<ReadBuffer> DirectoryBackup::readFileImpl(const String & file_name) const
{
    String file_path = path + file_name;
    return disk->readFile(file_path);
}

std::unique_ptr<WriteBuffer> DirectoryBackup::addFileImpl(const String & file_name)
{
    String file_path = path + file_name;
    disk->createDirectories(fs::path(file_path).parent_path());
    return disk->writeFile(file_path);
}

}
