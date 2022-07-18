#include <Backups/DirectoryBackup.h>
#include <Disks/DiskLocal.h>


namespace DB
{

DirectoryBackup::DirectoryBackup(
    const String & backup_name_,
    const DiskPtr & disk_,
    const String & path_,
    const ContextPtr & context_,
    const std::optional<BackupInfo> & base_backup_info_)
    : BackupImpl(backup_name_, context_, base_backup_info_)
    , disk(disk_)
{
    /// Remove terminating slash.
    path = (std::filesystem::path(path_) / "").parent_path();

    /// If `disk` is not specified, we create an internal instance of `DiskLocal` here.
    if (!disk)
    {
        disk = std::make_shared<DiskLocal>(path, path, 0);
        path = ".";
    }
}


DirectoryBackup::~DirectoryBackup()
{
    close();
}

bool DirectoryBackup::backupExists() const
{
    return disk->isDirectory(path);
}

void DirectoryBackup::openImpl(OpenMode open_mode_)
{
    if (open_mode_ == OpenMode::WRITE)
        disk->createDirectories(path);
}

void DirectoryBackup::closeImpl(const Strings & written_files_, bool writing_finalized_)
{
    if ((getOpenModeNoLock() == OpenMode::WRITE) && !writing_finalized_ && !written_files_.empty())
    {
        /// Creating of the backup wasn't finished correctly,
        /// so the backup cannot be used and it's better to remove its files.
        const auto & files_to_delete = written_files_;
        for (const String & file_name : files_to_delete)
            disk->removeFileIfExists(path / file_name);
        if (disk->isDirectory(path) && disk->isDirectoryEmpty(path))
            disk->removeDirectory(path);
    }
}

std::unique_ptr<ReadBuffer> DirectoryBackup::readFileImpl(const String & file_name) const
{
    auto file_path = path / file_name;
    return disk->readFile(file_path);
}

std::unique_ptr<WriteBuffer> DirectoryBackup::writeFileImpl(const String & file_name)
{
    auto file_path = path / file_name;
    disk->createDirectories(fs::path(file_path).parent_path());
    return disk->writeFile(file_path);
}

}
