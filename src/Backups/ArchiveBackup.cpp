#include <Backups/ArchiveBackup.h>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/Archives/IArchiveReader.h>
#include <IO/Archives/IArchiveWriter.h>
#include <IO/Archives/createArchiveReader.h>
#include <IO/Archives/createArchiveWriter.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


ArchiveBackup::ArchiveBackup(
    const String & backup_name_,
    const DiskPtr & disk_,
    const String & path_,
    const ContextPtr & context_,
    const std::optional<BackupInfo> & base_backup_info_)
    : BackupImpl(backup_name_, context_, base_backup_info_), disk(disk_), path(path_)
{
}

ArchiveBackup::~ArchiveBackup()
{
    close();
}

bool ArchiveBackup::backupExists() const
{
    return disk ? disk->exists(path) : fs::exists(path);
}

void ArchiveBackup::openImpl(OpenMode open_mode_)
{
    /// mutex is already locked
    if (open_mode_ == OpenMode::WRITE)
    {
        /// Create a directory to contain the archive.
        auto dir_path = fs::path(path).parent_path();
        if (disk)
            disk->createDirectories(dir_path);
        else
            std::filesystem::create_directories(dir_path);

        /// Start writing the archive.
        if (disk)
            writer = createArchiveWriter(path, disk->writeFile(path));
        else
            writer = createArchiveWriter(path);

        writer->setCompression(compression_method, compression_level);
        writer->setPassword(password);
    }
    else if (open_mode_ == OpenMode::READ)
    {
        if (disk)
        {
            auto archive_read_function = [d = disk, p = path]() -> std::unique_ptr<SeekableReadBuffer> { return d->readFile(p); };
            size_t archive_size = disk->getFileSize(path);
            reader = createArchiveReader(path, archive_read_function, archive_size);
        }
        else
            reader = createArchiveReader(path);

        reader->setPassword(password);
    }
}

void ArchiveBackup::closeImpl(const Strings &, bool writing_finalized_)
{
    /// mutex is already locked
    if (writer && writer->isWritingFile())
        throw Exception("There is some writing unfinished on close", ErrorCodes::LOGICAL_ERROR);

    writer.reset();
    reader.reset();

    if ((getOpenModeNoLock() == OpenMode::WRITE) && !writing_finalized_)
        fs::remove(path);
}

std::unique_ptr<ReadBuffer> ArchiveBackup::readFileImpl(const String & file_name) const
{
    /// mutex is already locked
    return reader->readFile(file_name);
}

std::unique_ptr<WriteBuffer> ArchiveBackup::writeFileImpl(const String & file_name)
{
    /// mutex is already locked
    return writer->writeFile(file_name);
}

void ArchiveBackup::setCompression(const String & compression_method_, int compression_level_)
{
    std::lock_guard lock{mutex};
    compression_method = compression_method_;
    compression_level = compression_level_;
    if (writer)
        writer->setCompression(compression_method, compression_level);
}

void ArchiveBackup::setPassword(const String & password_)
{
    std::lock_guard lock{mutex};
    password = password_;
    if (writer)
        writer->setPassword(password);
    if (reader)
        reader->setPassword(password);
}

}
