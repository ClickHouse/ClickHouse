#include <Backups/BackupEntryFromImmutableFile.h>
#include <Disks/IDisk.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Poco/File.h>


namespace DB
{

BackupEntryFromImmutableFile::BackupEntryFromImmutableFile(
    const String & file_path_,
    const std::optional<UInt64> & file_size_,
    const std::optional<UInt128> & checksum_,
    const std::shared_ptr<Poco::TemporaryFile> & temporary_file_)
    : file_path(file_path_), file_size(file_size_), checksum(checksum_), temporary_file(temporary_file_)
{
}

BackupEntryFromImmutableFile::BackupEntryFromImmutableFile(
    const DiskPtr & disk_,
    const String & file_path_,
    const std::optional<UInt64> & file_size_,
    const std::optional<UInt128> & checksum_,
    const std::shared_ptr<TemporaryFileOnDisk> & temporary_file_)
    : disk(disk_), file_path(file_path_), file_size(file_size_), checksum(checksum_), temporary_file_on_disk(temporary_file_)
{
}

BackupEntryFromImmutableFile::~BackupEntryFromImmutableFile() = default;

UInt64 BackupEntryFromImmutableFile::getSize() const
{
    std::lock_guard lock{get_file_size_mutex};
    if (!file_size)
        file_size = disk ? disk->getFileSize(file_path) : Poco::File(file_path).getSize();
    return *file_size;
}

std::unique_ptr<SeekableReadBuffer> BackupEntryFromImmutableFile::getReadBuffer() const
{
    if (disk)
        return disk->readFile(file_path);
    else
        return createReadBufferFromFileBase(file_path, /* settings= */ {});
}

}
