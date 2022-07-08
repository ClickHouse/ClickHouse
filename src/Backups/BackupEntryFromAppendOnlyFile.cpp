#include <Backups/BackupEntryFromAppendOnlyFile.h>
#include <IO/LimitSeekableReadBuffer.h>


namespace DB
{

BackupEntryFromAppendOnlyFile::BackupEntryFromAppendOnlyFile(
    const String & file_path_,
    const std::optional<UInt64> & file_size_,
    const std::optional<UInt128> & checksum_,
    const std::shared_ptr<Poco::TemporaryFile> & temporary_file_)
    : BackupEntryFromImmutableFile(file_path_, file_size_, checksum_, temporary_file_)
    , limit(BackupEntryFromImmutableFile::getSize())
{
}

BackupEntryFromAppendOnlyFile::BackupEntryFromAppendOnlyFile(
    const DiskPtr & disk_,
    const String & file_path_,
    const std::optional<UInt64> & file_size_,
    const std::optional<UInt128> & checksum_,
    const std::shared_ptr<TemporaryFileOnDisk> & temporary_file_)
    : BackupEntryFromImmutableFile(disk_, file_path_, file_size_, checksum_, temporary_file_)
    , limit(BackupEntryFromImmutableFile::getSize())
{
}

std::unique_ptr<SeekableReadBuffer> BackupEntryFromAppendOnlyFile::getReadBuffer() const
{
    auto buf = BackupEntryFromImmutableFile::getReadBuffer();
    return std::make_unique<LimitSeekableReadBuffer>(std::move(buf), limit);
}

}
