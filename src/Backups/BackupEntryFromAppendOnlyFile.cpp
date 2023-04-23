#include <Backups/BackupEntryFromAppendOnlyFile.h>
#include <IO/LimitSeekableReadBuffer.h>


namespace DB
{

BackupEntryFromAppendOnlyFile::BackupEntryFromAppendOnlyFile(
    const DiskPtr & disk_,
    const String & file_path_,
    const ReadSettings & settings_,
    const std::optional<UInt64> & file_size_,
    const std::optional<UInt128> & checksum_)
    : BackupEntryFromImmutableFile(disk_, file_path_, settings_, file_size_, checksum_)
    , limit(BackupEntryFromImmutableFile::getSize())
{
}

std::unique_ptr<SeekableReadBuffer> BackupEntryFromAppendOnlyFile::getReadBuffer() const
{
    auto buf = BackupEntryFromImmutableFile::getReadBuffer();
    return std::make_unique<LimitSeekableReadBuffer>(std::move(buf), 0, limit);
}

}
