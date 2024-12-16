#include <Backups/BackupEntryFromAppendOnlyFile.h>
#include <Disks/IDisk.h>
#include <IO/LimitSeekableReadBuffer.h>
#include <IO/ReadBufferFromFileBase.h>


namespace DB
{

namespace
{
    /// For append-only files we must calculate its size on the construction of a backup entry.
    UInt64 calculateSize(const DiskPtr & disk, const String & file_path, bool copy_encrypted, std::optional<UInt64> unencrypted_file_size)
    {
        if (!unencrypted_file_size)
            return copy_encrypted ? disk->getEncryptedFileSize(file_path) : disk->getFileSize(file_path);
        if (copy_encrypted)
            return disk->getEncryptedFileSize(*unencrypted_file_size);
        return *unencrypted_file_size;
    }
}

BackupEntryFromAppendOnlyFile::BackupEntryFromAppendOnlyFile(
    const DiskPtr & disk_, const String & file_path_, bool copy_encrypted_, const std::optional<UInt64> & file_size_)
    : disk(disk_)
    , file_path(file_path_)
    , data_source_description(disk->getDataSourceDescription())
    , copy_encrypted(copy_encrypted_ && data_source_description.is_encrypted)
    , size(calculateSize(disk_, file_path_, copy_encrypted, file_size_))
{
}

BackupEntryFromAppendOnlyFile::~BackupEntryFromAppendOnlyFile() = default;

std::unique_ptr<SeekableReadBuffer> BackupEntryFromAppendOnlyFile::getReadBuffer(const ReadSettings & read_settings) const
{
    std::unique_ptr<SeekableReadBuffer> buf;
    if (copy_encrypted)
        buf = disk->readEncryptedFile(file_path, read_settings.adjustBufferSize(size));
    else
        buf = disk->readFile(file_path, read_settings.adjustBufferSize(size));
    return std::make_unique<LimitSeekableReadBuffer>(std::move(buf), 0, size);
}

}
