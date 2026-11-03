#include <memory>
#include <Backups/BackupEntryFromAppendOnlyFile.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Disks/IDisk.h>
#include <IO/LimitSeekableReadBuffer.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/SeekableReadBuffer.h>


namespace DB
{

namespace
{
    /// For append-only files we must calculate its size on the construction of a backup entry.
    UInt64 calculateSize(const DiskPtr & disk, const String & file_path, bool copy_encrypted, std::optional<UInt64> passed_file_size)
    {
        if (copy_encrypted)
            return passed_file_size ? disk->getEncryptedFileSize(*passed_file_size) : disk->getEncryptedFileSize(file_path);

        if (passed_file_size)
            return *passed_file_size;

        return disk->getFileSize(file_path);
    }
}

BackupEntryFromAppendOnlyFile::BackupEntryFromAppendOnlyFile(
    const DiskPtr & disk_,
    const String & file_path_,
    bool copy_encrypted_,
    const std::optional<UInt64> & file_size_,
    bool allow_checksum_from_remote_path_)
    : disk(disk_)
    , file_path(file_path_)
    , data_source_description(disk->getDataSourceDescription())
    , copy_encrypted(copy_encrypted_ && data_source_description.is_encrypted)
    , size(calculateSize(disk_, file_path_, copy_encrypted, file_size_))
    , allow_checksum_from_remote_path(allow_checksum_from_remote_path_)
{
}

BackupEntryFromAppendOnlyFile::BackupEntryFromAppendOnlyFile(TemporaryDataBufferPtr tmp_file_)
    : tmp_file(std::move(tmp_file_))
    , file_path(tmp_file->describeFilePath())
    , size(tmp_file->getStat().compressed_size)
{
}

BackupEntryFromAppendOnlyFile::~BackupEntryFromAppendOnlyFile() = default;

std::unique_ptr<SeekableReadBuffer> BackupEntryFromAppendOnlyFile::getReadBuffer(const ReadSettings & read_settings) const
{
    std::unique_ptr<SeekableReadBuffer> buf;
    if (tmp_file)
        buf = tmp_file->readRaw();
    else if (copy_encrypted)
        buf = disk->readEncryptedFile(file_path, read_settings.adjustBufferSize(size));
    else
        buf = disk->readFile(file_path, read_settings.adjustBufferSize(size));
    return std::make_unique<LimitSeekableReadBuffer>(std::move(buf), 0, size);
}

}
