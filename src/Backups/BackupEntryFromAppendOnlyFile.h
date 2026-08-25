#pragma once

#include <Backups/BackupEntryWithChecksumCalculation.h>


namespace DB
{

class TemporaryDataBuffer;
using TemporaryDataBufferPtr = std::unique_ptr<TemporaryDataBuffer>;

/// Represents a file prepared to be included in a backup, assuming that until this backup entry is destroyed
/// the file can be appended with new data, but the bytes which are already in the file won't be changed.
class BackupEntryFromAppendOnlyFile : public BackupEntryWithChecksumCalculation
{
public:
    /// The constructor is allowed to not set `file_size_`, in that case it will be calculated from the data.
    BackupEntryFromAppendOnlyFile(
        const DiskPtr & disk_,
        const String & file_path_,
        bool copy_encrypted_ = false,
        const std::optional<UInt64> & file_size_ = {},
        bool allow_checksum_from_remote_path_ = true);

    explicit BackupEntryFromAppendOnlyFile(TemporaryDataBufferPtr tmp_file_);

    ~BackupEntryFromAppendOnlyFile() override;

    std::unique_ptr<SeekableReadBuffer> getReadBuffer(const ReadSettings & read_settings) const override;
    UInt64 getSize() const override { return size; }

    DataSourceDescription getDataSourceDescription() const override { return data_source_description; }
    bool isEncryptedByDisk() const override { return copy_encrypted; }

    bool isFromFile() const override { return true; }
    DiskPtr getDisk() const override { return disk; }
    String getFilePath() const override { return file_path; }

protected:
    bool isPartialChecksumAllowed() const override { return true; }
    bool isChecksumFromRemotePathAllowed() const override { return allow_checksum_from_remote_path; }

private:
    TemporaryDataBufferPtr tmp_file;

    DiskPtr disk;
    String file_path;
    DataSourceDescription data_source_description;
    bool copy_encrypted = false;
    UInt64 size = 0;
    bool allow_checksum_from_remote_path = false;
};

}
