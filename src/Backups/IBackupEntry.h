#pragma once

#include <Core/Types.h>
#include <memory>
#include <optional>
#include <vector>
#include <Disks/DiskType.h>
#include <Disks/IDisk.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class SeekableReadBuffer;

/// A backup entry represents some data which should be written to the backup or has been read from the backup.
class IBackupEntry
{
public:
    virtual ~IBackupEntry() = default;

    /// Returns the size of the data.
    virtual UInt64 getSize() const = 0;

    /// Returns the checksum of the data.
    virtual UInt128 getChecksum(const ReadSettings & read_settings) const = 0;

    /// Returns a partial checksum, i.e. the checksum calculated for a prefix part of the data.
    /// Can return nullopt if the partial checksum is too difficult to calculate.
    virtual std::optional<UInt128> getPartialChecksum(size_t /* prefix_length */, const ReadSettings &) const { return {}; }

    /// Returns a read buffer for reading the data.
    virtual std::unique_ptr<SeekableReadBuffer> getReadBuffer(const ReadSettings & read_settings) const = 0;

    /// Returns true if the data returned by getReadBuffer() is encrypted by an encrypted disk.
    virtual bool isEncryptedByDisk() const { return false; }

    /// Returns information about disk and file if this backup entry is generated from a file.
    virtual bool isFromFile() const { return false; }
    virtual bool isFromImmutableFile() const { return false; }
    virtual String getFilePath() const { return ""; }
    virtual DiskPtr getDisk() const { return nullptr; }

    virtual bool isReference() const { return false; }
    virtual String getReferenceTarget() const
    {
        throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "getReferenceTarget not implemented for the backup entry");
    }

    virtual DataSourceDescription getDataSourceDescription() const = 0;
};

using BackupEntryPtr = std::shared_ptr<const IBackupEntry>;
using BackupEntries = std::vector<std::pair<String, BackupEntryPtr>>;

}
