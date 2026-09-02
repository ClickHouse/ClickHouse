#pragma once

#include <Core/Types.h>
#include <memory>
#include <optional>
#include <vector>
#include <Disks/DiskType.h>
#include <Disks/IDisk.h>
#include <IO/SeekableReadBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

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
    virtual std::optional<UInt128> getPartialChecksum(UInt64 /* limit */, const ReadSettings &) const { return {}; }

    /// Returns a read buffer for reading the data.
    virtual std::unique_ptr<SeekableReadBuffer> getReadBuffer(const ReadSettings & read_settings) const = 0;

    /// Returns a raw read buffer suitable for reading an encrypted file header.
    virtual std::unique_ptr<SeekableReadBuffer> getReadBufferForEncryptionHeader(const ReadSettings & read_settings) const
    {
        return getReadBuffer(read_settings);
    }

    /// Returns true if the data returned by getReadBuffer() is encrypted by an encrypted disk.
    virtual bool isEncryptedByDisk() const { return false; }

    /// Returns information about disk and file if this backup entry is generated from a file.
    virtual bool isFromFile() const { return false; }
    virtual bool isFromImmutableFile() const { return false; }
    /// if it is a BackupEntryFromRemotePath, return true.
    virtual bool isFromRemoteFile() const { return false; }
    /// Returns true if this is a BackupEntryFromSnapshot (file referenced from a snapshot).
    virtual bool isFromSnapshot() const { return false; }
    /// Returns whether the current disk can be used to copy a snapshot entry natively.
    virtual bool isNativeCopyAllowed() const { return true; }
    /// Returns the remote path (object key) for snapshot (BackupEntryFromSnapshot) or remote file (BackupEntryFromRemotePath) entries.
    virtual String getRemotePath() const { return "invalid remote path"; }
    virtual String getEndpointURI() const { return "invalid endpoint"; }
    virtual String getNamespace() const { return "invalid namespace"; }
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
