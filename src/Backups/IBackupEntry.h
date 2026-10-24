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
    [[nodiscard]] virtual UInt64 getSize() const = 0;

    /// Returns the checksum of the data.
    [[nodiscard]] virtual UInt128 getChecksum(const ReadSettings & read_settings) const = 0;

    /// Returns a partial checksum, i.e. the checksum calculated for a prefix part of the data.
    /// Can return nullopt if the partial checksum is too difficult to calculate.
    [[nodiscard]] virtual std::optional<UInt128> getPartialChecksum(UInt64 /* limit */, const ReadSettings &) const { return {}; }

    /// Returns a read buffer for reading the data.
    [[nodiscard]] virtual std::unique_ptr<SeekableReadBuffer> getReadBuffer(const ReadSettings & read_settings) const = 0;

    /// Returns a raw read buffer suitable for reading an encrypted file header.
    [[nodiscard]] virtual std::unique_ptr<SeekableReadBuffer> getReadBufferForEncryptionHeader(const ReadSettings & read_settings) const
    {
        return getReadBuffer(read_settings);
    }

    /// Returns true if the data returned by getReadBuffer() is encrypted by an encrypted disk.
    [[nodiscard]] virtual bool isEncryptedByDisk() const { return false; }

    /// Returns information about disk and file if this backup entry is generated from a file.
    [[nodiscard]] virtual bool isFromFile() const { return false; }
    [[nodiscard]] virtual bool isFromImmutableFile() const { return false; }
    /// if it is a BackupEntryFromRemotePath, return true.
    [[nodiscard]] virtual bool isFromRemoteFile() const { return false; }
    /// Returns true if this is a BackupEntryFromSnapshot (file referenced from a snapshot).
    [[nodiscard]] virtual bool isFromSnapshot() const { return false; }
    /// Returns whether the current disk can be used to copy a snapshot entry natively.
    [[nodiscard]] virtual bool isNativeCopyAllowed() const { return true; }
    /// Returns the remote path (object key) for snapshot (BackupEntryFromSnapshot) or remote file (BackupEntryFromRemotePath) entries.
    [[nodiscard]] virtual String getRemotePath() const { return "invalid remote path"; }
    [[nodiscard]] virtual String getEndpointURI() const { return "invalid endpoint"; }
    [[nodiscard]] virtual String getNamespace() const { return "invalid namespace"; }
    [[nodiscard]] virtual String getFilePath() const { return ""; }
    [[nodiscard]] virtual DiskPtr getDisk() const { return nullptr; }

    [[nodiscard]] virtual bool isReference() const { return false; }
    [[nodiscard]] virtual String getReferenceTarget() const
    {
        throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "getReferenceTarget not implemented for the backup entry");
    }

    [[nodiscard]] virtual DataSourceDescription getDataSourceDescription() const = 0;
};

using BackupEntryPtr = std::shared_ptr<const IBackupEntry>;
using BackupEntries = std::vector<std::pair<String, BackupEntryPtr>>;

}
