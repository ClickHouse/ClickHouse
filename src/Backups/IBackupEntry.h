#pragma once

#include <Core/Types.h>
#include <memory>
#include <optional>
#include <vector>

namespace DB
{
class SeekableReadBuffer;

/// A backup entry represents some data which should be written to the backup or has been read from the backup.
class IBackupEntry
{
public:
    virtual ~IBackupEntry() = default;

    /// Returns the size of the data.
    virtual UInt64 getSize() const = 0;

    /// Returns the checksum of the data if it's precalculated.
    /// Can return nullopt which means the checksum should be calculated from the read buffer.
    virtual std::optional<UInt128> getChecksum() const { return {}; }

    /// Returns a read buffer for reading the data.
    virtual std::unique_ptr<SeekableReadBuffer> getReadBuffer() const = 0;
};

using BackupEntryPtr = std::shared_ptr<const IBackupEntry>;
using BackupEntries = std::vector<std::pair<String, BackupEntryPtr>>;

}
