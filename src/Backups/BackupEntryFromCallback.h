#pragma once

#include <Backups/IBackupEntry.h>


namespace DB
{

/// Represents small preloaded data to be included in a backup.
class BackupEntryFromCallback : public IBackupEntry
{
public:
    using ReadBufferCreator = std::function<std::unique_ptr<ReadBuffer>()>;

    /// The constructor is allowed to not set `checksum_`, in that case it will be calculated from the data.
    BackupEntryFromCallback(const ReadBufferCreator & callback_, size_t size_, const std::optional<UInt128> & checksum_ = {})
        : callback(callback_), size(size_), checksum(checksum_)
    {
    }

    UInt64 getSize() const override { return size; }
    std::optional<UInt128> getChecksum() const override { return checksum; }
    std::unique_ptr<ReadBuffer> getReadBuffer() const override { return callback(); }

private:
    const ReadBufferCreator callback;
    const size_t size;
    const std::optional<UInt128> checksum;
};

}
