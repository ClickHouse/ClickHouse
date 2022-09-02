#pragma once

#include <Backups/IBackupEntry.h>
#include <IO/ReadBufferFromString.h>


namespace DB
{

/// Represents small preloaded data to be included in a backup.
class BackupEntryFromMemory : public IBackupEntry
{
public:
    /// The constructor is allowed to not set `checksum_`, in that case it will be calculated from the data.
    BackupEntryFromMemory(const void * data_, size_t size_, const std::optional<UInt128> & checksum_ = {});
    explicit BackupEntryFromMemory(String data_, const std::optional<UInt128> & checksum_ = {});

    UInt64 getSize() const override { return data.size(); }
    std::optional<UInt128> getChecksum() const override { return checksum; }
    std::unique_ptr<ReadBuffer> getReadBuffer() const override;

private:
    const String data;
    const std::optional<UInt128> checksum;
};

}
