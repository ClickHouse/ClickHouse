#pragma once

#include <Backups/IBackupEntry.h>
#include <mutex>

namespace DB
{

/// Helper class designed to generate multiple backup entries from one source.
class IBackupEntriesBatch : public std::enable_shared_from_this<IBackupEntriesBatch>
{
public:
    BackupEntries getBackupEntries();

    virtual ~IBackupEntriesBatch() = default;

protected:
    IBackupEntriesBatch(const Strings & entry_names_) : entry_names(entry_names_) {}

    virtual std::unique_ptr<SeekableReadBuffer> getReadBuffer(size_t index) = 0;
    virtual UInt64 getSize(size_t index) = 0;
    virtual std::optional<UInt128> getChecksum(size_t) { return {}; }

private:
    class BackupEntryFromBatch;
    const Strings entry_names;
};

}
