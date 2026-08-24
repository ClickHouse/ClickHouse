#pragma once

#include <Backups/IBackupEntry.h>
#include <mutex>

namespace DB
{

/// Helper class designed to generate multiple backup entries from one source.
class IBackupEntriesLazyBatch : public std::enable_shared_from_this<IBackupEntriesLazyBatch>
{
public:
    BackupEntries getBackupEntries();
    virtual ~IBackupEntriesLazyBatch();

protected:
    virtual size_t getSize() const = 0;
    virtual const String & getName(size_t i) const = 0;
    virtual BackupEntries generate() = 0;

private:
    void generateIfNecessary();

    class BackupEntryFromBatch;
    std::mutex mutex;
    BackupEntries entries;
    bool generated = false;
};

}
