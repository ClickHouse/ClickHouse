#include <Backups/IBackupEntriesLazyBatch.h>
#include <Common/Exception.h>
#include <IO/SeekableReadBuffer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


class IBackupEntriesLazyBatch::BackupEntryFromBatch : public IBackupEntry
{
public:
    BackupEntryFromBatch(const std::shared_ptr<IBackupEntriesLazyBatch> & batch_, size_t index_) : batch(batch_), index(index_) { }

    UInt64 getSize() const override { return getInternalBackupEntry()->getSize(); }
    std::optional<UInt128> getChecksum() const override { return getInternalBackupEntry()->getChecksum(); }
    std::unique_ptr<SeekableReadBuffer> getReadBuffer() const override { return getInternalBackupEntry()->getReadBuffer(); }

private:
    BackupEntryPtr getInternalBackupEntry() const
    {
        std::lock_guard lock{mutex};
        if (!entry)
        {
            batch->generateIfNecessary();
            entry = batch->entries[index].second;
        }
        return entry;
    }

    const std::shared_ptr<IBackupEntriesLazyBatch> batch;
    const size_t index;
    mutable std::mutex mutex;
    mutable BackupEntryPtr entry;
};


BackupEntries IBackupEntriesLazyBatch::getBackupEntries()
{
    BackupEntries res;
    size_t size = getSize();
    res.reserve(size);
    for (size_t i = 0; i != size; ++i)
    {
        res.emplace_back(getName(i), std::make_unique<BackupEntryFromBatch>(shared_from_this(), i));
    }
    return res;
}

void IBackupEntriesLazyBatch::generateIfNecessary()
{
    std::lock_guard lock{mutex};
    if (generated)
        return;

    entries = generate();

    if (entries.size() != getSize())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Backup entries were generated incorrectly");

    for (size_t i = 0; i != entries.size(); ++i)
    {
        if (entries[i].first != getName(i))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Backup entries were generated incorrectly");
    }

    generated = true;
}

IBackupEntriesLazyBatch::~IBackupEntriesLazyBatch() = default;

}
