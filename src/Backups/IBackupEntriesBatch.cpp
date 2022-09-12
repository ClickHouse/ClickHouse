#include <Backups/IBackupEntriesBatch.h>
#include <IO/SeekableReadBuffer.h>


namespace DB
{

class IBackupEntriesBatch::BackupEntryFromBatch : public IBackupEntry
{
public:
    BackupEntryFromBatch(const std::shared_ptr<IBackupEntriesBatch> & generator_, size_t index_) : batch(generator_), index(index_)
    {
        assert(batch);
    }

    UInt64 getSize() const override { return batch->getSize(index); }
    std::optional<UInt128> getChecksum() const override { return batch->getChecksum(index); }
    std::unique_ptr<SeekableReadBuffer> getReadBuffer() const override { return batch->getReadBuffer(index); }

private:
    const std::shared_ptr<IBackupEntriesBatch> batch;
    const size_t index;
};


BackupEntries IBackupEntriesBatch::getBackupEntries()
{
    BackupEntries res;
    res.reserve(entry_names.size());
    for (size_t i = 0; i != entry_names.size(); ++i)
    {
        res.emplace_back(entry_names[i], std::make_unique<BackupEntryFromBatch>(shared_from_this(), i));
    }
    return res;
}

}
