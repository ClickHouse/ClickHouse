#pragma once
#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{
/**
* @brief VersionMetadataOnDisk is a subclass of VersionMetadata which persiss data on a disk along with the data part.
*/
class VersionMetadataOnDisk : public VersionMetadata
{
public:
    explicit VersionMetadataOnDisk(IMergeTreeDataPart * merge_tree_data_part_, bool support_writing_with_append_);

    void loadMetadata() override;
    void storeMetadata(bool force) const override;

    bool tryLockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context, TIDHash * locked_by_id) override;
    void unlockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context) override;
    bool isRemovalTIDLocked() const override;
    TIDHash getRemovalTIDLock() const override { return removal_tid_lock; }

    bool hasStoredMetadata() const override;

protected:
    void setRemovalTIDLock(TIDHash removal_tid_lock_hash) override;
    void storeCreationCSNToStoredMetadataImpl() override;
    void storeRemovalCSNToStoredMetadataImpl() override;
    void storeRemovalTIDToStoredMetadataImpl() override;
    Info readStoredMetadata(String & content) const override;

private:
    void storeMetadataHelper(std::function<void(WriteBuffer & buf)> write_func, bool sync);

    /// Hash of removal_tid, used to lock the object for removal
    std::atomic<TIDHash> removal_tid_lock = 0;
    /// If supported, try to append info to metadata instead of rewriting it.
    const bool support_writing_with_append;
    const bool can_write_metadata;
    /// If the object is not involved in transaction, delay the metadata storing if possible.
    mutable bool pending_store_metadata{false};
};

}
