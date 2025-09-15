#pragma once
#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{
/**
* @brief VersionMetadataOnDisk is a subclass of VersionMetadata which persists data on a disk along with the data part.
*/
class VersionMetadataOnDisk : public VersionMetadata
{
public:
    explicit VersionMetadataOnDisk(IMergeTreeDataPart * merge_tree_data_part_, bool support_writing_with_append_);

    void loadMetadata() override;
    void storeMetadata(bool force) override;

    bool tryLockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context, TIDHash * locked_by_id) override;
    void unlockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context) override;
    bool isRemovalTIDLocked() override;
    TIDHash getRemovalTIDLock() override { return removal_tid_lock; }

    bool hasStoredMetadata() const override;

    void removeStoredMetadata() override;

protected:
    void storeCreationCSNToStoredMetadataImpl() override;
    void storeRemovalCSNToStoredMetadataImpl() override;
    void storeRemovalTIDToStoredMetadataImpl() override;
    VersionInfo readStoredMetadata(String & content) override;

private:
    void storeMetadataHelper(std::function<void(WriteBuffer & buf)> write_func, bool sync);

    /// If supported, try to append info to metadata instead of rewriting it.
    const bool support_writing_with_append;
    const bool can_write_metadata;
    /// If the object is not involved in transaction, delay the metadata storing if possible.
    mutable bool pending_store_metadata{false};
};

}
