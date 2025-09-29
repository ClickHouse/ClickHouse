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
    void storeMetadata(bool force) override;

    bool tryLockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context, TIDHash * locked_by_id) override;
    void unlockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context) override;
    bool isRemovalTIDLocked() const override;
    TIDHash getRemovalTIDLock() const override { return removal_tid_lock; }

    bool hasStoredMetadata() const override;

    inline static constexpr auto TXN_VERSION_METADATA_FILE_NAME = "txn_version.txt";

protected:
    void setRemovalTIDLock(TIDHash removal_tid_hash) override;
    void appendCreationCSNToStoredMetadataImpl() override;
    void appendRemovalCSNToStoredMetadataImpl() override;
    void appendRemovalTIDToStoredMetadataImpl() override;
    Info readStoredMetadata(String & content) const override;

private:
    /// Hash of removal_tid, used to lock the object for removal
    std::atomic<TIDHash> removal_tid_lock = 0;
    const bool support_writing_with_append;
    const bool can_write_metadata;
    bool pending_store_metadata{false};
};

}
