#pragma once
#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{

using GetZooKeeperFunc = std::function<zkutil::ZooKeeperPtr()>;

/**
* @brief VersionMetadataOnKeeper is a subclass of VersionMetadata which persists data on Keeper.
*/
class VersionMetadataOnKeeper : public VersionMetadata
{
public:
    VersionMetadataOnKeeper(
        IMergeTreeDataPart * merge_tree_data_part_, GetZooKeeperFunc get_zk_func_, String metadata_path_, String lock_path_);
    VersionMetadataOnKeeper(IMergeTreeDataPart * merge_tree_data_part_, GetZooKeeperFunc get_zk_func_);
    void loadMetadata() override;
    void storeMetadata(bool force) const override;

    bool tryLockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context, TIDHash * locked_by_id) override;
    void unlockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context) override;
    bool isRemovalTIDLocked() const override;
    TIDHash getRemovalTIDLock() const override;

    bool hasStoredMetadata() const override;

protected:
    void setRemovalTIDLock(TIDHash removal_tid_lock_hash) override;
    void storeCreationCSNToStoredMetadataImpl() override;
    void storeRemovalCSNToStoredMetadataImpl() override;
    void storeRemovalTIDToStoredMetadataImpl() override;
    Info readStoredMetadata(String & content) const override;

private:
    String txn_keeper_node;
    GetZooKeeperFunc get_zk_func{nullptr};
    String metadata_path;
    mutable std::optional<Int32> metadata_version;
    String lock_path;
    /// If the object is not involved in transaction, delay the metadata storing if possible.
    mutable bool pending_store_metadata{false};
};

}
