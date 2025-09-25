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
    explicit VersionMetadataOnKeeper(
        IMergeTreeDataPart * merge_tree_data_part_, GetZooKeeperFunc get_zk_func_, String metadata_path_, String lock_path_);

    void loadMetadata() override;
    void storeMetadata(bool force) override;

    bool tryLockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context, TIDHash * locked_by_id) override;
    void unlockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context) override;
    bool isRemovalTIDLocked() const override;
    TIDHash getRemovalTIDLock() const override;

    bool hasStoredMetadata() const override;

protected:
    void setRemovalTIDLock(TIDHash removal_tid_hash) override;
    void appendCreationCSNToStoredMetadataImpl() override;
    void appendRemovalCSNToStoredMetadataImpl() override;
    void appendRemovalTIDToStoredMetadataImpl(const TransactionID & tid) override;
    Info readStoredMetadata(String & content) const override;

private:
    GetZooKeeperFunc get_zk_func{nullptr};
    String metadata_path;
    std::optional<Int32> metadata_version;
    String lock_path;
};

}
