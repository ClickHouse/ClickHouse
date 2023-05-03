#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage.h>
#include <Storages/SetSettings.h>
#include <Interpreters/Set.h>
#include <Disks/IDisk.h>
#include <QueryPipeline/ProfileInfo.h>
#include <Common/logger_useful.h>

namespace DB
{


// class Set;
using SetPtr_ = std::shared_ptr<Set>;

// class ProbSet;
//using ProbSetPtr_ = std::shared_ptr<ProbSet>;


/** Common part of StorageSet and StorageJoin.
  */
class StorageSetOrJoinBase : public IStorage
{
    friend class SetOrJoinSink;

public:
    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

    bool storesDataOnDisk() const override { return true; }
    Strings getDataPaths() const override { return {path}; }

protected:
    StorageSetOrJoinBase(
        DiskPtr disk_,
        const String & relative_path_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        bool persistent_);

    DiskPtr disk;
    String path;
    bool persistent;

    std::atomic<UInt64> increment = 0;    /// For the backup file names.

    /// Restore from backup.
    void restore();

private:
    void restoreFromFile(const String & file_path);

    /// Insert the block into the state.
    virtual void insertBlock(const Block & block, ContextPtr context) = 0;
    /// Call after all blocks were inserted.
    virtual void finishInsert() = 0;
    virtual size_t getSize(ContextPtr context) const = 0;
};


/** Lets you save the set for later use on the right side of the IN statement.
  * When inserted into a table, the data will be inserted into the set,
  *  and also written to a file-backup, for recovery after a restart.
  * Reading from the table is not possible directly - it is possible to specify only the right part of the IN statement.
  */
template <bool is_prob>
class StorageSet final : public StorageSetOrJoinBase
{
public:
    StorageSet(
        DiskPtr disk_,
        const String & relative_path_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        bool persistent_);

    String getName() const override {
        if constexpr (is_prob) {
            return "ProbSet";
        } else {
            return "Set"; 
        }
    }

    /// Access the insides.
    SetPtr & getSet() { return set; }

    void truncate(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr, TableExclusiveLockHolder &) override;

    std::optional<UInt64> totalRows(const Settings & settings) const override;
    std::optional<UInt64> totalBytes(const Settings & settings) const override;

private:
    SetPtr_ set;
    //ProbSetPtr_ probSet;

    void insertBlock(const Block & block, ContextPtr) override;
    void finishInsert() override;
    size_t getSize(ContextPtr) const override;
};


template <bool is_prob>
StorageSet<is_prob>::StorageSet(
    DiskPtr disk_,
    const String & relative_path_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    bool persistent_)
    : StorageSetOrJoinBase{disk_, relative_path_, table_id_, columns_, constraints_, comment, persistent_}
    , set(std::make_shared<Set>(SizeLimits(), false, true))
{
    Block header = getInMemoryMetadataPtr()->getSampleBlock();
    set->setHeader(header.getColumnsWithTypeAndName(), is_prob);

    restore();
}


template <bool is_prob>
void StorageSet<is_prob>::insertBlock(const Block & block, ContextPtr) { set->insertFromBlock(block.getColumnsWithTypeAndName()); }

template <bool is_prob>
void StorageSet<is_prob>::finishInsert() { set->finishInsert(); }

template <bool is_prob>
size_t StorageSet<is_prob>::getSize(ContextPtr) const { return set->getTotalRowCount(); }

template <bool is_prob>
std::optional<UInt64> StorageSet<is_prob>::totalRows(const Settings &) const { return set->getTotalRowCount(); }


template <bool is_prob>
std::optional<UInt64> StorageSet<is_prob>::totalBytes(const Settings &) const { return set->getTotalByteCount(); }


template <bool is_prob>
void StorageSet<is_prob>::truncate(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr, TableExclusiveLockHolder &)
{
    if (disk->exists(path))
        disk->removeRecursive(path);
    else
        LOG_INFO(&Poco::Logger::get("StorageSet"), "Path {} is already removed from disk {}", path, disk->getName());

    disk->createDirectories(path);
    disk->createDirectories(fs::path(path) / "tmp/");

    Block header = metadata_snapshot->getSampleBlock();

    increment = 0;
    set = std::make_shared<Set>(SizeLimits(), false, true);
    set->setHeader(header.getColumnsWithTypeAndName(), is_prob);
}


}


