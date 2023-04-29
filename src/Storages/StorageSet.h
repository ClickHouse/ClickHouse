#pragma once

#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Storages/SetSettings.h>
#include <Interpreters/Set.h>
#include <Interpreters/ProbSet.h>
#include <Disks/IDisk.h>

namespace DB
{

// class Set;
using SetPtr_ = std::shared_ptr<Set>;

// class ProbSet;
using ProbSetPtr_ = std::shared_ptr<ProbSet>;


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
{
    set = std::make_shared<Set>(SizeLimits(), false, true);

    // if constexpr (!is_prob) {
    //     set = std::make_shared<Set>(SizeLimits(), false, true);
    // } else {
    //     probSet = std::make_shared<ProbSet>(SizeLimits(), false, true);
    // }
    Block header = getInMemoryMetadataPtr()->getSampleBlock();

     if constexpr (!is_prob) {
        set->setHeader(header.getColumnsWithTypeAndName());
    } else {
        set->setHeader(header.getColumnsWithTypeAndName(), true);
    }
   

    restore();
}

template <bool is_prob>
void StorageSet<is_prob>::insertBlock(const Block & block, ContextPtr) { 
    set->insertFromBlock(block.getColumnsWithTypeAndName()); 
    // if constexpr (!is_prob) {
    //     set->insertFromBlock(block.getColumnsWithTypeAndName()); 
    // } else {
    //     probSet->insertFromBlock(block.getColumnsWithTypeAndName()); 
    // }
}

template <bool is_prob>
void StorageSet<is_prob>::finishInsert() { 
    set->finishInsert(); 
    // if constexpr (!is_prob) {
    //     set->finishInsert(); 
    // } else {
    //     probSet->finishInsert(); 
    // }
}

template <bool is_prob>
size_t StorageSet<is_prob>::getSize(ContextPtr) const { 
    return set->getTotalRowCount(); 
    // if constexpr (!is_prob) {
    //     return set->getTotalRowCount(); 
    // } else {
    //     return probSet->getTotalRowCount(); 
    // }

}

template <bool is_prob>
std::optional<UInt64> StorageSet<is_prob>::totalRows(const Settings &) const {
    return set->getTotalRowCount(); 
    // if constexpr (!is_prob) {
    //     return set->getTotalRowCount(); 
    // } else {
    //     return probSet->getTotalRowCount(); 
    // } 
}


template <bool is_prob>
std::optional<UInt64> StorageSet<is_prob>::totalBytes(const Settings &) const { 
    return set->getTotalByteCount();  
    // if constexpr (!is_prob) {
    //     return set->getTotalByteCount();  
    // } else {
    //     return probSet->getTotalByteCount();  
    // } 
}


template <bool is_prob>
void StorageSet<is_prob>::truncate(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr, TableExclusiveLockHolder &)
{
    disk->removeRecursive(path);
    disk->createDirectories(path);
    disk->createDirectories(fs::path(path) / "tmp/");

    Block header = metadata_snapshot->getSampleBlock();

    increment = 0;

    set = std::make_shared<Set>(SizeLimits(), false, true);
    if constexpr (!is_prob) {
        //set = std::make_shared<Set>(SizeLimits(), false, true);
        set->setHeader(header.getColumnsWithTypeAndName());
    } else {
        //probSet = std::make_shared<ProbSet>(SizeLimits(), false, true);
        set->setHeader(header.getColumnsWithTypeAndName(), true);
    } 
    
}

}


