#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage.h>

namespace DB
{

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

class Set;
using SetPtr = std::shared_ptr<Set>;


/** Common part of StorageSet and StorageJoin.
  */
class StorageSetOrJoinBase : public IStorage
{
    friend class SetOrJoinSink;

public:
    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context, bool async_insert) override;

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
    virtual void finishInsert(ContextPtr) = 0;
    virtual size_t getSize(ContextPtr context) const = 0;
};


/** Lets you save the set for later use on the right side of the IN statement.
  * When inserted into a table, the data will be inserted into the set,
  *  and also written to a file-backup, for recovery after a restart.
  * Reading from the table is not possible directly - it is possible to specify only the right part of the IN statement.
  */
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

    String getName() const override { return "Set"; }

    /// Access the insides.
    SetPtr getSet() const;

    void truncate(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr, TableExclusiveLockHolder &) override;

    /// Only delete is supported.
    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;
    void mutate(const MutationCommands & commands, ContextPtr context) override;

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool async_insert) override;

    std::optional<UInt64> totalRows(ContextPtr query_context) const override;
    std::optional<UInt64> totalBytes(ContextPtr query_context) const override;

    bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr) const override { return true; }

private:
    /// Allows to concurrently truncate the set and work (read/fill) the existing set.
    mutable std::mutex mutex;
    SetPtr set;

    void insertBlock(const Block & block, ContextPtr) override;
    void finishInsert(ContextPtr) override;
    size_t getSize(ContextPtr) const override;

    mutable RWLock rwlock = RWLockImpl::create();

    RWLockImpl::LockHolder tryLockTimedWithContext(const RWLock & lock, RWLockImpl::Type type, ContextPtr context) const;
    static RWLockImpl::LockHolder tryLockForCurrentQueryTimedWithContext(const RWLock & lock, RWLockImpl::Type type, ContextPtr context);
};

}
