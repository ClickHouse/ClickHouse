#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/StorageWithCommonVirtualColumns.h>


namespace DB
{

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

class Set;
using SetPtr = std::shared_ptr<Set>;


/** Common part of StorageSet and StorageJoin.
  */
class StorageSetOrJoinBase : public StorageWithCommonVirtualColumns
{
    friend class SetOrJoinSink;

public:
    static VirtualColumnsDescription createVirtuals();

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

    /** The names, relative to `path`, of the files the swap of a mutation goes through: the
      * replacement is written into `mutation_data_file_name`, and `mutation_commit_file_name` marks
      * the window in which the old files are being removed and the replacement is not in place yet.
      * See `finishInterruptedMutation`.
      */
    static constexpr auto mutation_data_file_name = "tmp/mut.bin";
    static constexpr auto mutation_commit_file_name = "tmp/mut.commit";

    /** A mutation replaces every persisted file with one that holds the rows it kept, which cannot be
      * done in one step: a crash in between would leave the table with whichever of the old files
      * happened to survive - rows the mutation never even matched would be gone - and the
      * replacement, staged outside the directory the load reads, would be ignored. The mutation
      * therefore marks that window, and this finishes what it was doing before anything is loaded:
      * the replacement is put in place when it is still staged, and the marker is cleared once it is.
      */
    void finishInterruptedMutation();

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

    std::optional<UInt64> totalRows(ContextPtr query_context) const override;
    std::optional<UInt64> totalBytes(ContextPtr query_context) const override;

private:
    /// Allows to concurrently truncate the set and work (read/fill) the existing set.
    mutable std::mutex mutex;
    SetPtr set TSA_GUARDED_BY(mutex);

    void insertBlock(const Block & block, ContextPtr) override;
    void finishInsert() override;
    size_t getSize(ContextPtr) const override;
};

}
