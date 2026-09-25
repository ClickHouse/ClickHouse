#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/StorageWithCommonVirtualColumns.h>
#include <base/defines.h>

#include <chrono>
#include <condition_variable>
#include <mutex>


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
      * replacement is written into `mutation_data_file_name`, and `mutation_commit_file_name`,
      * holding the number the replacement is going to be persisted under, marks the window in which
      * the files the mutation replaces are being removed and the replacement is not in place yet. The
      * marker is put in place by renaming `mutation_commit_tmp_file_name`, so it either exists with
      * its whole content or not at all. See `completeMutation`.
      */
    static constexpr auto mutation_data_file_name = "tmp/mut.bin";
    static constexpr auto mutation_commit_file_name = "tmp/mut.commit";
    static constexpr auto mutation_commit_tmp_file_name = "tmp/mut.commit.tmp";

    /** Creates the marker that commits a mutation whose replacement is staged in
      * `mutation_data_file_name` and is going to be persisted under `mutation_id`. From this point the
      * mutation is durable: `completeMutation` finishes the swap, whether it is called by the mutation
      * itself or by the load after an interruption.
      */
    void commitMutation(UInt64 mutation_id);

    /** A mutation replaces every persisted file with one that holds the rows it kept, which cannot be
      * done in one step: a crash in between would leave the table with whichever of the old files
      * happened to survive - rows the mutation never even matched would be gone - and the
      * replacement, staged outside the directory the load reads, would be ignored. This is the swap
      * itself, and it is idempotent, so it is also how the load finishes a swap that was interrupted:
      * the files numbered below `mutation_id` are removed, the replacement is put in place as
      * `<mutation_id>.bin` when it is still staged, and the marker is cleared. Files numbered above
      * `mutation_id` are inserts made after the mutation was committed and are kept. If the replacement
      * is neither staged nor in place, nothing is touched and an exception is thrown.
      */
    void completeMutation(UInt64 mutation_id);

    /** Finishes the swap of a committed mutation the marker of which is found, at load and before a new
      * mutation stages its own replacement in the same file. See `completeMutation`.
      */
    void finishInterruptedMutation();

    /** Inserts that were started before a mutation may still be running: `write` only assigns them a
      * file, and the sink inserts the rows and publishes the file later, on its own. A mutation must
      * not take its snapshot of the rows or touch the files while such a sink is alive, or the rows
      * it inserts are missing from the snapshot, or its file is published next to the replacement
      * and loaded twice. The sinks are counted from creation to destruction, and the mutation waits
      * for the count to drop to zero while it holds the lock that stops new sinks from being created.
      */
    void waitForOutstandingSinks(std::chrono::milliseconds timeout);

    std::mutex outstanding_sinks_mutex;
    std::condition_variable outstanding_sinks_changed;
    size_t outstanding_sinks TSA_GUARDED_BY(outstanding_sinks_mutex) = 0;

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

    /// `read` is not supported, but the number of rows is known exactly, so a bare `count` can still
    /// be answered from `totalRows` instead of failing. `StorageJoin` does the same.
    bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr) const override { return true; }

private:
    /// Allows to concurrently truncate the set and work (read/fill) the existing set.
    mutable std::mutex mutex;
    SetPtr set TSA_GUARDED_BY(mutex);

    void insertBlock(const Block & block, ContextPtr) override;
    void finishInsert() override;
    size_t getSize(ContextPtr) const override;
};

}
