#pragma once

#include <atomic>
#include <optional>
#include <mutex>

#include <base/shared_ptr_helper.h>

#include <Core/NamesAndTypes.h>
#include <Storages/IStorage.h>

#include <Common/MultiVersion.h>

namespace DB
{

/** Implements storage in the RAM.
  * Suitable for temporary data.
  * It does not support keys.
  * Data is stored as a set of blocks and is not stored anywhere else.
  */
class StorageMemory final : public shared_ptr_helper<StorageMemory>, public IStorage
{
friend class MemorySink;
friend class MemoryRestoreTask;
friend struct shared_ptr_helper<StorageMemory>;

public:
    String getName() const override { return "Memory"; }

    size_t getSize() const { return data.get()->size(); }

    /// Snapshot for StorageMemory contains current set of blocks
    /// at the moment of the start of query.
    struct SnapshotData : public StorageSnapshot::Data
    {
        std::shared_ptr<const Blocks> blocks;
    };

    StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const override;

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    bool supportsParallelInsert() const override { return true; }
    bool supportsSubcolumns() const override { return true; }
    bool supportsDynamicSubcolumns() const override { return true; }

    /// Smaller blocks (e.g. 64K rows) are better for CPU cache.
    bool prefersLargeBlocks() const override { return false; }

    bool hasEvenlyDistributedRead() const override { return true; }

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) override;

    void drop() override;

    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;
    void mutate(const MutationCommands & commands, ContextPtr context) override;

    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &) override;

    bool hasDataToBackup() const override { return true; }
    BackupEntries backupData(ContextPtr context, const ASTs & partitions) override;
    RestoreTaskPtr restoreData(ContextMutablePtr context, const ASTs & partitions, const BackupPtr & backup, const String & data_path_in_backup, const StorageRestoreSettings & restore_settings) override;

    std::optional<UInt64> totalRows(const Settings &) const override;
    std::optional<UInt64> totalBytes(const Settings &) const override;

    /** Delays initialization of StorageMemory::read() until the first read is actually happen.
      * Usually, fore code like this:
      *
      *     auto out = StorageMemory::write();
      *     auto in = StorageMemory::read();
      *     out->write(new_data);
      *
      * `new_data` won't appear into `in`.
      *  However, if delayReadForGlobalSubqueries is called, first read from `in` will check for new_data and return it.
      *
      *
      * Why is delayReadForGlobalSubqueries needed?
      *
      * The fact is that when processing a query of the form
      *  SELECT ... FROM remote_test WHERE column GLOBAL IN (subquery),
      *  if the distributed remote_test table contains localhost as one of the servers,
      *  the query will be interpreted locally again (and not sent over TCP, as in the case of a remote server).
      *
      * The query execution pipeline will be:
      * CreatingSets
      *  subquery execution, filling the temporary table with _data1 (1)
      *  CreatingSets
      *   reading from the table _data1, creating the set (2)
      *   read from the table subordinate to remote_test.
      *
      * (The second part of the pipeline under CreateSets is a reinterpretation of the query inside StorageDistributed,
      *  the query differs in that the database name and tables are replaced with subordinates, and the subquery is replaced with _data1.)
      *
      * But when creating the pipeline, when creating the source (2), it will be found that the _data1 table is empty
      *  (because the query has not started yet), and empty source will be returned as the source.
      * And then, when the query is executed, an empty set will be created in step (2).
      *
      * Therefore, we make the initialization of step (2) delayed
      *  - so that it does not occur until step (1) is completed, on which the table will be populated.
      */
    void delayReadForGlobalSubqueries() { delay_read_for_global_subqueries = true; }

private:
    /// MultiVersion data storage, so that we can copy the vector of blocks to readers.

    MultiVersion<Blocks> data;

    mutable std::mutex mutex;

    bool delay_read_for_global_subqueries = false;

    std::atomic<size_t> total_size_bytes = 0;
    std::atomic<size_t> total_size_rows = 0;

    bool compress;

protected:
    StorageMemory(
        const StorageID & table_id_,
        ColumnsDescription columns_description_,
        ConstraintsDescription constraints_,
        const String & comment,
        bool compress_ = false);
};

}
