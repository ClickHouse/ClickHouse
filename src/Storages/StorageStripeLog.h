#pragma once

#include <map>
#include <shared_mutex>

#include <base/shared_ptr_helper.h>

#include <Core/Defines.h>
#include <Storages/IStorage.h>
#include <Formats/IndexForNativeFormat.h>
#include <Common/FileChecker.h>
#include <Common/escapeForFileName.h>


namespace DB
{
struct IndexForNativeFormat;

/** Implements a table engine that is suitable for small chunks of the log.
  * In doing so, stores all the columns in a single Native file, with a nearby index.
  */
class StorageStripeLog final : public shared_ptr_helper<StorageStripeLog>, public IStorage
{
    friend class StripeLogSource;
    friend class StripeLogSink;
    friend struct shared_ptr_helper<StorageStripeLog>;

public:
    ~StorageStripeLog() override;

    String getName() const override { return "StripeLog"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override;

    CheckResults checkData(const ASTPtr & /* query */, ContextPtr /* context */) override;

    bool storesDataOnDisk() const override { return true; }
    Strings getDataPaths() const override { return {DB::fullPath(disk, table_path)}; }

    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder&) override;

    BackupEntries backup(const ASTs & partitions, ContextPtr context) override;
    RestoreDataTasks restoreFromBackup(const BackupPtr & backup, const String & data_path_in_backup, const ASTs & partitions, ContextMutablePtr context) override;

protected:
    StorageStripeLog(
        DiskPtr disk_,
        const String & relative_path_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        bool attach,
        size_t max_compress_block_size_);

private:
    using ReadLock = std::shared_lock<std::shared_timed_mutex>;
    using WriteLock = std::unique_lock<std::shared_timed_mutex>;

    /// Reads the index file if it hasn't read yet.
    /// It is done lazily, so that with a large number of tables, the server starts quickly.
    void loadIndices(std::chrono::seconds lock_timeout);
    void loadIndices(const WriteLock &);

    /// Saves the index file.
    void saveIndices(const WriteLock &);

    /// Removes all unsaved indices.
    void removeUnsavedIndices(const WriteLock &);

    /// Saves the sizes of the data and index files.
    void saveFileSizes(const WriteLock &);

    const DiskPtr disk;
    String table_path;

    String data_file_path;
    String index_file_path;
    FileChecker file_checker;

    IndexForNativeFormat indices;
    std::atomic<bool> indices_loaded = false;
    size_t num_indices_saved = 0;

    const size_t max_compress_block_size;

    mutable std::shared_timed_mutex rwlock;

    Poco::Logger * log;
};

}
