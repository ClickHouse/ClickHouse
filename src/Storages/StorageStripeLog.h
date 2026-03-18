#pragma once

#include <map>
#include <shared_mutex>

#include <Core/Defines.h>
#include <Storages/IStorage.h>
#include <Formats/IndexForNativeFormat.h>
#include <Common/FileChecker.h>
#include <Common/escapeForFileName.h>
#include <Disks/IDisk.h>


namespace DB
{
struct IndexForNativeFormat;
class IBackup;
using BackupPtr = std::shared_ptr<const IBackup>;

/** Implements a table engine that is suitable for small chunks of the log.
  * In doing so, stores all the columns in a single Native file, with a nearby index.
  */
class StorageStripeLog final : public IStorage, public WithMutableContext
{
friend class StripeLogSource;
friend class StripeLogSink;

public:
    StorageStripeLog(
        DiskPtr disk_,
        const String & relative_path_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        LoadingStrictnessLevel mode,
        ContextMutablePtr context_);

    ~StorageStripeLog() override;

    String getName() const override { return "StripeLog"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr local_context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool async_insert) override;

    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override;

    DataValidationTasksPtr getCheckTaskList(const CheckTaskFilter & check_task_filter, ContextPtr context) override;
    std::optional<CheckResult> checkDataNext(DataValidationTasksPtr & check_task_list) override;

    bool storesDataOnDisk() const override { return true; }
    Strings getDataPaths() const override { return {DB::fullPath(disk, table_path)}; }

    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder&) override;

    std::optional<UInt64> totalRows(const Settings & settings) const override;
    std::optional<UInt64> totalBytes(const Settings & settings) const override;

    void backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override;
    void restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override;

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

    /// Recalculates the number of rows stored in this table.
    void updateTotalRows(const WriteLock &);

    /// Restores the data of this table from backup.
    void restoreDataImpl(const BackupPtr & backup, const String & data_path_in_backup, std::chrono::seconds lock_timeout);

    const DiskPtr disk;
    String table_path;

    struct DataValidationTasks : public IStorage::DataValidationTasksBase
    {
        DataValidationTasks(FileChecker::DataValidationTasksPtr file_checker_tasks_, ReadLock && lock_)
            : file_checker_tasks(std::move(file_checker_tasks_)), lock(std::move(lock_))
        {}

        size_t size() const override { return file_checker_tasks->size(); }

        FileChecker::DataValidationTasksPtr file_checker_tasks;

        /// Lock to prevent table modification while checking
        ReadLock lock;
    };

    String data_file_path;
    String index_file_path;
    FileChecker file_checker;

    IndexForNativeFormat indices;
    std::atomic<bool> indices_loaded = false;
    size_t num_indices_saved = 0;

    std::atomic<UInt64> total_rows = 0;
    std::atomic<UInt64> total_bytes = 0;

    const size_t max_compress_block_size;

    mutable std::shared_timed_mutex rwlock;

    LoggerPtr log;
};

}
