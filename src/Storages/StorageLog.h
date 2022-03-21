#pragma once

#include <map>
#include <shared_mutex>
#include <base/shared_ptr_helper.h>

#include <Disks/IDisk.h>
#include <Storages/IStorage.h>
#include <Common/FileChecker.h>
#include <Common/escapeForFileName.h>
#include <Core/NamesAndTypes.h>


namespace DB
{
/** Implements Log - a simple table engine without support of indices.
  * The data is stored in a compressed form.
  *
  * Also implements TinyLog - a table engine that is suitable for small chunks of the log.
  * It differs from Log in the absence of mark files.
  */
class StorageLog final : public shared_ptr_helper<StorageLog>, public IStorage
{
    friend class LogSource;
    friend class LogSink;
    friend class LogRestoreTask;
    friend struct shared_ptr_helper<StorageLog>;

public:
    ~StorageLog() override;
    String getName() const override { return engine_name; }

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

    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &) override;

    bool storesDataOnDisk() const override { return true; }
    Strings getDataPaths() const override { return {DB::fullPath(disk, table_path)}; }
    bool supportsSubcolumns() const override { return true; }
    ColumnSizeByName getColumnSizes() const override;

    bool hasDataToBackup() const override { return true; }
    BackupEntries backupData(ContextPtr context, const ASTs & partitions) override;
    RestoreTaskPtr restoreData(ContextMutablePtr context, const ASTs & partitions, const BackupPtr & backup, const String & data_path_in_backup, const StorageRestoreSettings & restore_settings) override;

protected:
    /** Attach the table with the appropriate name, along the appropriate path (with / at the end),
      *  (the correctness of names and paths is not verified)
      *  consisting of the specified columns; Create files if they do not exist.
      */
    StorageLog(
        const String & engine_name_,
        DiskPtr disk_,
        const std::string & relative_path_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        bool attach,
        size_t max_compress_block_size_);

private:
    using ReadLock = std::shared_lock<std::shared_timed_mutex>;
    using WriteLock = std::unique_lock<std::shared_timed_mutex>;

    /// The order of adding files should not change: it corresponds to the order of the columns in the marks file.
    /// Should be called from the constructor only.
    void addDataFiles(const NameAndTypePair & column);

    /// Reads the marks file if it hasn't read yet.
    /// It is done lazily, so that with a large number of tables, the server starts quickly.
    void loadMarks(std::chrono::seconds lock_timeout);
    void loadMarks(const WriteLock &);

    /// Saves the marks file.
    void saveMarks(const WriteLock &);

    /// Removes all unsaved marks.
    void removeUnsavedMarks(const WriteLock &);

    /// Saves the sizes of the data and marks files.
    void saveFileSizes(const WriteLock &);

    /** Offsets to some row number in a file for column in table.
      * They are needed so that you can read the data in several threads.
      */
    struct Mark
    {
        size_t rows;   /// How many rows are before this offset including the block at this offset.
        size_t offset; /// The offset in compressed file.

        void write(WriteBuffer & out) const;
        void read(ReadBuffer & in);
    };
    using Marks = std::vector<Mark>;

    /// Column data
    struct DataFile
    {
        size_t index;
        String name;
        String path;
        Marks marks;
    };

    const String engine_name;
    const DiskPtr disk;
    String table_path;

    std::vector<DataFile> data_files;
    size_t num_data_files = 0;
    std::map<String, DataFile *> data_files_by_names;

    /// The Log engine uses the marks file, and the TinyLog engine doesn't.
    const bool use_marks_file;

    String marks_file_path;
    std::atomic<bool> marks_loaded = false;
    size_t num_marks_saved = 0;

    FileChecker file_checker;

    const size_t max_compress_block_size;

    mutable std::shared_timed_mutex rwlock;
};

}
