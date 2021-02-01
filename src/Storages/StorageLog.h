#pragma once

#include <map>
#include <shared_mutex>
#include <ext/shared_ptr_helper.h>

#include <Disks/IDisk.h>
#include <Storages/IStorage.h>
#include <Common/FileChecker.h>
#include <Common/escapeForFileName.h>


namespace DB
{
/** Implements simple table engine without support of indices.
  * The data is stored in a compressed form.
  */
class StorageLog final : public ext::shared_ptr_helper<StorageLog>, public IStorage
{
    friend class LogSource;
    friend class LogBlockOutputStream;
    friend struct ext::shared_ptr_helper<StorageLog>;

public:
    String getName() const override { return "Log"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, const Context & context) override;

    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override;

    CheckResults checkData(const ASTPtr & /* query */, const Context & /* context */) override;

    void truncate(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, const Context &, TableExclusiveLockHolder &) override;

    bool storesDataOnDisk() const override { return true; }
    Strings getDataPaths() const override { return {DB::fullPath(disk, table_path)}; }

protected:
    /** Attach the table with the appropriate name, along the appropriate path (with / at the end),
      *  (the correctness of names and paths is not verified)
      *  consisting of the specified columns; Create files if they do not exist.
      */
    StorageLog(
        DiskPtr disk_,
        const std::string & relative_path_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        bool attach,
        size_t max_compress_block_size_);

private:
    /** Offsets to some row number in a file for column in table.
      * They are needed so that you can read the data in several threads.
      */
    struct Mark
    {
        size_t rows;   /// How many rows are before this offset including the block at this offset.
        size_t offset; /// The offset in compressed file.
    };
    using Marks = std::vector<Mark>;

    /// Column data
    struct ColumnData
    {
        /// Specifies the column number in the marks file.
        /// Does not necessarily match the column number among the columns of the table: columns with lengths of arrays are also numbered here.
        size_t column_index;

        String data_file_path;
        Marks marks;
    };
    using Files = std::map<String, ColumnData>; /// file name -> column data

    DiskPtr disk;
    String table_path;

    mutable std::shared_mutex rwlock;

    Files files;

    Names column_names_by_idx; /// column_index -> name

    String marks_file_path;

    /// The order of adding files should not change: it corresponds to the order of the columns in the marks file.
    void addFiles(const String & column_name, const IDataType & type);

    bool loaded_marks = false;

    size_t max_compress_block_size;
    size_t file_count = 0;

    FileChecker file_checker;

    /// Read marks files if they are not already read.
    /// It is done lazily, so that with a large number of tables, the server starts quickly.
    /// You can not call with a write locked `rwlock`.
    void loadMarks();

    /** For normal columns, the number of rows in the block is specified in the marks.
      * For array columns and nested structures, there are more than one group of marks that correspond to different files
      *  - for elements (file name.bin) - the total number of array elements in the block is specified,
      *  - for array sizes (file name.size0.bin) - the number of rows (the whole arrays themselves) in the block is specified.
      *
      * Return the first group of marks that contain the number of rows, but not the internals of the arrays.
      */
    const Marks & getMarksWithRealRowCount(const StorageMetadataPtr & metadata_snapshot) const;
};

}
