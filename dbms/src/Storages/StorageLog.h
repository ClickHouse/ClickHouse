#pragma once

#include <map>
#include <shared_mutex>
#include <ext/shared_ptr_helper.h>

#include <Poco/File.h>

#include <Storages/IStorage.h>
#include <Common/FileChecker.h>
#include <Common/escapeForFileName.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}


/** Offsets to every single set of values.
  * These sets are the same size in different columns.
  * They are needed so that you can read the data in several threads.
  */
struct Mark
{
    size_t rows;    /// How many lines are contained in this set and all previous ones.
    size_t offset;  /// The offset to the set in the compressed file.
};

using Marks = std::vector<Mark>;


/** Implements a table engine that is suitable for logs.
  * Keys are not supported.
  * The data is stored in a compressed form.
  */
class StorageLog : public ext::shared_ptr_helper<StorageLog>, public IStorage
{
friend class ext::shared_ptr_helper<StorageLog>;
friend class LogBlockInputStream;
friend class LogBlockOutputStream;

public:
    std::string getName() const override { return "Log"; }
    std::string getTableName() const override { return name; }

    const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;

    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override;

    /// Column data
    struct ColumnData
    {
        /// Specifies the column number in the marks file.
        /// Does not necessarily match the column number among the columns of the table: columns with lengths of arrays are also numbered here.
        size_t column_index;

        Poco::File data_file;
        Marks marks;
    };
    using Files_t = std::map<String, ColumnData>;

    bool checkData() const override;

protected:
    String path;
    String name;
    NamesAndTypesListPtr columns;

    mutable std::shared_mutex rwlock;

    /** Attach the table with the appropriate name, along the appropriate path (with / at the end),
      *  (the correctness of names and paths is not verified)
      *  consisting of the specified columns; Create files if they do not exist.
      */
    StorageLog(
        const std::string & path_,
        const std::string & name_,
        NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        size_t max_compress_block_size_);

    /// Read marks files if they are not already read.
    /// It is done lazily, so that with a large number of tables, the server starts quickly.
    /// You can not call with a write locked `rwlock`.
    void loadMarks();

    /// Can be called with any state of `rwlock`.
    size_t marksCount();

private:
    Files_t files; /// name -> data

    Names column_names; /// column_index -> name
    Names null_map_filenames;

    Poco::File marks_file;
    Poco::File null_marks_file;

    void loadMarksImpl(bool load_null_marks);

    /// The order of adding files should not change: it corresponds to the order of the columns in the marks file.
    void addFile(const String & column_name, const IDataType & type, size_t level = 0);

    bool loaded_marks;
    bool has_nullable_columns = false;

    size_t max_compress_block_size;
    size_t file_count = 0;
    size_t null_file_count = 0;

protected:
    FileChecker file_checker;

private:
    /** For normal columns, the number of rows in the block is specified in the marks.
      * For array columns and nested structures, there are more than one group of marks that correspond to different files
      *  - for insides (file name.bin) - the total number of array elements in the block is specified,
      *  - for array sizes (file name.size0.bin) - the number of rows (the whole arrays themselves) in the block is specified.
      *
      * Return the first group of marks that contain the number of rows, but not the internals of the arrays.
      */
    const Marks & getMarksWithRealRowCount() const;

    std::string getFullPath() const { return path + escapeForFileName(name) + '/';}
};

}
