#pragma once

#include <map>

#include <ext/shared_ptr_helper.h>

#include <Poco/File.h>

#include <Storages/IStorage.h>
#include <Common/FileChecker.h>
#include <Common/escapeForFileName.h>
#include <Core/Defines.h>


namespace DB
{

/** Implements a table engine that is suitable for small chunks of the log.
  * It differs from StorageLog in the absence of mark files.
  */
class StorageTinyLog : public ext::shared_ptr_helper<StorageTinyLog>, public IStorage
{
friend class TinyLogBlockInputStream;
friend class TinyLogBlockOutputStream;
friend struct ext::shared_ptr_helper<StorageTinyLog>;

public:
    std::string getName() const override { return "TinyLog"; }
    std::string getTableName() const override { return table_name; }
    std::string getDatabaseName() const override { return database_name; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Context & context) override;

    void rename(const String & new_path_to_table_data, const String & new_database_name, const String & new_table_name, TableStructureWriteLockHolder &) override;

    CheckResults checkData(const ASTPtr & /* query */, const Context & /* context */) override;

    /// Column data
    struct ColumnData
    {
        Poco::File data_file;
    };
    using Files_t = std::map<String, ColumnData>;

    std::string fullPath() const { return path; }

    Strings getDataPaths() const override { return {fullPath()}; }

    void truncate(const ASTPtr &, const Context &, TableStructureWriteLockHolder &) override;

private:
    String base_path;
    String path;
    String table_name;
    String database_name;

    size_t max_compress_block_size;

    Files_t files;

    FileChecker file_checker;
    mutable std::shared_mutex rwlock;

    Logger * log;

    void addFile(const String & column_name, const IDataType & type, size_t level = 0);
    void addFiles(const String & column_name, const IDataType & type);

protected:
    StorageTinyLog(
        const std::string & relative_path_,
        const std::string & database_name_,
        const std::string & table_name_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        bool attach,
        size_t max_compress_block_size_,
        const Context & context_);
};

}
