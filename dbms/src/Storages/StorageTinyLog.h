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
friend class ext::shared_ptr_helper<StorageTinyLog>;
friend class TinyLogBlockInputStream;
friend class TinyLogBlockOutputStream;

public:
    std::string getName() const override { return "TinyLog"; }
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

    void drop() override;

    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override;

    bool checkData() const override;

    /// Column data
    struct ColumnData
    {
        Poco::File data_file;
    };
    using Files_t = std::map<String, ColumnData>;

    std::string full_path() { return path + escapeForFileName(name) + '/';}

private:
    String path;
    String name;
    NamesAndTypesListPtr columns;

    size_t max_compress_block_size;

    Files_t files;

    FileChecker file_checker;

    Logger * log;

    StorageTinyLog(
        const std::string & path_,
        const std::string & name_,
        NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        bool attach,
        size_t max_compress_block_size_ = DEFAULT_MAX_COMPRESS_BLOCK_SIZE);

    void addFile(const String & column_name, const IDataType & type, size_t level = 0);
};

}
