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

public:
    std::string getName() const override { return "TinyLog"; }
    std::string getTableName() const override { return name; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;

    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override;

    bool checkData() const override;

    /// Column data
    struct ColumnData
    {
        Poco::File data_file;
    };
    using Files_t = std::map<String, ColumnData>;

    std::string full_path() const { return path + escapeForFileName(name) + '/';}

    String getDataPath() const override { return full_path(); }

    void truncate(const ASTPtr &) override;

private:
    String path;
    String name;

    size_t max_compress_block_size;

    Files_t files;

    FileChecker file_checker;

    Logger * log;

    void addFile(const String & column_name, const IDataType & type, size_t level = 0);
    void addFiles(const String & column_name, const IDataType & type);

protected:
    StorageTinyLog(
        const std::string & path_,
        const std::string & name_,
        const ColumnsDescription & columns_,
        bool attach,
        size_t max_compress_block_size_);
};

}
