#pragma once

#include <map>
#include <shared_mutex>

#include <ext/shared_ptr_helper.h>

#include <Poco/File.h>

#include <Storages/IStorage.h>
#include <Common/FileChecker.h>
#include <Common/escapeForFileName.h>
#include <Core/Defines.h>


namespace DB
{

/** Implements a table engine that is suitable for small chunks of the log.
  * In doing so, stores all the columns in a single Native file, with a nearby index.
  */
class StorageStripeLog : public ext::shared_ptr_helper<StorageStripeLog>, public IStorage
{
friend class StripeLogBlockInputStream;
friend class StripeLogBlockOutputStream;

public:
    std::string getName() const override { return "StripeLog"; }
    std::string getTableName() const override { return name; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Context & context) override;

    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override;

    bool checkData() const override;

    /// Data of the file.
    struct ColumnData
    {
        Poco::File data_file;
    };
    using Files_t = std::map<String, ColumnData>;

    std::string full_path() const { return path + escapeForFileName(name) + '/';}

    String getDataPath() const override { return full_path(); }

    void truncate(const ASTPtr &, const Context &) override;

private:
    String path;
    String name;

    size_t max_compress_block_size;

    FileChecker file_checker;
    mutable std::shared_mutex rwlock;

    Logger * log;

protected:
    StorageStripeLog(
        const std::string & path_,
        const std::string & name_,
        const ColumnsDescription & columns_,
        bool attach,
        size_t max_compress_block_size_);
};

}
