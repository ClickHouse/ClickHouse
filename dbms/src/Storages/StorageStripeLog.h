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
friend struct ext::shared_ptr_helper<StorageStripeLog>;

public:
    std::string getName() const override { return "StripeLog"; }
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

    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name, TableStructureWriteLockHolder &) override;

    CheckResults checkData(const ASTPtr & /* query */, const Context & /* context */) override;

    /// Data of the file.
    struct ColumnData
    {
        Poco::File data_file;
    };
    using Files_t = std::map<String, ColumnData>;

    std::string full_path() const { return path + escapeForFileName(table_name) + '/';}

    String getDataPath() const override { return full_path(); }

    void truncate(const ASTPtr &, const Context &, TableStructureWriteLockHolder &) override;

private:
    String path;
    String table_name;
    String database_name;

    size_t max_compress_block_size;

    FileChecker file_checker;
    mutable std::shared_mutex rwlock;

    Logger * log;

protected:
    StorageStripeLog(
        const std::string & path_,
        const std::string & database_name_,
        const std::string & table_name_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        bool attach,
        size_t max_compress_block_size_);
};

}
