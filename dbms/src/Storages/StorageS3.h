#pragma once

#include <Storages/IStorage.h>

#include <Poco/File.h>
#include <Poco/Path.h>
#include <Poco/URI.h>

#include <common/logger_useful.h>

#include <atomic>
#include <shared_mutex>
#include <ext/shared_ptr_helper.h>


namespace DB
{

class StorageS3BlockInputStream;
class StorageS3BlockOutputStream;

class StorageS3 : public ext::shared_ptr_helper<StorageS3>, public IStorage
{
public:
    std::string getName() const override
    {
        return "S3";
    }

    std::string getTableName() const override
    {
        return table_name;
    }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(
        const ASTPtr & query,
        const Context & context) override;

    void drop() override;

    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override;

protected:
    friend class StorageS3BlockInputStream;
    friend class StorageS3BlockOutputStream;

    /** there are three options (ordered by priority):
    - use specified file descriptor if (fd >= 0)
    - use specified table_path if it isn't empty
    - create own table inside data/db/table/
    */
    StorageS3(
        const std::string & table_uri_,
        const std::string & table_name_,
        const std::string & format_name_,
        const ColumnsDescription & columns_,
        Context & context_);

private:

    std::string table_name;
    std::string format_name;
    Context & context_global;

    Poco::URI uri;

    bool is_db_table = true;                     /// Table is stored in real database, not user's file

    mutable std::shared_mutex rwlock;

    Logger * log = &Logger::get("StorageS3");
};

}
