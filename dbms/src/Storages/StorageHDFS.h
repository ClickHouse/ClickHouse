#pragma once
#include <Common/config.h>
#if USE_HDFS

#include <Storages/IStorage.h>
#include <Poco/URI.h>
#include <common/logger_useful.h>
#include <ext/shared_ptr_helper.h>

namespace DB
{
/**
 * This class represents table engine for external hdfs files.
 * Read method is supported for now.
 */
class StorageHDFS : public ext::shared_ptr_helper<StorageHDFS>, public IStorage
{
public:
    String getName() const override
    {
        return "HDFS";
    }

    String getTableName() const override
    {
        return table_name;
    }

    BlockInputStreams read(const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Context & context) override;

    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override;

protected:
    StorageHDFS(const String & uri_,
        const String & table_name_,
        const String & format_name_,
        const ColumnsDescription & columns_,
        Context & context_);

private:
    String uri;
    String format_name;
    String table_name;
    Context & context;

    Logger * log = &Logger::get("StorageHDFS");
};
}

#endif
