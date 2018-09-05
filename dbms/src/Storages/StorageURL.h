#pragma once

#include <Storages/IStorage.h>
#include <Poco/URI.h>
#include <common/logger_useful.h>
#include <ext/shared_ptr_helper.h>

namespace DB
{
/**
 * This class represents table engine for external urls.
 * It sends HTTP GET to server when select is called and
 * HTTP POST when insert is called. In POST request the data is send
 * using Chunked transfer encoding, so server have to support it.
 */
class StorageURL : public ext::shared_ptr_helper<StorageURL>, public IStorage
{
public:
    String getName() const override
    {
        return "URL";
    }

    String getTableName() const override
    {
        return table_name;
    }

    BlockInputStreams read(const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;

    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override;

protected:
    StorageURL(const Poco::URI & uri_,
        const std::string & table_name_,
        const String & format_name_,
        const ColumnsDescription & columns_,
        Context & context_);

private:
    Poco::URI uri;
    String format_name;
    String table_name;
    Context & context_global;

    Logger * log = &Logger::get("StorageURL");
};
}
