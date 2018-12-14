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
class IStorageURLBase : public IStorage
{
public:
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

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;

    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override;

protected:
    IStorageURLBase(const Poco::URI & uri_,
        const Context & context_,
        const std::string & table_name_,
        const String & format_name_,
        const ColumnsDescription & columns_);

    Poco::URI uri;
    const Context & context_global;

private:
    String format_name;
    String table_name;

    virtual std::string getReadMethod() const;

    virtual std::vector<std::pair<std::string, std::string>> getReadURIParams(const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size) const;

    virtual std::function<void(std::ostream &)> getReadPOSTDataCallback(const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size) const;

    virtual Block getHeaderBlock(const Names & column_names) const = 0;
};

class StorageURL : public ext::shared_ptr_helper<StorageURL>, public IStorageURLBase
{
public:
    StorageURL(const Poco::URI & uri_,
        const std::string & table_name_,
        const String & format_name_,
        const ColumnsDescription & columns_,
        Context & context_)
        : IStorageURLBase(uri_, context_, table_name_, format_name_, columns_)
    {
    }

    String getName() const override
    {
        return "URL";
    }

    Block getHeaderBlock(const Names & /*column_names*/) const override
    {
        return getSampleBlock();
    }
};
}
