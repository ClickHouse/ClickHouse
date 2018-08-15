#pragma once

#include <Storages/StorageURL.h>
#include <Common/ODBCBridgeHelper.h>
#include <ext/shared_ptr_helper.h>

namespace DB
{
/** Implements storage in the ODBC database.
  * Use ENGINE = odbc(connection_string, table_name)
  * Example ENGINE = odbc('dsn=test', table)
  * Read only.
  */
class StorageODBC : public ext::shared_ptr_helper<StorageODBC>, public IStorageURLBase
{
public:
    std::string getName() const override
    {
        return "ODBC";
    }

    BlockInputStreams read(const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;


protected:
    StorageODBC(const std::string & table_name_,
        const std::string & connection_string,
        const std::string & remote_database_name,
        const std::string & remote_table_name,
        const ColumnsDescription & columns_,
        const Context & context_);

private:
    ODBCBridgeHelper odbc_bridge_helper;
    std::string remote_database_name;
    std::string remote_table_name;

    Poco::Logger * log;

    std::string getReadMethod() const override;

    std::vector<std::pair<std::string, std::string>> getReadURIParams(const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size) const override;

    std::function<void(std::ostream &)> getReadPOSTDataCallback(const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size) const override;
};
}
