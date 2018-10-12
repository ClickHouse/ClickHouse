#pragma once

#include <Storages/StorageURL.h>
#include <ext/shared_ptr_helper.h>
#include <Common/XDBCBridgeHelper.h>

namespace DB
{
/** Implements storage in the XDBC database.
  * Use ENGINE = xdbc(connection_string, table_name)
  * Example ENGINE = odbc('dsn=test', table)
  * Read only.
  */
    class StorageXDBC : public IStorageURLBase
    {
    public:

        BlockInputStreams read(const Names & column_names,
                               const SelectQueryInfo & query_info,
                               const Context & context,
                               QueryProcessingStage::Enum processed_stage,
                               size_t max_block_size,
                               unsigned num_streams) override;


        StorageXDBC(const std::string & table_name_,
                    const std::string & remote_database_name,
                    const std::string & remote_table_name,
                    const ColumnsDescription & columns_,
                    const Context & context_, BridgeHelperPtr bridge_helper_);

    private:

        BridgeHelperPtr bridge_helper;
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

        Block getHeaderBlock(const Names & column_names) const override;

        std::string getName() const override;
    };
}
