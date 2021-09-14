#pragma once

#include <Storages/StorageURL.h>
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
    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    StorageXDBC(
        const StorageID & table_id_,
        const std::string & remote_database_name,
        const std::string & remote_table_name,
        const ColumnsDescription & columns_,
        const Context & context_,
        BridgeHelperPtr bridge_helper_);

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, const Context & context) override;

    std::string getName() const override;
private:

    BridgeHelperPtr bridge_helper;
    std::string remote_database_name;
    std::string remote_table_name;

    Poco::Logger * log;

    std::string getReadMethod() const override;

    std::vector<std::pair<std::string, std::string>> getReadURIParams(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size) const override;

    std::function<void(std::ostream &)> getReadPOSTDataCallback(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size) const override;

    Block getHeaderBlock(const Names & column_names, const StorageMetadataPtr & metadata_snapshot) const override;
};

}
