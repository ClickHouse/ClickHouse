#pragma once

#include <Storages/StorageURL.h>
#include <BridgeHelper/XDBCBridgeHelper.h>

namespace Poco
{
class Logger;
}

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
    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    StorageXDBC(
        const StorageID & table_id_,
        const std::string & remote_database_name,
        const std::string & remote_table_name,
        ColumnsDescription columns_,
        ConstraintsDescription constraints_,
        const String & comment,
        ContextPtr context_,
        BridgeHelperPtr bridge_helper_);

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context, bool async_insert) override;

    std::string getName() const override;
private:
    BridgeHelperPtr bridge_helper;
    std::string remote_database_name;
    std::string remote_table_name;

    LoggerPtr log;

    std::string getReadMethod() const override;

    std::vector<std::pair<std::string, std::string>> getReadURIParams(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        const SelectQueryInfo & query_info,
        const ContextPtr & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size) const override;

    std::function<void(std::ostream &)> getReadPOSTDataCallback(
        const Names & column_names,
        const ColumnsDescription & columns_description,
        const SelectQueryInfo & query_info,
        const ContextPtr & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size) const override;

    Block getHeaderBlock(const Names & column_names, const StorageSnapshotPtr & storage_snapshot) const override;

    bool supportsSubsetOfColumns(const ContextPtr &) const override;
};

}
