#include "Storages/StorageAzureBlobCluster.h"

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/IStorage.h>
#include <Storages/StorageURL.h>
#include <Storages/StorageDictionary.h>
#include <Storages/extractTableFunctionArgumentsFromSelectQuery.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/Exception.h>
#include <Parsers/queryToString.h>
#include <TableFunctions/TableFunctionAzureBlobStorageCluster.h>

#include <memory>
#include <string>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

StorageAzureBlobCluster::StorageAzureBlobCluster(
    const String & cluster_name_,
    const StorageAzureBlob::Configuration & configuration_,
    std::unique_ptr<AzureObjectStorage> && object_storage_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const ContextPtr & context)
    : IStorageCluster(cluster_name_, table_id_, getLogger("StorageAzureBlobCluster (" + table_id_.table_name + ")"))
    , configuration{configuration_}
    , object_storage(std::move(object_storage_))
{
    context->getGlobalContext()->getRemoteHostFilter().checkURL(configuration_.getConnectionURL());
    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        ColumnsDescription columns;
        /// `format_settings` is set to std::nullopt, because StorageAzureBlobCluster is used only as table function
        if (configuration.format == "auto")
            std::tie(columns, configuration.format) = StorageAzureBlob::getTableStructureAndFormatFromData(object_storage.get(), configuration, /*format_settings=*/std::nullopt, context);
        else
            columns = StorageAzureBlob::getTableStructureFromData(object_storage.get(), configuration, /*format_settings=*/std::nullopt, context);
        storage_metadata.setColumns(columns);
    }
    else
    {
        if (configuration.format == "auto")
            configuration.format = StorageAzureBlob::getTableStructureAndFormatFromData(object_storage.get(), configuration, /*format_settings=*/std::nullopt, context).second;
        storage_metadata.setColumns(columns_);
    }

    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);
    setVirtuals(VirtualColumnUtils::getVirtualsForFileLikeStorage(storage_metadata.getColumns()));
}

void StorageAzureBlobCluster::updateQueryToSendIfNeeded(DB::ASTPtr & query, const DB::StorageSnapshotPtr & storage_snapshot, const DB::ContextPtr & context)
{
    ASTExpressionList * expression_list = extractTableFunctionArgumentsFromSelectQuery(query);
    if (!expression_list)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected SELECT query from table function s3Cluster, got '{}'", queryToString(query));

    TableFunctionAzureBlobStorageCluster::updateStructureAndFormatArgumentsIfNeeded(
        expression_list->children, storage_snapshot->metadata->getColumns().getAll().toNamesAndTypesDescription(), configuration.format, context);
}

RemoteQueryExecutor::Extension StorageAzureBlobCluster::getTaskIteratorExtension(const ActionsDAG::Node * predicate, const ContextPtr & context) const
{
    auto iterator = std::make_shared<StorageAzureBlobSource::GlobIterator>(
        object_storage.get(), configuration.container, configuration.blob_path,
        predicate, getVirtualsList(), context, nullptr);

    auto callback = std::make_shared<std::function<String()>>([iterator]() mutable -> String{ return iterator->next().relative_path; });
    return RemoteQueryExecutor::Extension{ .task_iterator = std::move(callback) };
}

}

#endif
