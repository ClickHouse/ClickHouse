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
    ContextPtr context_,
    bool structure_argument_was_provided_)
    : IStorageCluster(cluster_name_, table_id_, &Poco::Logger::get("StorageAzureBlobCluster (" + table_id_.table_name + ")"), structure_argument_was_provided_)
    , configuration{configuration_}
    , object_storage(std::move(object_storage_))
{
    context_->getGlobalContext()->getRemoteHostFilter().checkURL(configuration_.getConnectionURL());
    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        /// `format_settings` is set to std::nullopt, because StorageAzureBlobCluster is used only as table function
        auto columns = StorageAzureBlob::getTableStructureFromData(object_storage.get(), configuration, /*format_settings=*/std::nullopt, context_, false);
        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);

    virtual_columns = VirtualColumnUtils::getPathAndFileVirtualsForStorage(storage_metadata.getSampleBlock().getNamesAndTypesList());
}

void StorageAzureBlobCluster::addColumnsStructureToQuery(ASTPtr & query, const String & structure, const ContextPtr & context)
{
    ASTExpressionList * expression_list = extractTableFunctionArgumentsFromSelectQuery(query);
    if (!expression_list)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected SELECT query from table function s3Cluster, got '{}'", queryToString(query));

    TableFunctionAzureBlobStorageCluster::addColumnsStructureToArguments(expression_list->children, structure, context);
}

RemoteQueryExecutor::Extension StorageAzureBlobCluster::getTaskIteratorExtension(ASTPtr query, const ContextPtr & context) const
{
    auto iterator = std::make_shared<StorageAzureBlobSource::GlobIterator>(
        object_storage.get(), configuration.container, configuration.blob_path,
        query, virtual_columns, context, nullptr);
    auto callback = std::make_shared<std::function<String()>>([iterator]() mutable -> String{ return iterator->next().relative_path; });
    return RemoteQueryExecutor::Extension{ .task_iterator = std::move(callback) };
}

NamesAndTypesList StorageAzureBlobCluster::getVirtuals() const
{
    return virtual_columns;
}


}

#endif
