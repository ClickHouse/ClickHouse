#include "Storages/StorageS3Cluster.h"

#include "config.h"

#if USE_AWS_S3

#include <DataTypes/DataTypeString.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/IStorage.h>
#include <Storages/StorageURL.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageDictionary.h>
#include <Storages/extractTableFunctionArgumentsFromSelectQuery.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/Exception.h>
#include <Parsers/queryToString.h>
#include <TableFunctions/TableFunctionS3Cluster.h>

#include <memory>
#include <string>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

StorageS3Cluster::StorageS3Cluster(
    const String & cluster_name_,
    const StorageS3::Configuration & configuration_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    ContextPtr context_,
    bool structure_argument_was_provided_)
    : IStorageCluster(cluster_name_, table_id_, &Poco::Logger::get("StorageS3Cluster (" + table_id_.table_name + ")"), structure_argument_was_provided_)
    , s3_configuration{configuration_}
{
    context_->getGlobalContext()->getRemoteHostFilter().checkURL(configuration_.url.uri);
    context_->getGlobalContext()->getHTTPHeaderFilter().checkHeaders(configuration_.headers_from_ast);

    StorageInMemoryMetadata storage_metadata;
    updateConfigurationIfChanged(context_);

    if (columns_.empty())
    {
        /// `format_settings` is set to std::nullopt, because StorageS3Cluster is used only as table function
        auto columns = StorageS3::getTableStructureFromDataImpl(s3_configuration, /*format_settings=*/std::nullopt, context_);
        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);

    virtual_columns = VirtualColumnUtils::getPathAndFileVirtualsForStorage(storage_metadata.getSampleBlock().getNamesAndTypesList());
}

void StorageS3Cluster::addColumnsStructureToQuery(ASTPtr & query, const String & structure, const ContextPtr & context)
{
    ASTExpressionList * expression_list = extractTableFunctionArgumentsFromSelectQuery(query);
    if (!expression_list)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected SELECT query from table function s3Cluster, got '{}'", queryToString(query));

    TableFunctionS3Cluster::addColumnsStructureToArguments(expression_list->children, structure, context);
}

void StorageS3Cluster::updateConfigurationIfChanged(ContextPtr local_context)
{
    s3_configuration.update(local_context);
}

RemoteQueryExecutor::Extension StorageS3Cluster::getTaskIteratorExtension(ASTPtr query, const ContextPtr & context) const
{
    auto iterator = std::make_shared<StorageS3Source::DisclosedGlobIterator>(
        *s3_configuration.client, s3_configuration.url, query, virtual_columns, context, nullptr, s3_configuration.request_settings, context->getFileProgressCallback());
    auto callback = std::make_shared<std::function<String()>>([iterator]() mutable -> String { return iterator->next().key; });
    return RemoteQueryExecutor::Extension{ .task_iterator = std::move(callback) };
}

NamesAndTypesList StorageS3Cluster::getVirtuals() const
{
    return virtual_columns;
}


}

#endif
