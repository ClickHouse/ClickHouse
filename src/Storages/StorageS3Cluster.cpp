#include "Storages/StorageS3Cluster.h"

#if USE_AWS_S3

#include <DataTypes/DataTypeString.h>
#include <IO/ConnectionTimeouts.h>
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
    const ContextPtr & context)
    : IStorageCluster(cluster_name_, table_id_, getLogger("StorageS3Cluster (" + table_id_.table_name + ")"))
    , s3_configuration{configuration_}
{
    context->getGlobalContext()->getRemoteHostFilter().checkURL(configuration_.url.uri);
    context->getGlobalContext()->getHTTPHeaderFilter().checkHeaders(configuration_.headers_from_ast);

    StorageInMemoryMetadata storage_metadata;
    updateConfigurationIfChanged(context);

    if (columns_.empty())
    {
        ColumnsDescription columns;
        /// `format_settings` is set to std::nullopt, because StorageS3Cluster is used only as table function
        if (s3_configuration.format == "auto")
            std::tie(columns, s3_configuration.format) = StorageS3::getTableStructureAndFormatFromData(s3_configuration, /*format_settings=*/std::nullopt, context);
        else
            columns = StorageS3::getTableStructureFromData(s3_configuration, /*format_settings=*/std::nullopt, context);

        storage_metadata.setColumns(columns);
    }
    else
    {
        if (s3_configuration.format == "auto")
            s3_configuration.format = StorageS3::getTableStructureAndFormatFromData(s3_configuration, /*format_settings=*/std::nullopt, context).second;

        storage_metadata.setColumns(columns_);
    }

    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);
    setVirtuals(VirtualColumnUtils::getVirtualsForFileLikeStorage(storage_metadata.getColumns()));
}

void StorageS3Cluster::updateQueryToSendIfNeeded(DB::ASTPtr & query, const DB::StorageSnapshotPtr & storage_snapshot, const DB::ContextPtr & context)
{
    ASTExpressionList * expression_list = extractTableFunctionArgumentsFromSelectQuery(query);
    if (!expression_list)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected SELECT query from table function s3Cluster, got '{}'", queryToString(query));

    TableFunctionS3Cluster::updateStructureAndFormatArgumentsIfNeeded(
        expression_list->children,
        storage_snapshot->metadata->getColumns().getAll().toNamesAndTypesDescription(),
        s3_configuration.format,
        context);
}

void StorageS3Cluster::updateConfigurationIfChanged(ContextPtr local_context)
{
    s3_configuration.update(local_context);
}

RemoteQueryExecutor::Extension StorageS3Cluster::getTaskIteratorExtension(const ActionsDAG::Node * predicate, const ContextPtr & context) const
{
    auto iterator = std::make_shared<StorageS3Source::DisclosedGlobIterator>(
        *s3_configuration.client, s3_configuration.url, predicate, getVirtualsList(), context, nullptr, s3_configuration.request_settings, context->getFileProgressCallback());

    auto callback = std::make_shared<std::function<String()>>([iterator]() mutable -> String
    {
        if (auto next = iterator->next())
            return next->key;
        return "";
    });
    return RemoteQueryExecutor::Extension{ .task_iterator = std::move(callback) };
}

}

#endif
