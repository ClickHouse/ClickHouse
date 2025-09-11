#include "Interpreters/Context_fwd.h"

#include <Common/HTTPHeaderFilter.h>
#include <Core/QueryProcessingStage.h>
#include <Core/Settings.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>

#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/ClusterFunctionReadTask.h>

#include <Processors/Sources/RemoteSource.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <QueryPipeline/RemoteQueryExecutor.h>

#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageURL.h>
#include <Storages/StorageURLCluster.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/extractTableFunctionFromSelectQuery.h>

#include <TableFunctions/TableFunctionURLCluster.h>

#include <memory>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsUInt64 glob_expansion_max_elements;
}

StorageURLCluster::StorageURLCluster(
    const ContextPtr & context,
    const String & cluster_name_,
    const String & uri_,
    const String & format_,
    const String & compression_method,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const StorageURL::Configuration & configuration_)
    : IStorageCluster(cluster_name_, table_id_, getLogger("StorageURLCluster (" + table_id_.getFullTableName() + ")"))
    , uri(uri_), format_name(format_)
{
    context->getRemoteHostFilter().checkURL(Poco::URI(uri));
    context->getHTTPHeaderFilter().checkHeaders(configuration_.headers);

    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        ColumnsDescription columns;
        if (format_name == "auto")
            std::tie(columns, format_name) = StorageURL::getTableStructureAndFormatFromData(
                uri, chooseCompressionMethod(Poco::URI(uri).getPath(), compression_method), configuration_.headers, std::nullopt, context);
        else
            columns = StorageURL::getTableStructureFromData(
                format_, uri, chooseCompressionMethod(Poco::URI(uri).getPath(), compression_method), configuration_.headers, std::nullopt, context);

        storage_metadata.setColumns(columns);
    }
    else
    {
        if (format_name == "auto")
            format_name = StorageURL::getTableStructureAndFormatFromData(
                uri, chooseCompressionMethod(Poco::URI(uri).getPath(), compression_method), configuration_.headers, std::nullopt, context).second;

        storage_metadata.setColumns(columns_);
    }

    auto virtual_columns_desc = VirtualColumnUtils::getVirtualsForFileLikeStorage(storage_metadata.columns, context, getSampleURI(uri, context));
    if (!storage_metadata.getColumns().has("_headers"))
    {
        virtual_columns_desc.addEphemeral(
            "_headers",
            std::make_shared<DataTypeMap>(
                std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
                std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())),
            "");
    }

    storage_metadata.setConstraints(constraints_);
    setVirtuals(virtual_columns_desc);
    setInMemoryMetadata(storage_metadata);
}

void StorageURLCluster::updateQueryToSendIfNeeded(ASTPtr & query, const StorageSnapshotPtr & storage_snapshot, const ContextPtr & context)
{
    auto * table_function = extractTableFunctionFromSelectQuery(query);
    if (!table_function)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected SELECT query from table function urlCluster, got '{}'", query->formatForErrorMessage());

    auto * expression_list = table_function->arguments->as<ASTExpressionList>();
    if (!expression_list)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected SELECT query from table function urlCluster, got '{}'", query->formatForErrorMessage());

    TableFunctionURLCluster::updateStructureAndFormatArgumentsIfNeeded(
        table_function,
        storage_snapshot->metadata->getColumns().getAll().toNamesAndTypesDescription(),
        format_name,
        context
    );
}

RemoteQueryExecutor::Extension StorageURLCluster::getTaskIteratorExtension(
    const ActionsDAG::Node * predicate,
    const ActionsDAG * /* filter */,
    const ContextPtr & context,
    size_t) const
{
    auto iterator = std::make_shared<StorageURLSource::DisclosedGlobIterator>(
        uri, context->getSettingsRef()[Setting::glob_expansion_max_elements], predicate, getVirtualsList(), context);

    auto next_callback = [iter = std::move(iterator)](size_t) mutable -> ClusterFunctionReadTaskResponsePtr
    {
        auto url = iter->next();
        if (url.empty())
            return std::make_shared<ClusterFunctionReadTaskResponse>();
        return std::make_shared<ClusterFunctionReadTaskResponse>(std::move(url));
    };
    auto callback = std::make_shared<TaskIterator>(std::move(next_callback));
    return RemoteQueryExecutor::Extension{.task_iterator = std::move(callback)};
}

}
