#include "Interpreters/Context_fwd.h"

#include <Storages/StorageURLCluster.h>

#include <Common/HTTPHeaderFilter.h>
#include <Core/QueryProcessingStage.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <QueryPipeline/RemoteQueryExecutor.h>

#include <Processors/Transforms/AddingDefaultsTransform.h>

#include <Processors/Sources/RemoteSource.h>
#include <Parsers/queryToString.h>

#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageURL.h>
#include <Storages/extractTableFunctionArgumentsFromSelectQuery.h>
#include <Storages/VirtualColumnUtils.h>

#include <TableFunctions/TableFunctionURLCluster.h>

#include <memory>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 glob_expansion_max_elements;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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
    : IStorageCluster(cluster_name_, table_id_, getLogger("StorageURLCluster (" + table_id_.table_name + ")"))
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

    storage_metadata.setConstraints(constraints_);
    setVirtuals(VirtualColumnUtils::getVirtualsForFileLikeStorage(storage_metadata.columns, context, getSampleURI(uri, context)));
    setInMemoryMetadata(storage_metadata);
}

void StorageURLCluster::updateQueryToSendIfNeeded(ASTPtr & query, const StorageSnapshotPtr & storage_snapshot, const ContextPtr & context)
{
    auto * table_function = extractTableFunctionFromSelectQuery(query);

    TableFunctionURLCluster::updateStructureAndFormatArgumentsIfNeeded(
        table_function,
        storage_snapshot->metadata->getColumns().getAll().toNamesAndTypesDescription(),
        format_name,
        context
    );
}

RemoteQueryExecutor::Extension StorageURLCluster::getTaskIteratorExtension(const ActionsDAG::Node * predicate, const ContextPtr & context) const
{
    auto iterator = std::make_shared<StorageURLSource::DisclosedGlobIterator>(
        uri, context->getSettingsRef()[Setting::glob_expansion_max_elements], predicate, getVirtualsList(), context);
    auto callback = std::make_shared<TaskIterator>([iter = std::move(iterator)]() mutable -> String { return iter->next(); });
    return RemoteQueryExecutor::Extension{.task_iterator = std::move(callback)};
}

}
