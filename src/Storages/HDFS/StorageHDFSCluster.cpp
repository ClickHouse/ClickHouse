#include "config.h"
#include "Interpreters/Context_fwd.h"

#if USE_HDFS

#include <Storages/HDFS/StorageHDFSCluster.h>

#include <Core/QueryProcessingStage.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <QueryPipeline/RemoteQueryExecutor.h>

#include <Processors/Transforms/AddingDefaultsTransform.h>

#include <Processors/Sources/RemoteSource.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/queryToString.h>

#include <Storages/HDFS/HDFSCommon.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/extractTableFunctionArgumentsFromSelectQuery.h>
#include <Storages/VirtualColumnUtils.h>

#include <TableFunctions/TableFunctionHDFSCluster.h>
#include <memory>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

StorageHDFSCluster::StorageHDFSCluster(
    ContextPtr context_,
    const String & cluster_name_,
    const String & uri_,
    const StorageID & table_id_,
    const String & format_name_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & compression_method)
    : IStorageCluster(cluster_name_, table_id_, getLogger("StorageHDFSCluster (" + table_id_.table_name + ")"))
    , uri(uri_)
    , format_name(format_name_)
{
    checkHDFSURL(uri_);
    context_->getRemoteHostFilter().checkURL(Poco::URI(uri_));

    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        ColumnsDescription columns;
        if (format_name == "auto")
            std::tie(columns, format_name) = StorageHDFS::getTableStructureAndFormatFromData(uri_, compression_method, context_);
        else
            columns = StorageHDFS::getTableStructureFromData(format_name, uri_, compression_method, context_);
        storage_metadata.setColumns(columns);
    }
    else
    {
        if (format_name == "auto")
            format_name = StorageHDFS::getTableStructureAndFormatFromData(uri_, compression_method, context_).second;

        storage_metadata.setColumns(columns_);
    }

    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);
    setVirtuals(VirtualColumnUtils::getVirtualsForFileLikeStorage(storage_metadata.getColumns()));
}

void StorageHDFSCluster::updateQueryToSendIfNeeded(DB::ASTPtr & query, const DB::StorageSnapshotPtr & storage_snapshot, const DB::ContextPtr & context)
{
    ASTExpressionList * expression_list = extractTableFunctionArgumentsFromSelectQuery(query);
    if (!expression_list)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected SELECT query from table function hdfsCluster, got '{}'", queryToString(query));

    TableFunctionHDFSCluster::updateStructureAndFormatArgumentsIfNeeded(
        expression_list->children, storage_snapshot->metadata->getColumns().getAll().toNamesAndTypesDescription(), format_name, context);
}


RemoteQueryExecutor::Extension StorageHDFSCluster::getTaskIteratorExtension(const ActionsDAG::Node * predicate, const ContextPtr & context) const
{
    auto iterator = std::make_shared<HDFSSource::DisclosedGlobIterator>(uri, predicate, getVirtualsList(), context);
    auto callback = std::make_shared<std::function<String()>>([iter = std::move(iterator)]() mutable -> String { return iter->next().path; });
    return RemoteQueryExecutor::Extension{.task_iterator = std::move(callback)};
}

}

#endif
