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
    const String & compression_method_,
    bool structure_argument_was_provided_)
    : IStorageCluster(cluster_name_, table_id_, &Poco::Logger::get("StorageHDFSCluster (" + table_id_.table_name + ")"), structure_argument_was_provided_)
    , uri(uri_)
    , format_name(format_name_)
    , compression_method(compression_method_)
{
    checkHDFSURL(uri_);
    context_->getRemoteHostFilter().checkURL(Poco::URI(uri_));

    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        auto columns = StorageHDFS::getTableStructureFromData(format_name, uri_, compression_method, context_);
        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);

    virtual_columns = VirtualColumnUtils::getPathAndFileVirtualsForStorage(storage_metadata.getSampleBlock().getNamesAndTypesList());
}

void StorageHDFSCluster::addColumnsStructureToQuery(ASTPtr & query, const String & structure, const ContextPtr & context)
{
    ASTExpressionList * expression_list = extractTableFunctionArgumentsFromSelectQuery(query);
    if (!expression_list)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected SELECT query from table function hdfsCluster, got '{}'", queryToString(query));

    TableFunctionHDFSCluster::addColumnsStructureToArguments(expression_list->children, structure, context);
}


RemoteQueryExecutor::Extension StorageHDFSCluster::getTaskIteratorExtension(ASTPtr query, const ContextPtr & context) const
{
    auto iterator = std::make_shared<HDFSSource::DisclosedGlobIterator>(uri, query, virtual_columns, context);
    auto callback = std::make_shared<std::function<String()>>([iter = std::move(iterator)]() mutable -> String { return iter->next().path; });
    return RemoteQueryExecutor::Extension{.task_iterator = std::move(callback)};
}

NamesAndTypesList StorageHDFSCluster::getVirtuals() const
{
    return NamesAndTypesList{
        {"_path", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())}};
}

}

#endif
