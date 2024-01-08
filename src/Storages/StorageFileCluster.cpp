#include "Interpreters/Context_fwd.h"
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Sources/RemoteSource.h>
#include <Parsers/queryToString.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/StorageFileCluster.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFile.h>
#include <Storages/extractTableFunctionArgumentsFromSelectQuery.h>
#include <Storages/VirtualColumnUtils.h>
#include <TableFunctions/TableFunctionFileCluster.h>

#include <memory>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

StorageFileCluster::StorageFileCluster(
    ContextPtr context_,
    const String & cluster_name_,
    const String & filename_,
    const String & format_name_,
    const String & compression_method_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    bool structure_argument_was_provided_)
    : IStorageCluster(cluster_name_, table_id_, &Poco::Logger::get("StorageFileCluster (" + table_id_.table_name + ")"), structure_argument_was_provided_)
    , filename(filename_)
    , format_name(format_name_)
    , compression_method(compression_method_)
{
    StorageInMemoryMetadata storage_metadata;

    size_t total_bytes_to_read; // its value isn't used as we are not reading files (just listing them). But it is required by getPathsList
    paths = StorageFile::getPathsList(filename_, context_->getUserFilesPath(), context_, total_bytes_to_read);

    if (columns_.empty())
    {
        auto columns = StorageFile::getTableStructureFromFile(format_name,
                                                             paths,
                                                             compression_method,
                                                             std::nullopt,
                                                             context_);
        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);

    virtual_columns = VirtualColumnUtils::getPathFileAndSizeVirtualsForStorage(storage_metadata.getSampleBlock().getNamesAndTypesList());
}

void StorageFileCluster::addColumnsStructureToQuery(ASTPtr & query, const String & structure, const ContextPtr & context)
{
    ASTExpressionList * expression_list = extractTableFunctionArgumentsFromSelectQuery(query);
    if (!expression_list)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected SELECT query from table function fileCluster, got '{}'", queryToString(query));

    TableFunctionFileCluster::addColumnsStructureToArguments(expression_list->children, structure, context);
}

RemoteQueryExecutor::Extension StorageFileCluster::getTaskIteratorExtension(const ActionsDAG::Node * predicate, const ContextPtr & context) const
{
    auto iterator = std::make_shared<StorageFileSource::FilesIterator>(paths, std::nullopt, predicate, virtual_columns, context);
    auto callback = std::make_shared<TaskIterator>([iter = std::move(iterator)]() mutable -> String { return iter->next(); });
    return RemoteQueryExecutor::Extension{.task_iterator = std::move(callback)};
}

}
