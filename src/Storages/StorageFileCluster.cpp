#include "Interpreters/Context_fwd.h"
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/ClusterFunctionReadTask.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Sources/RemoteSource.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/StorageFileCluster.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFile.h>
#include <Storages/extractTableFunctionFromSelectQuery.h>
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
    const ContextPtr & context,
    const String & cluster_name_,
    const String & filename_,
    const String & format_name_,
    const String & compression_method,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_)
    : IStorageCluster(cluster_name_, table_id_, getLogger("StorageFileCluster (" + table_id_.getFullTableName() + ")"))
    , filename(filename_)
    , format_name(format_name_)
{
    StorageInMemoryMetadata storage_metadata;

    size_t total_bytes_to_read; // its value isn't used as we are not reading files (just listing them). But it is required by getPathsList
    paths = StorageFile::getPathsList(filename_, context->getUserFilesPath(), context, total_bytes_to_read);

    if (columns_.empty())
    {
        ColumnsDescription columns;
        if (format_name == "auto")
            std::tie(columns, format_name) = StorageFile::getTableStructureAndFormatFromFile(paths, compression_method, std::nullopt, context);
        else
            columns = StorageFile::getTableStructureFromFile(format_name, paths, compression_method, std::nullopt, context);

        storage_metadata.setColumns(columns);
    }
    else
    {
        if (format_name == "auto")
            format_name = StorageFile::getTableStructureAndFormatFromFile(paths, compression_method, std::nullopt, context).second;
        storage_metadata.setColumns(columns_);
    }

    storage_metadata.setConstraints(constraints_);
    setVirtuals(VirtualColumnUtils::getVirtualsForFileLikeStorage(storage_metadata.columns, context, paths.empty() ? "" : paths[0]));
    setInMemoryMetadata(storage_metadata);
}

void StorageFileCluster::updateQueryToSendIfNeeded(DB::ASTPtr & query, const StorageSnapshotPtr & storage_snapshot, const DB::ContextPtr & context)
{
    auto * table_function = extractTableFunctionFromSelectQuery(query);
    if (!table_function)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected SELECT query from table function fileCluster, got '{}'", query->formatForErrorMessage());

    TableFunctionFileCluster::updateStructureAndFormatArgumentsIfNeeded(
        table_function,
        storage_snapshot->metadata->getColumns().getAll().toNamesAndTypesDescription(),
        format_name,
        context
    );
}

RemoteQueryExecutor::Extension StorageFileCluster::getTaskIteratorExtension(
    const ActionsDAG::Node * predicate,
    const ActionsDAG * /* filter */,
    const ContextPtr & context,
    const size_t) const
{
    auto iterator = std::make_shared<StorageFileSource::FilesIterator>(paths, std::nullopt, predicate, getVirtualsList(), context);
    auto next_callback = [iter = std::move(iterator)](size_t) mutable -> ClusterFunctionReadTaskResponsePtr
    {
        auto file = iter->next();
        if (file.empty())
            return std::make_shared<ClusterFunctionReadTaskResponse>();
        return std::make_shared<ClusterFunctionReadTaskResponse>(std::move(file));
    };
    auto callback = std::make_shared<TaskIterator>(std::move(next_callback));
    return RemoteQueryExecutor::Extension{.task_iterator = std::move(callback)};
}

}
