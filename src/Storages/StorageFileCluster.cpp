#include <Access/ContextAccess.h>
#include <Access/Common/AccessFlags.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/ClusterFunctionReadTask.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
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
#include <Storages/HivePartitioningUtils.h>
#include <Core/Settings.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsBool use_hive_partitioning;
    extern const SettingsString rename_files_after_processing;
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

    /// The archive syntax (e.g. "archive*.zip::file.parquet") is not supported by function fileCluster() yet.
    auto file_source = StorageFile::FileSource::parse(filename_, context, /* allow_archive_path_syntax = */ false);
    paths = std::move(file_source.paths);
    user_files_volume = std::move(file_source.user_files_volume);

    if (columns_.empty())
    {
        ColumnsDescription columns;
        if (format_name == "auto")
            std::tie(columns, format_name) = StorageFile::getTableStructureAndFormatFromFile(
                paths, compression_method, std::nullopt, context, std::nullopt, user_files_volume);
        else
            columns = StorageFile::getTableStructureFromFile(
                format_name, paths, compression_method, std::nullopt, context, std::nullopt, user_files_volume);

        storage_metadata.setColumns(columns);
    }
    else
    {
        if (format_name == "auto")
            format_name = StorageFile::getTableStructureAndFormatFromFile(
                paths, compression_method, std::nullopt, context, std::nullopt, user_files_volume).second;
        storage_metadata.setColumns(columns_);
    }

    auto & storage_columns = storage_metadata.columns;

    /// Not grabbing the file_columns because it is not necessary to do it here.
    std::tie(hive_partition_columns_to_read_from_file_path, std::ignore) = HivePartitioningUtils::setupHivePartitioningForFileURLLikeStorage(
        storage_columns,
        paths.empty() ? "" : StorageFile::getUserFilesVirtualPath(paths.front(), user_files_volume),
        columns_.empty(),
        std::nullopt,
        context);

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setVirtuals(VirtualColumnUtils::getVirtualsForFileLikeStorage(storage_metadata.columns, context));
    setInMemoryMetadata(storage_metadata);
}

namespace
{

/// The workers rename the files they read, so the user who asks for it must be allowed to write here,
/// on the node it authenticated to: without a cluster secret a secondary query is authorized as the
/// cluster's configured user, not as the user who issued this query.
void checkWriteAccessIfFilesAreRenamed(const ContextPtr & context)
{
    if (!context->getSettingsRef()[Setting::rename_files_after_processing].value.empty())
        context->getAccess()->checkAccessWithFilter(
            AccessType::WRITE, toStringSource(AccessTypeObjects::Source::FILE), /* filter */ "");
}

}

void StorageFileCluster::updateBeforeRead(const ContextPtr & context)
{
    checkWriteAccessIfFilesAreRenamed(context);
}

void StorageFileCluster::updateQueryToSendIfNeeded(
    DB::ASTPtr & query,
    const StorageSnapshotPtr & storage_snapshot,
    const DB::ContextPtr & context,
    const String & target_cluster_name)
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

    /// `fileCluster` has no plain counterpart that `parallel_replicas_for_cluster_engines` could convert, so
    /// the function is always already the `*Cluster` variant and carries a cluster name the user wrote.
    /// Replace it with the cluster whose nodes will actually run the query - the two differ when the
    /// destination drives the fan-out (`INSERT INTO <Distributed table> SELECT`), and those nodes reject a
    /// name their own `remote_servers` does not define even though they take their share of the work from
    /// the initiator rather than dispatching by it.
    auto * expression_list = table_function->arguments->as<ASTExpressionList>();
    if (!expression_list || expression_list->children.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected SELECT query from table function fileCluster, got '{}'", query->formatForErrorMessage());
    if (!target_cluster_name.empty())
        expression_list->children.front() = make_intrusive<ASTLiteral>(target_cluster_name);
}

namespace
{

bool predicateUsesColumn(const ActionsDAG::Node * predicate, const String & column_name)
{
    std::unordered_set<const ActionsDAG::Node *> visited;
    std::vector<const ActionsDAG::Node *> nodes{predicate};
    while (!nodes.empty())
    {
        const auto * node = nodes.back();
        nodes.pop_back();
        if (!visited.insert(node).second)
            continue;
        if (node->type == ActionsDAG::ActionType::INPUT && node->result_name == column_name)
            return true;
        for (const auto * child : node->children)
            nodes.push_back(child);
    }
    return false;
}

}

RemoteQueryExecutor::Extension StorageFileCluster::getTaskIteratorExtension(
    const ActionsDAG::Node * predicate, const ActionsDAG * /* filter */, const ContextPtr & context, ClusterPtr, StorageMetadataPtr metadata) const
{
    /// A distributed `INSERT ... SELECT` hands the workers their tasks from here without going
    /// through `IStorageCluster::read`, so this is the one place every path shares.
    checkWriteAccessIfFilesAreRenamed(context);

    /// `_path` is materialized by the worker that reads the file, and its value depends on the configuration of that
    /// worker: the root of a plain local `user_files_policy` disk, a disk-relative path on other disks, or the absolute
    /// path without a policy. This server cannot know it, so the files are not filtered here by a predicate on `_path`:
    /// the workers materialize `_path` and the query filters by it. Other predicates, e.g. on `_file`, still prune.
    const bool path_depends_on_server = predicate && predicateUsesColumn(predicate, "_path");
    auto iterator = std::make_shared<StorageFileSource::FilesIterator>(paths, std::nullopt, path_depends_on_server ? nullptr : predicate, metadata->virtuals.getSampleBlock(VirtualsKind::All, VirtualsMaterializationPlace::Reader).getNamesAndTypesList(), hive_partition_columns_to_read_from_file_path, context,
        /*distributed_processing_=*/ false, /*archive_member_path_=*/ String{}, user_files_volume);
    auto next_callback = [iter = std::move(iterator), volume = user_files_volume](size_t) mutable -> ClusterFunctionReadTaskResponsePtr
    {
        auto file = iter->next();
        if (file.empty())
            return std::make_shared<ClusterFunctionReadTaskResponse>();
        /// With a `user_files_policy` volume `file` is prefixed with the root of the disk of this
        /// server, which can differ on the workers. Send the disk-relative path instead: each
        /// worker resolves it against its own disk in `StorageFileSource::FilesIterator::next`.
        if (volume)
        {
            auto [disk, relative_path] = splitUserFilesAbsolutePath(file, volume->getDisks());
            if (!disk)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Path {} is not on the user files disk", file);
            file = std::move(relative_path);
        }
        return std::make_shared<ClusterFunctionReadTaskResponse>(std::move(file));
    };
    auto callback = std::make_shared<TaskIterator>(std::move(next_callback));
    return RemoteQueryExecutor::Extension{.task_iterator = std::move(callback)};
}

}
