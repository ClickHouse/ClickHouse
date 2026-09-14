#include <Storages/ObjectStorage/StorageObjectStorageCluster.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/parseGlobs.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Databases/DatabasesCommon.h>
#include <Databases/DataLake/Common.h>
#include <Databases/DataLake/ICatalog.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/AlterCommands.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Processors/Sources/RemoteSource.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/IPartitionStrategy.h>

#include <Storages/VirtualColumnUtils.h>
#include <Storages/HivePartitioningUtils.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/extractTableFunctionFromSelectQuery.h>
#include <Storages/ObjectStorage/StorageObjectStorageStableTaskDistributor.h>

#include <Common/CurrentThread.h>
#include <Common/FailPoint.h>
#include <base/sleep.h>
namespace DB
{
namespace Setting
{
    extern const SettingsBool iceberg_delete_data_on_drop;
    extern const SettingsBool use_hive_partitioning;
    extern const SettingsBool cluster_function_process_archive_on_multiple_nodes;
    extern const SettingsObjectStorageGranularityLevel cluster_table_function_split_granularity;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace FailPoints
{
    extern const char storage_cluster_read_sleep[];
}

String StorageObjectStorageCluster::getPathSample(ContextPtr context)
{
    const auto path = configuration->getRawPath();

    /// An archive entry is exposed as `<archive path>::<path in archive>` (see `ObjectInfoInArchive::getPath`),
    /// so the sample path can be synthesized the same way as for a plain object as long as the member name is
    /// known. A glob in the member name requires opening the archive to enumerate its entries, but the sample
    /// path is needed only to infer hive partitioning, and `parseHivePartitioningKeysAndValues` looks only at
    /// the directory part of the path - which is fully contained in the outer archive path. So a globbed member
    /// name is simply omitted from the sample instead of disabling the fast path.
    const bool is_archive = configuration->isArchive();
    const bool member_name_is_known = !is_archive || !configuration->isPathInArchiveWithGlobs();
    const String archive_suffix = member_name_is_known && is_archive ? "::" + configuration->getPathInArchive() : "";

    /// For non-glob paths, return directly without any object storage API calls.
    /// Besides saving a request, this keeps hive partition inference working for an explicitly
    /// specified key that does not exist (or is filtered out before reading): the path string
    /// itself carries the partition columns, so it must not depend on the object being present.
    if (!path.hasGlobs())
        return path.path + archive_suffix;

    /// For pure brace expansions, one of the expanded path strings is sufficient to infer
    /// hive partition columns. Avoid probing object metadata, because all explicit keys may
    /// be absent or later filtered out.
    if (containsOnlyEnumGlobs(path.path))
    {
        auto expanded = expandSelectionGlob(path.path);
        if (!expanded.empty())
            return expanded.front() + archive_suffix;
    }

    auto query_settings = configuration->getQuerySettings(context);
    /// We don't want to throw an exception if there are no files with specified path.
    query_settings.throw_on_zero_files_match = false;
    /// For an explicitly specified key, `throw_on_zero_files_match` is not enough: `KeysIterator` probes
    /// the object metadata, and that probe throws for a key that does not exist. Sampling a path is only
    /// needed to infer hive partitioning, so a missing key must leave the sample empty instead of failing
    /// the query during analysis. A key that is really needed for reading is probed again by the reader,
    /// which does report the error.
    query_settings.ignore_non_existent_file = true;
    auto file_iterator = StorageObjectStorageSource::createFileIterator(
        configuration,
        query_settings,
        object_storage,
        nullptr, // storage_metadata
        false, // distributed_processing
        context,
        {}, // predicate
        {},
        {}, // virtual_columns
        {}, // hive_columns
        nullptr, // read_keys
        {} // file_progress_callback
    );

    if (auto file = file_iterator->next(0))
        return file->getPath();
    return "";
}

StorageObjectStorageCluster::StorageObjectStorageCluster(
    const String & cluster_name_,
    StorageObjectStorageConfigurationPtr configuration_,
    ObjectStoragePtr object_storage_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_in_table_or_function_definition,
    const ConstraintsDescription & constraints_,
    const ASTPtr & partition_by,
    ContextPtr context_,
    bool is_table_function,
    std::optional<FormatSettings> format_settings_,
    std::shared_ptr<DataLake::ICatalog> catalog_)
    : IStorageCluster(
        cluster_name_, table_id_, getLogger(fmt::format("{}({})", configuration_->getEngineName(), table_id_.table_name)))
    , configuration{configuration_}
    , object_storage(object_storage_)
    , format_settings(std::move(format_settings_))
    , catalog(std::move(catalog_))
{
    configuration->initPartitionStrategy(partition_by, columns_in_table_or_function_definition, context_);
    configuration->check(context_);
    /// We allow exceptions to be thrown on update(),
    /// because Cluster engine can only be used as table function,
    /// so no lazy initialization is allowed.
    configuration->update(object_storage, context_);

    ColumnsDescription columns{columns_in_table_or_function_definition};
    std::string sample_path;
    resolveSchemaAndFormat(columns, configuration->format, object_storage, configuration, {}, sample_path, context_);

    if (sample_path.empty()
        && context_->getSettingsRef()[Setting::use_hive_partitioning]
        && !configuration->isDataLakeConfiguration()
        && !configuration->partition_strategy)
        sample_path = getPathSample(context_);

    /// Not grabbing the file_columns because it is not necessary to do it here.
    std::tie(hive_partition_columns_to_read_from_file_path, std::ignore) = HivePartitioningUtils::setupHivePartitioningForObjectStorage(
        columns,
        configuration,
        sample_path,
        columns_in_table_or_function_definition.empty(),
        std::nullopt,
        context_);

    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns);
    if (is_table_function && configuration->isDataLakeConfiguration())
    {
        /// For datalake table functions, always pin the current snapshot version so that
        /// query execution uses the same snapshot as query analysis (logical-race fix).
        /// Additionally reload columns from the snapshot when the per-format setting is enabled.
        if (auto state = configuration->getTableStateSnapshot(context_))
        {
            metadata.setDataLakeTableState(*state);
            if (configuration->shouldReloadSchemaForConsistency(context_))
            {
                if (auto metadata_snapshot = configuration->buildStorageMetadataFromState(*state, context_))
                    metadata = *metadata_snapshot;
            }
        }
    }

    metadata.setConstraints(constraints_);
    metadata.setVirtuals(VirtualColumnUtils::getVirtualsForFileLikeStorage(
        metadata.columns,
        context_,
        /* format_settings */std::nullopt,
        configuration->partition_strategy_type,
        sample_path));

    setInMemoryMetadata(metadata);
}

std::string StorageObjectStorageCluster::getName() const
{
    return configuration->getEngineName();
}

SinkToStoragePtr StorageObjectStorageCluster::write(
    const ASTPtr &,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr local_context,
    bool /* async_insert */)
{
    if (!configuration->isDataLakeConfiguration())
        configuration->update(object_storage, local_context);

    return StorageObjectStorage::createSink(
        configuration, object_storage, getStorageID(), format_settings, catalog, metadata_snapshot, local_context);
}

bool StorageObjectStorageCluster::supportsParallelInsert() const
{
    if (configuration->isDataLakeConfiguration())
        configuration->lazyInitializeIfNeeded(object_storage, CurrentThread::tryGetQueryContext());
    return configuration->supportsParallelInsert();
}

bool StorageObjectStorageCluster::supportsDelete() const
{
    if (configuration->isDataLakeConfiguration())
        configuration->lazyInitializeIfNeeded(object_storage, CurrentThread::tryGetQueryContext());
    return configuration->supportsDelete();
}

bool StorageObjectStorageCluster::optimize(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & metadata_snapshot,
    const ASTPtr & /*partition*/,
    bool /*final*/,
    bool /*deduplicate*/,
    const Names & /*deduplicate_by_columns*/,
    bool /*cleanup*/,
    ContextPtr context)
{
    return configuration->optimize(object_storage, metadata_snapshot, context, format_settings);
}

void StorageObjectStorageCluster::mutate(const MutationCommands & commands, ContextPtr context)
{
    updateExternalDynamicMetadataIfExists(context);
    auto metadata_snapshot = getInMemoryMetadataPtr(context, false);
    configuration->mutate(commands, context, shared_from_this(), getStorageID(), metadata_snapshot, catalog, format_settings);
}

void StorageObjectStorageCluster::checkMutationIsPossible(const MutationCommands & commands, const Settings & /*settings*/) const
{
    configuration->checkMutationIsPossible(object_storage, CurrentThread::tryGetQueryContext(), commands);
}

void StorageObjectStorageCluster::alter(const AlterCommands & params, ContextPtr context, AlterLockHolder & /*alter_lock_holder*/)
{
    auto metadata_snapshot = getInMemoryMetadataPtr(context, false);
    StorageInMemoryMetadata new_metadata = *metadata_snapshot;
    params.apply(new_metadata, context);

    checkMetadataDoesNotExceedMaxQuerySize(getStorageID(), new_metadata, context);

    configuration->alter(object_storage, params, context, getStorageID(), catalog);

    if (catalog)
        return;

    const auto storage_id = getStorageID();
    DatabaseCatalog::instance()
        .getDatabase(storage_id.database_name)
        ->alterTable(context, storage_id, new_metadata, /*validate_new_create_query=*/true);
    setInMemoryMetadata(new_metadata);
}

void StorageObjectStorageCluster::checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const
{
    configuration->checkAlterIsPossible(object_storage, context, commands);
}

Pipe StorageObjectStorageCluster::executeCommand(const String & command_name, const ASTPtr & args, ContextPtr context)
{
    if (!configuration->isDataLakeConfiguration())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "EXECUTE command '{}' is not supported by this storage", command_name);

    configuration->update(object_storage, context);
    auto metadata = configuration->getExternalMetadata();
    if (!metadata)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "EXECUTE command '{}' is not supported by this storage", command_name);

    return metadata->executeCommand(command_name, args, object_storage, configuration, catalog, context, getStorageID());
}

void StorageObjectStorageCluster::drop()
{
    /// We cannot use query context here, because drop is executed in the background.
    auto drop_context = Context::getGlobalContextInstance();
    if (catalog)
    {
        const auto [namespace_name, table_name] = DataLake::parseTableName(getStorageID().getTableName());
        catalog->dropTable(namespace_name, table_name, drop_context->getSettingsRef()[Setting::iceberg_delete_data_on_drop]);
    }
    configuration->drop(drop_context);
}

std::optional<UInt64> StorageObjectStorageCluster::totalRows(ContextPtr query_context) const
{
    configuration->lazyInitializeIfNeeded(
        object_storage,
        query_context);
    return configuration->totalRows(query_context);
}

std::optional<UInt64> StorageObjectStorageCluster::totalBytes(ContextPtr query_context) const
{
    configuration->lazyInitializeIfNeeded(
        object_storage,
        query_context);
    return configuration->totalBytes(query_context);
}

void StorageObjectStorageCluster::updateQueryToSendIfNeeded(
    ASTPtr & query,
    const DB::StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context)
{
    auto * table_function = extractTableFunctionFromSelectQuery(query);
    if (!table_function)
        return;
    auto * expression_list = table_function->arguments->as<ASTExpressionList>();
    if (!expression_list)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected SELECT query from table function {}, got '{}'",
            configuration->getEngineName(), query->formatForErrorMessage());
    }

    ASTs & args = expression_list->children;
    const auto & structure = storage_snapshot->metadata->getColumns().getAll().toNamesAndTypesDescription();
    if (args.empty())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unexpected empty list of arguments for {}Cluster table function",
            configuration->getEngineName());
    }

    ASTPtr settings_temporary_storage = nullptr;
    for (auto it = args.begin(); it != args.end(); ++it)
    {
        ASTSetQuery * settings_ast = (*it)->as<ASTSetQuery>();
        if (settings_ast)
        {
            settings_temporary_storage = std::move(*it);
            args.erase(it);
            break;
        }
    }

    if (!endsWith(table_function->name, "Cluster"))
    {
        configuration->addStructureAndFormatToArgsIfNeeded(args, structure, configuration->format, context, /*with_structure=*/true);

        /// When a non-cluster table function (e.g. `s3`) was auto-converted to cluster mode
        /// by the `parallel_replicas_for_cluster_engines` setting, rename it to the Cluster variant
        /// (e.g. `s3Cluster`) and prepend the cluster name argument. This ensures that on the shard,
        /// `TableFunctionObjectStorageCluster::executeImpl` is called, which correctly handles
        /// `distributed_processing` for task-based file distribution from the initiator.
        ///
        /// Some table functions (e.g. `paimonLocal`, `deltaLakeLocal`) do not have a Cluster variant,
        /// so we only rename when the target function actually exists.
        const String cluster_function_name = table_function->name + "Cluster";
        if (TableFunctionFactory::instance().isTableFunctionName(cluster_function_name))
        {
            args.insert(args.begin(), make_intrusive<ASTLiteral>(getClusterName()));
            table_function->name = cluster_function_name;
        }
    }
    else
    {
        ASTPtr cluster_name_arg = args.front();
        args.erase(args.begin());
        configuration->addStructureAndFormatToArgsIfNeeded(args, structure, configuration->format, context, /*with_structure=*/true);
        args.insert(args.begin(), cluster_name_arg);
    }
    if (settings_temporary_storage)
    {
        args.insert(args.end(), std::move(settings_temporary_storage));
    }
}

void StorageObjectStorageCluster::updateExternalDynamicMetadataIfExists(ContextPtr query_context)
{
    if (!configuration->isDataLakeConfiguration())
        return;

    /// Always force an update to pick up the latest snapshot version.
    /// Using if_not_updated_before=true would leave latest_snapshot_version
    /// stale from the first query and silently omit new files.
    configuration->update(
        object_storage,
        query_context);

    auto state = configuration->getTableStateSnapshot(query_context);
    if (!state)
        return;

    auto current_metadata = getInMemoryMetadataPtr(query_context, false);
    auto new_metadata = *current_metadata;
    new_metadata.setDataLakeTableState(*state);

    if (configuration->shouldReloadSchemaForConsistency(query_context))
    {
        if (auto metadata_snapshot = configuration->buildStorageMetadataFromState(*state, query_context))
            new_metadata = *metadata_snapshot;
    }

    setInMemoryMetadata(new_metadata.withVirtuals(VirtualColumnUtils::getVirtualsForFileLikeStorage(
        new_metadata.columns,
        query_context,
        /* format_settings */ std::nullopt,
        configuration->partition_strategy_type)));
}

RemoteQueryExecutor::Extension StorageObjectStorageCluster::getTaskIteratorExtension(
    const ActionsDAG::Node * predicate,
    const ActionsDAG * filter,
    const ContextPtr & local_context,
    ClusterPtr cluster,
    StorageMetadataPtr storage_metadata_snapshot) const
{
    auto iterator = StorageObjectStorageSource::createFileIterator(
        configuration,
        configuration->getQuerySettings(local_context),
        object_storage,
        storage_metadata_snapshot,
        /* distributed_processing */ false,
        local_context,
        predicate,
        filter,
        storage_metadata_snapshot->virtuals.getSampleBlock(VirtualsKind::All, VirtualsMaterializationPlace::Reader).getNamesAndTypesList(),
        hive_partition_columns_to_read_from_file_path,
        nullptr,
        local_context->getFileProgressCallback(),
        /*ignore_archive_globs=*/false,
        /*skip_object_metadata=*/true);

    if (local_context->getSettingsRef()[Setting::cluster_table_function_split_granularity] == ObjectStorageGranularityLevel::BUCKET)
    {
        iterator = std::make_shared<ObjectIteratorSplitByBuckets>(
            std::move(iterator),
            configuration->format,
            object_storage,
            local_context
        );
    }

    std::vector<std::string> ids_of_hosts;
    for (const auto & shard : cluster->getShardsInfo())
    {
        if (shard.per_replica_pools.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cluster {} with empty shard {}", cluster->getName(), shard.shard_num);
        for (const auto & replica : shard.per_replica_pools)
        {
            if (!replica)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cluster {}, shard {} with empty node", cluster->getName(), shard.shard_num);
            ids_of_hosts.push_back(replica->getAddress());
        }
    }

    auto task_distributor = std::make_shared<StorageObjectStorageStableTaskDistributor>(
        iterator,
        std::move(ids_of_hosts),
        /* send_over_whole_archive */!local_context->getSettingsRef()[Setting::cluster_function_process_archive_on_multiple_nodes]);

    auto callback = std::make_shared<TaskIterator>(
        [task_distributor, local_context](size_t number_of_current_replica) mutable -> ClusterFunctionReadTaskResponsePtr
        {
            fiu_do_on(FailPoints::storage_cluster_read_sleep,
            {
                sleepForSeconds(10);
            });

            auto task = task_distributor->getNextTask(number_of_current_replica);
            if (task)
                return std::make_shared<ClusterFunctionReadTaskResponse>(std::move(task), local_context);
            return std::make_shared<ClusterFunctionReadTaskResponse>();
        });

    return RemoteQueryExecutor::Extension{ .task_iterator = std::move(callback) };
}

}
