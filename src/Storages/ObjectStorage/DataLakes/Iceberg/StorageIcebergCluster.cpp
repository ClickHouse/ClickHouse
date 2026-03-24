#include <Storages/ObjectStorage/DataLakes/Iceberg/StorageIcebergCluster.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSetQuery.h>
#include <Interpreters/Context.h>

#include <Core/Settings.h>
#include <Core/SettingsEnums.h>
#include <Formats/FormatFactory.h>
#include <Processors/Sources/RemoteSource.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/IPartitionStrategy.h>

#include <Storages/VirtualColumnUtils.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/extractTableFunctionFromSelectQuery.h>
#include <Storages/ObjectStorage/StorageObjectStorageStableTaskDistributor.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool cluster_function_process_archive_on_multiple_nodes;
    extern const SettingsObjectStorageGranularityLevel cluster_table_function_split_granularity;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void StorageDataLakeCluster<IcebergMetadata>::ensureMetadataInitialized(ContextPtr context) const
{
    if (current_metadata)
        return;
    current_metadata = IcebergMetadata::create(object_storage, configuration, context);
}

void StorageDataLakeCluster<IcebergMetadata>::updateMetadata(ContextPtr context) const
{
    if (current_metadata)
    {
        current_metadata->update(context);
        return;
    }
    current_metadata = IcebergMetadata::create(object_storage, configuration, context);
}

StorageDataLakeCluster<IcebergMetadata>::StorageDataLakeCluster(
    const String & cluster_name_,
    StorageObjectStorageConfigurationPtr configuration_,
    ObjectStoragePtr object_storage_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_in_table_or_function_definition,
    const ConstraintsDescription & constraints_,
    const ASTPtr & partition_by,
    ContextPtr context_,
    DataLakeStorageSettingsPtr datalake_settings_,
    bool is_table_function)
    : IStorageCluster(
        cluster_name_, table_id_, getLogger(fmt::format("{}({})", String(IcebergMetadata::name) + configuration_->getEngineName(), table_id_.table_name)))
    , configuration{configuration_}
    , object_storage(object_storage_)
    , datalake_settings(std::move(datalake_settings_))
{
    if (datalake_settings)
        configuration->setDataLakeSettings(datalake_settings);

    /// Ensure trailing slash on the raw path for data lake storages.
    auto path = configuration->getRawPath();
    if (!path.path.ends_with('/'))
        configuration->setRawPath(StorageObjectStorageConfiguration::Path(path.path + "/"));

    configuration->initPartitionStrategy(partition_by, columns_in_table_or_function_definition, context_);
    /// We allow exceptions to be thrown on update(),
    /// because Cluster engine can only be used as table function,
    /// so no lazy initialization is allowed.
    configuration->update(object_storage, context_);

    current_metadata = IcebergMetadata::create(object_storage, configuration, context_);

    ColumnsDescription columns{columns_in_table_or_function_definition};
    std::string sample_path;
    resolveSchemaAndFormat(columns, configuration->format, object_storage, configuration, {}, sample_path, context_);
    configuration->check(context_);

    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns);
    if (is_table_function)
    {
        /// For datalake table functions, always pin the current snapshot version so that
        /// query execution uses the same snapshot as query analysis (logical-race fix).
        /// Additionally reload columns from the snapshot when the per-format setting is enabled.
        if (auto state = current_metadata->getTableStateSnapshot(context_))
        {
            metadata.setDataLakeTableState(*state);
            if (current_metadata->shouldReloadSchemaForConsistency(context_))
            {
                if (auto metadata_snapshot = current_metadata->buildStorageMetadataFromState(*state, context_))
                    metadata = *metadata_snapshot;
            }
        }
    }

    metadata.setConstraints(constraints_);

    setVirtuals(VirtualColumnUtils::getVirtualsForFileLikeStorage(
        metadata.columns,
        context_,
        /* format_settings */std::nullopt,
        configuration->partition_strategy_type,
        sample_path));

    setInMemoryMetadata(metadata);
}

std::string StorageDataLakeCluster<IcebergMetadata>::getName() const
{
    return String(IcebergMetadata::name) + configuration->getEngineName();
}

std::optional<UInt64> StorageDataLakeCluster<IcebergMetadata>::totalRows(ContextPtr query_context) const
{
    ensureMetadataInitialized(query_context);
    return current_metadata->totalRows(query_context);
}

std::optional<UInt64> StorageDataLakeCluster<IcebergMetadata>::totalBytes(ContextPtr query_context) const
{
    ensureMetadataInitialized(query_context);
    return current_metadata->totalBytes(query_context);
}

void StorageDataLakeCluster<IcebergMetadata>::updateQueryToSendIfNeeded(
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
            String(IcebergMetadata::name) + configuration->getEngineName(), query->formatForErrorMessage());
    }

    ASTs & args = expression_list->children;
    const auto & structure = storage_snapshot->metadata->getColumns().getAll().toNamesAndTypesDescription();
    if (args.empty())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unexpected empty list of arguments for {}Cluster table function",
            String(IcebergMetadata::name) + configuration->getEngineName());
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
        configuration->addStructureAndFormatToArgsIfNeeded(args, structure, configuration->format, context, /*with_structure=*/true);
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

void StorageDataLakeCluster<IcebergMetadata>::updateExternalDynamicMetadataIfExists(ContextPtr query_context)
{
    /// Always force an update to pick up the latest snapshot version.
    updateMetadata(query_context);

    auto state = current_metadata->getTableStateSnapshot(query_context);
    if (!state)
        return;

    auto new_metadata = *getInMemoryMetadataPtr();
    new_metadata.setDataLakeTableState(*state);

    if (current_metadata->shouldReloadSchemaForConsistency(query_context))
    {
        if (auto metadata_snapshot = current_metadata->buildStorageMetadataFromState(*state, query_context))
            new_metadata = *metadata_snapshot;
    }

    setInMemoryMetadata(new_metadata);
}

RemoteQueryExecutor::Extension StorageDataLakeCluster<IcebergMetadata>::getTaskIteratorExtension(
    const ActionsDAG::Node * /*predicate*/,
    const ActionsDAG * filter,
    const ContextPtr & local_context,
    ClusterPtr cluster,
    StorageMetadataPtr storage_metadata_snapshot) const
{
    ensureMetadataInitialized(local_context);

    auto iterator = current_metadata->iterate(
        filter,
        local_context->getFileProgressCallback(),
        configuration->getQuerySettings(local_context).list_object_keys_size,
        storage_metadata_snapshot,
        local_context);

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
            auto task = task_distributor->getNextTask(number_of_current_replica);
            if (task)
                return std::make_shared<ClusterFunctionReadTaskResponse>(std::move(task), local_context);
            return std::make_shared<ClusterFunctionReadTaskResponse>();
        });

    return RemoteQueryExecutor::Extension{ .task_iterator = std::move(callback) };
}

}
