#include <Storages/ObjectStorage/ClusterUtils.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSetQuery.h>

#include <Core/Settings.h>
#include <Core/SettingsEnums.h>
#include <Interpreters/Context.h>
#include <Interpreters/ClusterFunctionReadTask.h>
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

void updateClusterQueryToSendIfNeeded(
    ASTPtr & query,
    const StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context,
    const ObjectStorageConnectionConfigurationPtr & configuration,
    const StorageObjectStorageTableOptions & table_options,
    const String & engine_name)
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
            engine_name, query->formatForErrorMessage());
    }

    ASTs & args = expression_list->children;
    const auto & structure = storage_snapshot->metadata->getColumns().getAll().toNamesAndTypesDescription();
    if (args.empty())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unexpected empty list of arguments for {}Cluster table function",
            engine_name);
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
        configuration->addStructureAndFormatToArgsIfNeeded(args, structure, table_options.format, context, /*with_structure=*/true);
    else
    {
        ASTPtr cluster_name_arg = args.front();
        args.erase(args.begin());
        configuration->addStructureAndFormatToArgsIfNeeded(args, structure, table_options.format, context, /*with_structure=*/true);
        args.insert(args.begin(), cluster_name_arg);
    }
    if (settings_temporary_storage)
    {
        args.insert(args.end(), std::move(settings_temporary_storage));
    }
}

RemoteQueryExecutor::Extension buildClusterTaskIteratorExtension(
    std::shared_ptr<IObjectIterator> iterator,
    const StorageObjectStorageTableOptions & table_options,
    const ObjectStoragePtr & object_storage,
    const ContextPtr & local_context,
    ClusterPtr cluster)
{
    if (local_context->getSettingsRef()[Setting::cluster_table_function_split_granularity] == ObjectStorageGranularityLevel::BUCKET)
    {
        iterator = std::make_shared<ObjectIteratorSplitByBuckets>(
            std::move(iterator),
            table_options.format,
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
