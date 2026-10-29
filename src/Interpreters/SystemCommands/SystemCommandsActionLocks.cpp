#include <Interpreters/SystemCommands/SystemCommandsActionLocks.h>
#include <Interpreters/SystemCommands/SystemCommandHelpers.h>

#include <Access/ContextAccess.h>
#include <Databases/IDatabase.h>
#include <Interpreters/ActionLocksManager.h>
#include <Storages/IStorage.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace ActionLocks
{
    extern const StorageActionBlockType PartsMerge;
    extern const StorageActionBlockType PartsFetch;
    extern const StorageActionBlockType PartsSend;
    extern const StorageActionBlockType ReplicationQueue;
    extern const StorageActionBlockType DistributedSend;
    extern const StorageActionBlockType PartsTTLMerge;
    extern const StorageActionBlockType PartsMove;
    extern const StorageActionBlockType PullReplicationLog;
    extern const StorageActionBlockType Cleanup;
    extern const StorageActionBlockType ViewRefresh;
    extern const StorageActionBlockType ViewRefreshPause;
    extern const StorageActionBlockType StreamConsume;
}

namespace
{

AccessType getRequiredAccessType(StorageActionBlockType action_type)
{
    if (action_type == ActionLocks::PartsMerge)
        return AccessType::SYSTEM_MERGES;
    if (action_type == ActionLocks::PartsFetch)
        return AccessType::SYSTEM_FETCHES;
    if (action_type == ActionLocks::PartsSend)
        return AccessType::SYSTEM_REPLICATED_SENDS;
    if (action_type == ActionLocks::ReplicationQueue)
        return AccessType::SYSTEM_REPLICATION_QUEUES;
    if (action_type == ActionLocks::DistributedSend)
        return AccessType::SYSTEM_DISTRIBUTED_SENDS;
    if (action_type == ActionLocks::PartsTTLMerge)
        return AccessType::SYSTEM_TTL_MERGES;
    if (action_type == ActionLocks::PartsMove)
        return AccessType::SYSTEM_MOVES;
    if (action_type == ActionLocks::PullReplicationLog)
        return AccessType::SYSTEM_PULLING_REPLICATION_LOG;
    if (action_type == ActionLocks::Cleanup)
        return AccessType::SYSTEM_CLEANUP;
    if (action_type == ActionLocks::ViewRefresh)
        return AccessType::SYSTEM_VIEWS;
    if (action_type == ActionLocks::ViewRefreshPause)
        return AccessType::SYSTEM_VIEWS;
    if (action_type == ActionLocks::StreamConsume)
        return AccessType::SYSTEM_STREAMING_ENGINES;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown action type: {}", std::to_string(action_type));
}

}

void startStopAction(SystemCommandContext & context, StorageActionBlockType action_type, bool start)
{
    auto manager = context.getContext()->getActionLocksManager();
    manager->cleanExpired();

    auto access = context.getContext()->getAccess();
    auto required_access_type = getRequiredAccessType(action_type);

    if (context.volume_ptr && action_type == ActionLocks::PartsMerge)
    {
        access->checkAccess(required_access_type);
        context.volume_ptr->setAvoidMergesUserOverride(!start);
    }
    else if (context.table_id)
    {
        access->checkAccess(required_access_type, context.table_id.database_name, context.table_id.table_name);
        auto table = DatabaseCatalog::instance().tryGetTable(context.table_id, context.getContext());
        if (table)
        {
            if (start)
            {
                manager->remove(table, action_type);
                table->onActionLockRemove(action_type);
            }
            else
                manager->add(table, action_type);
        }
    }
    else
    {
        for (auto & elem : DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false}))
        {
            startStopActionInDatabase(action_type, start, elem.first, elem.second, context.getContext(), context.log);
        }
    }
}

void startStopActionInDatabase(StorageActionBlockType action_type, bool start,
                               const String & database_name, const DatabasePtr & database,
                               const ContextPtr & local_context, LoggerPtr log)
{
    auto manager = local_context->getActionLocksManager();
    auto access = local_context->getAccess();
    auto required_access_type = getRequiredAccessType(action_type);

    for (auto iterator = database->getTablesIterator(local_context); iterator->isValid(); iterator->next())
    {
        StoragePtr table = iterator->table();
        if (!table)
            continue;

        if (!access->isGranted(required_access_type, database_name, iterator->name()))
        {
            LOG_INFO(log, "Access {} denied, skipping {}.{}", toString(required_access_type), database_name, iterator->name());
            continue;
        }

        if (start)
        {
            manager->remove(table, action_type);
            table->onActionLockRemove(action_type);
        }
        else
            manager->add(table, action_type);
    }
}

void registerSystemCommandsActionLocks(SystemCommandFactory & factory)
{
    using Type = ASTSystemQuery::Type;

    auto register_start_stop = [&](Type stop_type, Type start_type, StorageActionBlockType action_type)
    {
        const auto access_on_cluster = tableOrGlobalAccessOnCluster(getRequiredAccessType(action_type));
        for (const bool start : {false, true})
        {
            factory.registerCommand(
                start ? start_type : stop_type,
                access_on_cluster,
                [action_type, start](ASTSystemQuery &, SystemCommandContext & context)
                {
                    startStopAction(context, action_type, start);
                    return BlockIO{};
                });
        }
    };

    register_start_stop(Type::STOP_MERGES, Type::START_MERGES, ActionLocks::PartsMerge);
    register_start_stop(Type::STOP_TTL_MERGES, Type::START_TTL_MERGES, ActionLocks::PartsTTLMerge);
    register_start_stop(Type::STOP_MOVES, Type::START_MOVES, ActionLocks::PartsMove);
    register_start_stop(Type::STOP_FETCHES, Type::START_FETCHES, ActionLocks::PartsFetch);
    register_start_stop(Type::STOP_REPLICATED_SENDS, Type::START_REPLICATED_SENDS, ActionLocks::PartsSend);
    register_start_stop(Type::STOP_REPLICATION_QUEUES, Type::START_REPLICATION_QUEUES, ActionLocks::ReplicationQueue);
    register_start_stop(Type::STOP_DISTRIBUTED_SENDS, Type::START_DISTRIBUTED_SENDS, ActionLocks::DistributedSend);
    register_start_stop(Type::STOP_PULLING_REPLICATION_LOG, Type::START_PULLING_REPLICATION_LOG, ActionLocks::PullReplicationLog);
    register_start_stop(Type::STOP_CLEANUP, Type::START_CLEANUP, ActionLocks::Cleanup);
}

}
