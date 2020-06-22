#include <Interpreters/InterpreterSystemQuery.h>
#include <Common/DNSResolver.h>
#include <Common/ActionLock.h>
#include <Common/typeid_cast.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/ThreadPool.h>
#include <Common/escapeForFileName.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/EmbeddedDictionaries.h>
#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/QueryThreadLog.h>
#include <Interpreters/TraceLog.h>
#include <Interpreters/TextLog.h>
#include <Interpreters/MetricLog.h>
#include <Interpreters/AsynchronousMetricLog.h>
#include <Access/ContextAccess.h>
#include <Access/AllowedClientHosts.h>
#include <Databases/IDatabase.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/StorageFactory.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <csignal>
#include <algorithm>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif


namespace DB
{


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_KILL;
    extern const int NOT_IMPLEMENTED;
    extern const int TIMEOUT_EXCEEDED;
}


namespace ActionLocks
{
    extern StorageActionBlockType PartsMerge;
    extern StorageActionBlockType PartsFetch;
    extern StorageActionBlockType PartsSend;
    extern StorageActionBlockType ReplicationQueue;
    extern StorageActionBlockType DistributedSend;
    extern StorageActionBlockType PartsTTLMerge;
    extern StorageActionBlockType PartsMove;
}


namespace
{

ExecutionStatus getOverallExecutionStatusOfCommands()
{
    return ExecutionStatus(0);
}

/// Consequently tries to execute all commands and generates final exception message for failed commands
template <typename Callable, typename ... Callables>
ExecutionStatus getOverallExecutionStatusOfCommands(Callable && command, Callables && ... commands)
{
    ExecutionStatus status_head(0);
    try
    {
        command();
    }
    catch (...)
    {
        status_head = ExecutionStatus::fromCurrentException();
    }

    ExecutionStatus status_tail = getOverallExecutionStatusOfCommands(std::forward<Callables>(commands)...);

    auto res_status = status_head.code != 0 ? status_head.code : status_tail.code;
    auto res_message = status_head.message + (status_tail.message.empty() ? "" : ("\n" + status_tail.message));

    return ExecutionStatus(res_status, res_message);
}

/// Consequently tries to execute all commands and throws exception with info about failed commands
template <typename ... Callables>
void executeCommandsAndThrowIfError(Callables && ... commands)
{
    auto status = getOverallExecutionStatusOfCommands(std::forward<Callables>(commands)...);
    if (status.code != 0)
        throw Exception(status.message, status.code);
}


AccessType getRequiredAccessType(StorageActionBlockType action_type)
{
    if (action_type == ActionLocks::PartsMerge)
        return AccessType::SYSTEM_MERGES;
    else if (action_type == ActionLocks::PartsFetch)
        return AccessType::SYSTEM_FETCHES;
    else if (action_type == ActionLocks::PartsSend)
        return AccessType::SYSTEM_REPLICATED_SENDS;
    else if (action_type == ActionLocks::ReplicationQueue)
        return AccessType::SYSTEM_REPLICATION_QUEUES;
    else if (action_type == ActionLocks::DistributedSend)
        return AccessType::SYSTEM_DISTRIBUTED_SENDS;
    else if (action_type == ActionLocks::PartsTTLMerge)
        return AccessType::SYSTEM_TTL_MERGES;
    else if (action_type == ActionLocks::PartsMove)
        return AccessType::SYSTEM_MOVES;
    else
        throw Exception("Unknown action type: " + std::to_string(action_type), ErrorCodes::LOGICAL_ERROR);
}

}

/// Implements SYSTEM [START|STOP] <something action from ActionLocks>
void InterpreterSystemQuery::startStopAction(StorageActionBlockType action_type, bool start)
{
    auto manager = context.getActionLocksManager();
    manager->cleanExpired();

    if (table_id)
    {
        context.checkAccess(getRequiredAccessType(action_type), table_id);
        if (start)
            manager->remove(table_id, action_type);
        else
            manager->add(table_id, action_type);
    }
    else
    {
        auto access = context.getAccess();
        for (auto & elem : DatabaseCatalog::instance().getDatabases())
        {
            for (auto iterator = elem.second->getTablesIterator(context); iterator->isValid(); iterator->next())
            {
                StoragePtr table = iterator->table();
                if (!table)
                    continue;

                if (!access->isGranted(log, getRequiredAccessType(action_type), elem.first, iterator->name()))
                    continue;

                if (start)
                    manager->remove(table, action_type);
                else
                    manager->add(table, action_type);
            }
        }
    }
}


InterpreterSystemQuery::InterpreterSystemQuery(const ASTPtr & query_ptr_, Context & context_)
        : query_ptr(query_ptr_->clone()), context(context_), log(&Poco::Logger::get("InterpreterSystemQuery"))
{
}


BlockIO InterpreterSystemQuery::execute()
{
    auto & query = query_ptr->as<ASTSystemQuery &>();

    if (!query.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, context, getRequiredAccessForDDLOnCluster());

    using Type = ASTSystemQuery::Type;

    /// Use global context with fresh system profile settings
    Context system_context = context.getGlobalContext();
    system_context.setSetting("profile", context.getSystemProfileName());

    /// Make canonical query for simpler processing
    if (!query.table.empty())
         table_id = context.resolveStorageID(StorageID(query.database, query.table), Context::ResolveOrdinary);

    if (!query.target_dictionary.empty() && !query.database.empty())
        query.target_dictionary = query.database + "." + query.target_dictionary;

    switch (query.type)
    {
        case Type::SHUTDOWN:
            context.checkAccess(AccessType::SYSTEM_SHUTDOWN);
            if (kill(0, SIGTERM))
                throwFromErrno("System call kill(0, SIGTERM) failed", ErrorCodes::CANNOT_KILL);
            break;
        case Type::KILL:
            context.checkAccess(AccessType::SYSTEM_SHUTDOWN);
            if (kill(0, SIGKILL))
                throwFromErrno("System call kill(0, SIGKILL) failed", ErrorCodes::CANNOT_KILL);
            break;
        case Type::DROP_DNS_CACHE:
            context.checkAccess(AccessType::SYSTEM_DROP_DNS_CACHE);
            DNSResolver::instance().dropCache();
            /// Reinitialize clusters to update their resolved_addresses
            system_context.reloadClusterConfig();
            break;
        case Type::DROP_MARK_CACHE:
            context.checkAccess(AccessType::SYSTEM_DROP_MARK_CACHE);
            system_context.dropMarkCache();
            break;
        case Type::DROP_UNCOMPRESSED_CACHE:
            context.checkAccess(AccessType::SYSTEM_DROP_UNCOMPRESSED_CACHE);
            system_context.dropUncompressedCache();
            break;
#if USE_EMBEDDED_COMPILER
        case Type::DROP_COMPILED_EXPRESSION_CACHE:
            context.checkAccess(AccessType::SYSTEM_DROP_COMPILED_EXPRESSION_CACHE);
            system_context.dropCompiledExpressionCache();
            break;
#endif
        case Type::RELOAD_DICTIONARY:
            context.checkAccess(AccessType::SYSTEM_RELOAD_DICTIONARY);
            system_context.getExternalDictionariesLoader().loadOrReload(query.target_dictionary);
            ExternalDictionariesLoader::resetAll();
            break;
        case Type::RELOAD_DICTIONARIES:
            context.checkAccess(AccessType::SYSTEM_RELOAD_DICTIONARY);
            executeCommandsAndThrowIfError(
                    [&] () { system_context.getExternalDictionariesLoader().reloadAllTriedToLoad(); },
                    [&] () { system_context.getEmbeddedDictionaries().reload(); }
            );
            ExternalDictionariesLoader::resetAll();
            break;
        case Type::RELOAD_EMBEDDED_DICTIONARIES:
            context.checkAccess(AccessType::SYSTEM_RELOAD_EMBEDDED_DICTIONARIES);
            system_context.getEmbeddedDictionaries().reload();
            break;
        case Type::RELOAD_CONFIG:
            context.checkAccess(AccessType::SYSTEM_RELOAD_CONFIG);
            system_context.reloadConfig();
            break;
        case Type::STOP_MERGES:
            startStopAction(ActionLocks::PartsMerge, false);
            break;
        case Type::START_MERGES:
            startStopAction(ActionLocks::PartsMerge, true);
            break;
        case Type::STOP_TTL_MERGES:
            startStopAction(ActionLocks::PartsTTLMerge, false);
            break;
        case Type::START_TTL_MERGES:
            startStopAction(ActionLocks::PartsTTLMerge, true);
            break;
        case Type::STOP_MOVES:
            startStopAction(ActionLocks::PartsMove, false);
            break;
        case Type::START_MOVES:
            startStopAction(ActionLocks::PartsMove, true);
            break;
        case Type::STOP_FETCHES:
            startStopAction(ActionLocks::PartsFetch, false);
            break;
        case Type::START_FETCHES:
            startStopAction(ActionLocks::PartsFetch, true);
            break;
        case Type::STOP_REPLICATED_SENDS:
            startStopAction(ActionLocks::PartsSend, false);
            break;
        case Type::START_REPLICATED_SENDS:
            startStopAction(ActionLocks::PartsSend, true);
            break;
        case Type::STOP_REPLICATION_QUEUES:
            startStopAction(ActionLocks::ReplicationQueue, false);
            break;
        case Type::START_REPLICATION_QUEUES:
            startStopAction(ActionLocks::ReplicationQueue, true);
            break;
        case Type::STOP_DISTRIBUTED_SENDS:
            startStopAction(ActionLocks::DistributedSend, false);
            break;
        case Type::START_DISTRIBUTED_SENDS:
            startStopAction(ActionLocks::DistributedSend, true);
            break;
        case Type::SYNC_REPLICA:
            syncReplica(query);
            break;
        case Type::FLUSH_DISTRIBUTED:
            flushDistributed(query);
            break;
        case Type::RESTART_REPLICAS:
            restartReplicas(system_context);
            break;
        case Type::RESTART_REPLICA:
            if (!tryRestartReplica(table_id, system_context))
                throw Exception("There is no " + query.database + "." + query.table + " replicated table",
                                ErrorCodes::BAD_ARGUMENTS);
            break;
        case Type::FLUSH_LOGS:
            context.checkAccess(AccessType::SYSTEM_FLUSH_LOGS);
            executeCommandsAndThrowIfError(
                    [&] () { if (auto query_log = context.getQueryLog()) query_log->flush(true); },
                    [&] () { if (auto part_log = context.getPartLog("")) part_log->flush(true); },
                    [&] () { if (auto query_thread_log = context.getQueryThreadLog()) query_thread_log->flush(true); },
                    [&] () { if (auto trace_log = context.getTraceLog()) trace_log->flush(true); },
                    [&] () { if (auto text_log = context.getTextLog()) text_log->flush(true); },
                    [&] () { if (auto metric_log = context.getMetricLog()) metric_log->flush(true); },
                    [&] () { if (auto asynchronous_metric_log = context.getAsynchronousMetricLog()) asynchronous_metric_log->flush(true); }
            );
            break;
        case Type::STOP_LISTEN_QUERIES:
        case Type::START_LISTEN_QUERIES:
            throw Exception(String(ASTSystemQuery::typeToString(query.type)) + " is not supported yet", ErrorCodes::NOT_IMPLEMENTED);
        default:
            throw Exception("Unknown type of SYSTEM query", ErrorCodes::BAD_ARGUMENTS);
    }

    return BlockIO();
}


StoragePtr InterpreterSystemQuery::tryRestartReplica(const StorageID & replica, Context & system_context, bool need_ddl_guard)
{
    context.checkAccess(AccessType::SYSTEM_RESTART_REPLICA, replica);

    auto table_ddl_guard = need_ddl_guard ? DatabaseCatalog::instance().getDDLGuard(replica.getDatabaseName(), replica.getTableName()) : nullptr;
    auto [database, table] = DatabaseCatalog::instance().tryGetDatabaseAndTable(replica, context);
    ASTPtr create_ast;

    /// Detach actions
    if (!table || !dynamic_cast<const StorageReplicatedMergeTree *>(table.get()))
        return nullptr;

    table->shutdown();
    {
        /// If table was already dropped by anyone, an exception will be thrown
        auto table_lock = table->lockExclusively(context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);
        create_ast = database->getCreateTableQuery(replica.table_name, context);

        database->detachTable(replica.table_name);
    }
    table.reset();

    /// Attach actions
    /// getCreateTableQuery must return canonical CREATE query representation, there are no need for AST postprocessing
    auto & create = create_ast->as<ASTCreateQuery &>();
    create.attach = true;

    auto columns = InterpreterCreateQuery::getColumnsDescription(*create.columns_list->columns, system_context, false);
    auto constraints = InterpreterCreateQuery::getConstraintsDescription(create.columns_list->constraints);
    auto data_path = database->getTableDataPath(create);

    table = StorageFactory::instance().get(create,
        data_path,
        system_context,
        system_context.getGlobalContext(),
        columns,
        constraints,
        false);

    database->attachTable(replica.table_name, table, data_path);

    table->startup();
    return table;
}

void InterpreterSystemQuery::restartReplicas(Context & system_context)
{
    std::vector<StorageID> replica_names;
    auto & catalog = DatabaseCatalog::instance();

    for (auto & elem : catalog.getDatabases())
    {
        DatabasePtr & database = elem.second;
        for (auto iterator = database->getTablesIterator(context); iterator->isValid(); iterator->next())
        {
            if (auto table = iterator->table())
            {
                if (dynamic_cast<const StorageReplicatedMergeTree *>(table.get()))
                    replica_names.emplace_back(StorageID{database->getDatabaseName(), iterator->name()});
            }
        }
    }

    if (replica_names.empty())
        return;

    TableGuards guards;
    for (const auto & name : replica_names)
        guards.emplace(UniqueTableName{name.database_name, name.table_name}, nullptr);
    for (auto & guard : guards)
        guard.second = catalog.getDDLGuard(guard.first.database_name, guard.first.table_name);

    ThreadPool pool(std::min(size_t(getNumberOfPhysicalCPUCores()), replica_names.size()));
    for (auto & table : replica_names)
        pool.scheduleOrThrowOnError([&]() { tryRestartReplica(table, system_context, false); });
    pool.wait();
}

void InterpreterSystemQuery::syncReplica(ASTSystemQuery &)
{
    context.checkAccess(AccessType::SYSTEM_SYNC_REPLICA, table_id);
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, context);

    if (auto * storage_replicated = dynamic_cast<StorageReplicatedMergeTree *>(table.get()))
    {
        LOG_TRACE(log, "Synchronizing entries in replica's queue with table's log and waiting for it to become empty");
        if (!storage_replicated->waitForShrinkingQueueSize(0, context.getSettingsRef().receive_timeout.totalMilliseconds()))
        {
            LOG_ERROR(log, "SYNC REPLICA {}: Timed out!", table_id.getNameForLogs());
            throw Exception(
                    "SYNC REPLICA " + table_id.getNameForLogs() + ": command timed out! "
                    "See the 'receive_timeout' setting", ErrorCodes::TIMEOUT_EXCEEDED);
        }
        LOG_TRACE(log, "SYNC REPLICA {}: OK", table_id.getNameForLogs());
    }
    else
        throw Exception("Table " + table_id.getNameForLogs() + " is not replicated", ErrorCodes::BAD_ARGUMENTS);
}

void InterpreterSystemQuery::flushDistributed(ASTSystemQuery &)
{
    context.checkAccess(AccessType::SYSTEM_FLUSH_DISTRIBUTED, table_id);

    if (auto * storage_distributed = dynamic_cast<StorageDistributed *>(DatabaseCatalog::instance().getTable(table_id, context).get()))
        storage_distributed->flushClusterNodesAllData();
    else
        throw Exception("Table " + table_id.getNameForLogs() + " is not distributed", ErrorCodes::BAD_ARGUMENTS);
}


AccessRightsElements InterpreterSystemQuery::getRequiredAccessForDDLOnCluster() const
{
    const auto & query = query_ptr->as<const ASTSystemQuery &>();
    using Type = ASTSystemQuery::Type;
    AccessRightsElements required_access;
    switch (query.type)
    {
        case Type::SHUTDOWN: [[fallthrough]];
        case Type::KILL:
        {
            required_access.emplace_back(AccessType::SYSTEM_SHUTDOWN);
            break;
        }
        case Type::DROP_DNS_CACHE: [[fallthrough]];
        case Type::DROP_MARK_CACHE: [[fallthrough]];
#if USE_EMBEDDED_COMPILER
        case Type::DROP_COMPILED_EXPRESSION_CACHE: [[fallthrough]];
#endif
        case Type::DROP_UNCOMPRESSED_CACHE:
        {
            required_access.emplace_back(AccessType::SYSTEM_DROP_CACHE);
            break;
        }
        case Type::RELOAD_DICTIONARY: [[fallthrough]];
        case Type::RELOAD_DICTIONARIES: [[fallthrough]];
        case Type::RELOAD_EMBEDDED_DICTIONARIES:
        {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_DICTIONARY);
            break;
        }
        case Type::RELOAD_CONFIG:
        {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_CONFIG);
            break;
        }
        case Type::STOP_MERGES: [[fallthrough]];
        case Type::START_MERGES:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::SYSTEM_MERGES);
            else
                required_access.emplace_back(AccessType::SYSTEM_MERGES, query.database, query.table);
            break;
        }
        case Type::STOP_TTL_MERGES: [[fallthrough]];
        case Type::START_TTL_MERGES:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::SYSTEM_TTL_MERGES);
            else
                required_access.emplace_back(AccessType::SYSTEM_TTL_MERGES, query.database, query.table);
            break;
        }
        case Type::STOP_MOVES: [[fallthrough]];
        case Type::START_MOVES:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::SYSTEM_MOVES);
            else
                required_access.emplace_back(AccessType::SYSTEM_MOVES, query.database, query.table);
            break;
        }
        case Type::STOP_FETCHES: [[fallthrough]];
        case Type::START_FETCHES:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::SYSTEM_FETCHES);
            else
                required_access.emplace_back(AccessType::SYSTEM_FETCHES, query.database, query.table);
            break;
        }
        case Type::STOP_DISTRIBUTED_SENDS: [[fallthrough]];
        case Type::START_DISTRIBUTED_SENDS:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::SYSTEM_DISTRIBUTED_SENDS);
            else
                required_access.emplace_back(AccessType::SYSTEM_DISTRIBUTED_SENDS, query.database, query.table);
            break;
        }
        case Type::STOP_REPLICATED_SENDS: [[fallthrough]];
        case Type::START_REPLICATED_SENDS:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::SYSTEM_REPLICATED_SENDS);
            else
                required_access.emplace_back(AccessType::SYSTEM_REPLICATED_SENDS, query.database, query.table);
            break;
        }
        case Type::STOP_REPLICATION_QUEUES: [[fallthrough]];
        case Type::START_REPLICATION_QUEUES:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::SYSTEM_REPLICATION_QUEUES);
            else
                required_access.emplace_back(AccessType::SYSTEM_REPLICATION_QUEUES, query.database, query.table);
            break;
        }
        case Type::SYNC_REPLICA:
        {
            required_access.emplace_back(AccessType::SYSTEM_SYNC_REPLICA, query.database, query.table);
            break;
        }
        case Type::RESTART_REPLICA:
        {
            required_access.emplace_back(AccessType::SYSTEM_RESTART_REPLICA, query.database, query.table);
            break;
        }
        case Type::RESTART_REPLICAS:
        {
            required_access.emplace_back(AccessType::SYSTEM_RESTART_REPLICA);
            break;
        }
        case Type::FLUSH_DISTRIBUTED:
        {
            required_access.emplace_back(AccessType::SYSTEM_FLUSH_DISTRIBUTED, query.database, query.table);
            break;
        }
        case Type::FLUSH_LOGS:
        {
            required_access.emplace_back(AccessType::SYSTEM_FLUSH_LOGS);
            break;
        }
        case Type::STOP_LISTEN_QUERIES: break;
        case Type::START_LISTEN_QUERIES: break;
        case Type::UNKNOWN: break;
        case Type::END: break;
    }
    return required_access;
}

}
