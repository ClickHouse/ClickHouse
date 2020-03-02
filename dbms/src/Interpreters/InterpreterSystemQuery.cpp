#include <Interpreters/InterpreterSystemQuery.h>
#include <Common/DNSResolver.h>
#include <Common/ActionLock.h>
#include "config_core.h"
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
#include <Interpreters/QueryLog.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/QueryThreadLog.h>
#include <Interpreters/TraceLog.h>
#include <Interpreters/TextLog.h>
#include <Interpreters/MetricLog.h>
#include <Access/AccessRightsContext.h>
#include <Databases/IDatabase.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/StorageFactory.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <csignal>
#include <algorithm>
#include "InterpreterSystemQuery.h"


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
        return AccessType::STOP_MERGES;
    else if (action_type == ActionLocks::PartsFetch)
        return AccessType::STOP_FETCHES;
    else if (action_type == ActionLocks::PartsSend)
        return AccessType::STOP_REPLICATED_SENDS;
    else if (action_type == ActionLocks::ReplicationQueue)
        return AccessType::STOP_REPLICATION_QUEUES;
    else if (action_type == ActionLocks::DistributedSend)
        return AccessType::STOP_DISTRIBUTED_SENDS;
    else if (action_type == ActionLocks::PartsTTLMerge)
        return AccessType::STOP_TTL_MERGES;
    else if (action_type == ActionLocks::PartsMove)
        return AccessType::STOP_MOVES;
    else
        throw Exception("Unknown action type: " + std::to_string(action_type), ErrorCodes::LOGICAL_ERROR);
}


/// Implements SYSTEM [START|STOP] <something action from ActionLocks>
void startStopAction(Context & context, Poco::Logger * log, ASTSystemQuery & query, StorageActionBlockType action_type, bool start)
{
    auto manager = context.getActionLocksManager();
    manager->cleanExpired();

    if (!query.table.empty())
    {
        String database = !query.database.empty() ? query.database : context.getCurrentDatabase();
        context.checkAccess(getRequiredAccessType(action_type), database, query.table);
        if (start)
            manager->remove(database, query.table, action_type);
        else
            manager->add(database, query.table, action_type);
    }
    else
    {
        for (auto & elem : DatabaseCatalog::instance().getDatabases())
        {
            for (auto iterator = elem.second->getTablesIterator(context); iterator->isValid(); iterator->next())
            {
                if (context.getAccessRights()->isGranted(log, getRequiredAccessType(action_type), elem.first, iterator->name()))
                {
                    if (start)
                        manager->remove(iterator->table(), action_type);
                    else
                        manager->add(iterator->table(), action_type);
                }
            }
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
    if (!query.table.empty() && query.database.empty())
         query.database = context.getCurrentDatabase();

    if (!query.target_dictionary.empty() && !query.database.empty())
        query.target_dictionary = query.database + "." + query.target_dictionary;

    switch (query.type)
    {
        case Type::SHUTDOWN:
            context.checkAccess(AccessType::SHUTDOWN);
            if (kill(0, SIGTERM))
                throwFromErrno("System call kill(0, SIGTERM) failed", ErrorCodes::CANNOT_KILL);
            break;
        case Type::KILL:
            context.checkAccess(AccessType::SHUTDOWN);
            if (kill(0, SIGKILL))
                throwFromErrno("System call kill(0, SIGKILL) failed", ErrorCodes::CANNOT_KILL);
            break;
        case Type::DROP_DNS_CACHE:
            context.checkAccess(AccessType::DROP_CACHE);
            DNSResolver::instance().dropCache();
            /// Reinitialize clusters to update their resolved_addresses
            system_context.reloadClusterConfig();
            break;
        case Type::DROP_MARK_CACHE:
            context.checkAccess(AccessType::DROP_CACHE);
            system_context.dropMarkCache();
            break;
        case Type::DROP_UNCOMPRESSED_CACHE:
            context.checkAccess(AccessType::DROP_CACHE);
            system_context.dropUncompressedCache();
            break;
#if USE_EMBEDDED_COMPILER
        case Type::DROP_COMPILED_EXPRESSION_CACHE:
            context.checkAccess(AccessType::DROP_CACHE);
            system_context.dropCompiledExpressionCache();
            break;
#endif
        case Type::RELOAD_DICTIONARY:
            context.checkAccess(AccessType::RELOAD_DICTIONARY);
            system_context.getExternalDictionariesLoader().loadOrReload(query.target_dictionary);
            break;
        case Type::RELOAD_DICTIONARIES:
            context.checkAccess(AccessType::RELOAD_DICTIONARY);
            executeCommandsAndThrowIfError(
                    [&] () { system_context.getExternalDictionariesLoader().reloadAllTriedToLoad(); },
                    [&] () { system_context.getEmbeddedDictionaries().reload(); }
            );
            break;
        case Type::RELOAD_EMBEDDED_DICTIONARIES:
            context.checkAccess(AccessType::RELOAD_DICTIONARY);
            system_context.getEmbeddedDictionaries().reload();
            break;
        case Type::RELOAD_CONFIG:
            context.checkAccess(AccessType::RELOAD_CONFIG);
            system_context.reloadConfig();
            break;
        case Type::STOP_MERGES:
            startStopAction(context, log, query, ActionLocks::PartsMerge, false);
            break;
        case Type::START_MERGES:
            startStopAction(context, log, query, ActionLocks::PartsMerge, true);
            break;
        case Type::STOP_TTL_MERGES:
            startStopAction(context, log, query, ActionLocks::PartsTTLMerge, false);
            break;
        case Type::START_TTL_MERGES:
            startStopAction(context, log, query, ActionLocks::PartsTTLMerge, true);
            break;
        case Type::STOP_MOVES:
            startStopAction(context, log, query, ActionLocks::PartsMove, false);
            break;
        case Type::START_MOVES:
            startStopAction(context, log, query, ActionLocks::PartsMove, true);
            break;
        case Type::STOP_FETCHES:
            startStopAction(context, log, query, ActionLocks::PartsFetch, false);
            break;
        case Type::START_FETCHES:
            startStopAction(context, log, query, ActionLocks::PartsFetch, true);
            break;
        case Type::STOP_REPLICATED_SENDS:
            startStopAction(context, log, query, ActionLocks::PartsSend, false);
            break;
        case Type::START_REPLICATED_SENDS:
            startStopAction(context, log, query, ActionLocks::PartsSend, true);
            break;
        case Type::STOP_REPLICATION_QUEUES:
            startStopAction(context, log, query, ActionLocks::ReplicationQueue, false);
            break;
        case Type::START_REPLICATION_QUEUES:
            startStopAction(context, log, query, ActionLocks::ReplicationQueue, true);
            break;
        case Type::STOP_DISTRIBUTED_SENDS:
            startStopAction(context, log, query, ActionLocks::DistributedSend, false);
            break;
        case Type::START_DISTRIBUTED_SENDS:
            startStopAction(context, log, query, ActionLocks::DistributedSend, true);
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
            if (!tryRestartReplica(query.database, query.table, system_context))
                throw Exception("There is no " + query.database + "." + query.table + " replicated table",
                                ErrorCodes::BAD_ARGUMENTS);
            break;
        case Type::FLUSH_LOGS:
            context.checkAccess(AccessType::FLUSH_LOGS);
            executeCommandsAndThrowIfError(
                    [&] () { if (auto query_log = context.getQueryLog()) query_log->flush(); },
                    [&] () { if (auto part_log = context.getPartLog("")) part_log->flush(); },
                    [&] () { if (auto query_thread_log = context.getQueryThreadLog()) query_thread_log->flush(); },
                    [&] () { if (auto trace_log = context.getTraceLog()) trace_log->flush(); },
                    [&] () { if (auto text_log = context.getTextLog()) text_log->flush(); },
                    [&] () { if (auto metric_log = context.getMetricLog()) metric_log->flush(); }
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


StoragePtr InterpreterSystemQuery::tryRestartReplica(const String & database_name, const String & table_name, Context & system_context)
{
    context.checkAccess(AccessType::RESTART_REPLICA, database_name, table_name);
    auto database = DatabaseCatalog::instance().getDatabase(database_name);

    auto table_ddl_guard = DatabaseCatalog::instance().getDDLGuard(database_name, table_name);
    ASTPtr create_ast;

    /// Detach actions
    {
        auto table = DatabaseCatalog::instance().tryGetTable({database_name, table_name});

        if (!table || !dynamic_cast<const StorageReplicatedMergeTree *>(table.get()))
            return nullptr;

        table->shutdown();

        /// If table was already dropped by anyone, an exception will be thrown
        auto table_lock = table->lockExclusively(context.getCurrentQueryId());
        create_ast = database->getCreateTableQuery(system_context, table_name);

        database->detachTable(table_name);
    }

    /// Attach actions
    {
        /// getCreateTableQuery must return canonical CREATE query representation, there are no need for AST postprocessing
        auto & create = create_ast->as<ASTCreateQuery &>();
        create.attach = true;

        auto columns = InterpreterCreateQuery::getColumnsDescription(*create.columns_list->columns, system_context);
        auto constraints = InterpreterCreateQuery::getConstraintsDescription(create.columns_list->constraints);

        StoragePtr table = StorageFactory::instance().get(create,
            database->getTableDataPath(create),
            system_context,
            system_context.getGlobalContext(),
            columns,
            constraints,
            false);

        database->createTable(system_context, table_name, table, create_ast);

        table->startup();
        return table;
    }
}

void InterpreterSystemQuery::restartReplicas(Context & system_context)
{
    std::vector<std::pair<String, String>> replica_names;

    for (auto & elem : DatabaseCatalog::instance().getDatabases())
    {
        DatabasePtr & database = elem.second;
        const String & database_name = elem.first;

        for (auto iterator = database->getTablesIterator(system_context); iterator->isValid(); iterator->next())
        {
            if (dynamic_cast<const StorageReplicatedMergeTree *>(iterator->table().get()))
                replica_names.emplace_back(database_name, iterator->name());
        }
    }

    if (replica_names.empty())
        return;

    ThreadPool pool(std::min(size_t(getNumberOfPhysicalCPUCores()), replica_names.size()));
    for (auto & table : replica_names)
        pool.scheduleOrThrowOnError([&]() { tryRestartReplica(table.first, table.second, system_context); });
    pool.wait();
}

void InterpreterSystemQuery::syncReplica(ASTSystemQuery & query)
{
    String database_name = context.resolveDatabase(query.database);
    const String & table_name = query.table;

    context.checkAccess(AccessType::SYNC_REPLICA, database_name, table_name);
    StoragePtr table = DatabaseCatalog::instance().getTable({database_name, table_name});

    if (auto storage_replicated = dynamic_cast<StorageReplicatedMergeTree *>(table.get()))
    {
        LOG_TRACE(log, "Synchronizing entries in replica's queue with table's log and waiting for it to become empty");
        if (!storage_replicated->waitForShrinkingQueueSize(0, context.getSettingsRef().receive_timeout.totalMilliseconds()))
        {
            LOG_ERROR(log, "SYNC REPLICA " + database_name + "." + table_name + ": Timed out!");
            throw Exception(
                    "SYNC REPLICA " + database_name + "." + table_name + ": command timed out! "
                    "See the 'receive_timeout' setting", ErrorCodes::TIMEOUT_EXCEEDED);
        }
        LOG_TRACE(log, "SYNC REPLICA " + database_name + "." + table_name + ": OK");
    }
    else
        throw Exception("Table " + database_name + "." + table_name + " is not replicated", ErrorCodes::BAD_ARGUMENTS);
}

void InterpreterSystemQuery::flushDistributed(ASTSystemQuery & query)
{
    String database_name = context.resolveDatabase(query.database);
    String & table_name = query.table;
    context.checkAccess(AccessType::FLUSH_DISTRIBUTED, database_name, table_name);

    if (auto storage_distributed = dynamic_cast<StorageDistributed *>(DatabaseCatalog::instance().getTable({database_name, table_name}).get()))
        storage_distributed->flushClusterNodesAllData();
    else
        throw Exception("Table " + database_name + "." + table_name + " is not distributed", ErrorCodes::BAD_ARGUMENTS);
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
            required_access.emplace_back(AccessType::SHUTDOWN);
            break;
        }
        case Type::DROP_DNS_CACHE: [[fallthrough]];
        case Type::DROP_MARK_CACHE: [[fallthrough]];
#if USE_EMBEDDED_COMPILER
        case Type::DROP_COMPILED_EXPRESSION_CACHE: [[fallthrough]];
#endif
        case Type::DROP_UNCOMPRESSED_CACHE:
        {
            required_access.emplace_back(AccessType::DROP_CACHE);
            break;
        }
        case Type::RELOAD_DICTIONARY: [[fallthrough]];
        case Type::RELOAD_DICTIONARIES: [[fallthrough]];
        case Type::RELOAD_EMBEDDED_DICTIONARIES:
        {
            required_access.emplace_back(AccessType::RELOAD_DICTIONARY);
            break;
        }
        case Type::RELOAD_CONFIG:
        {
            required_access.emplace_back(AccessType::RELOAD_CONFIG);
            break;
        }
        case Type::STOP_MERGES: [[fallthrough]];
        case Type::START_MERGES:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::STOP_MERGES);
            else
                required_access.emplace_back(AccessType::STOP_MERGES, query.database, query.table);
            break;
        }
        case Type::STOP_TTL_MERGES: [[fallthrough]];
        case Type::START_TTL_MERGES:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::STOP_TTL_MERGES);
            else
                required_access.emplace_back(AccessType::STOP_TTL_MERGES, query.database, query.table);
            break;
        }
        case Type::STOP_MOVES: [[fallthrough]];
        case Type::START_MOVES:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::STOP_MOVES);
            else
                required_access.emplace_back(AccessType::STOP_MOVES, query.database, query.table);
            break;
        }
        case Type::STOP_FETCHES: [[fallthrough]];
        case Type::START_FETCHES:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::STOP_FETCHES);
            else
                required_access.emplace_back(AccessType::STOP_FETCHES, query.database, query.table);
            break;
        }
        case Type::STOP_DISTRIBUTED_SENDS: [[fallthrough]];
        case Type::START_DISTRIBUTED_SENDS:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::STOP_DISTRIBUTED_SENDS);
            else
                required_access.emplace_back(AccessType::STOP_DISTRIBUTED_SENDS, query.database, query.table);
            break;
        }
        case Type::STOP_REPLICATED_SENDS: [[fallthrough]];
        case Type::START_REPLICATED_SENDS:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::STOP_REPLICATED_SENDS);
            else
                required_access.emplace_back(AccessType::STOP_REPLICATED_SENDS, query.database, query.table);
            break;
        }
        case Type::STOP_REPLICATION_QUEUES: [[fallthrough]];
        case Type::START_REPLICATION_QUEUES:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::STOP_REPLICATION_QUEUES);
            else
                required_access.emplace_back(AccessType::STOP_REPLICATION_QUEUES, query.database, query.table);
            break;
        }
        case Type::SYNC_REPLICA:
        {
            required_access.emplace_back(AccessType::SYNC_REPLICA, query.database, query.table);
            break;
        }
        case Type::RESTART_REPLICA:
        {
            required_access.emplace_back(AccessType::RESTART_REPLICA, query.database, query.table);
            break;
        }
        case Type::RESTART_REPLICAS:
        {
            required_access.emplace_back(AccessType::RESTART_REPLICA);
            break;
        }
        case Type::FLUSH_DISTRIBUTED:
        {
            required_access.emplace_back(AccessType::FLUSH_DISTRIBUTED, query.database, query.table);
            break;
        }
        case Type::FLUSH_LOGS:
        {
            required_access.emplace_back(AccessType::FLUSH_LOGS);
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
