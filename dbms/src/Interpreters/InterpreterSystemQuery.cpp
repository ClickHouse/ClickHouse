#include <Interpreters/InterpreterSystemQuery.h>
#include <Common/DNSResolver.h>
#include <Common/ActionLock.h>
#include <Common/config.h>
#include <Common/typeid_cast.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <common/ThreadPool.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionaries.h>
#include <Interpreters/EmbeddedDictionaries.h>
#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/QueryThreadLog.h>
#include <Databases/IDatabase.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/StorageFactory.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <csignal>


namespace DB
{


namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_KILL;
    extern const int NOT_IMPLEMENTED;
}


namespace ActionLocks
{
    extern StorageActionBlockType PartsMerge;
    extern StorageActionBlockType PartsFetch;
    extern StorageActionBlockType PartsSend;
    extern StorageActionBlockType ReplicationQueue;
}


namespace
{

ExecutionStatus getOverallExecutionStatusOfCommands()
{
    return ExecutionStatus(0);
}

/// Consequently tries to execute all commands and genreates final exception message for failed commands
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


/// Implements SYSTEM [START|STOP] <something action from ActionLocks>
void startStopAction(Context & context, ASTSystemQuery & query, StorageActionBlockType action_type, bool start)
{
    auto manager = context.getActionLocksManager();
    manager->cleanExpired();

    if (!query.target_table.empty())
    {
        String database = !query.target_database.empty() ? query.target_database : context.getCurrentDatabase();

        if (start)
            manager->remove(database, query.target_table, action_type);
        else
            manager->add(database, query.target_table, action_type);
    }
    else
    {
        if (start)
            manager->remove(action_type);
        else
            manager->add(action_type);
    }
}
}


InterpreterSystemQuery::InterpreterSystemQuery(const ASTPtr & query_ptr_, Context & context_)
        : query_ptr(query_ptr_->clone()), context(context_), log(&Poco::Logger::get("InterpreterSystemQuery")) {}


BlockIO InterpreterSystemQuery::execute()
{
    auto & query = typeid_cast<ASTSystemQuery &>(*query_ptr);

    using Type = ASTSystemQuery::Type;

    /// Use global context with fresh system profile settings
    Context system_context = context.getGlobalContext();
    system_context.setSetting("profile", context.getSystemProfileName());

    /// Make canonical query for simpler processing
    if (!query.target_table.empty() && query.target_database.empty())
         query.target_database = context.getCurrentDatabase();

    switch (query.type)
    {
        case Type::SHUTDOWN:
            if (kill(0, SIGTERM))
                throwFromErrno("System call kill(0, SIGTERM) failed", ErrorCodes::CANNOT_KILL);
            break;
        case Type::KILL:
            if (kill(0, SIGKILL))
                throwFromErrno("System call kill(0, SIGKILL) failed", ErrorCodes::CANNOT_KILL);
            break;
        case Type::DROP_DNS_CACHE:
            DNSResolver::instance().dropCache();
            /// Reinitialize clusters to update their resolved_addresses
            system_context.reloadClusterConfig();
            break;
        case Type::DROP_MARK_CACHE:
            system_context.dropMarkCache();
            break;
        case Type::DROP_UNCOMPRESSED_CACHE:
            system_context.dropUncompressedCache();
            break;
#if USE_EMBEDDED_COMPILER
        case Type::DROP_COMPILED_EXPRESSION_CACHE:
            system_context.dropCompiledExpressionCache();
            break;
#endif
        case Type::RELOAD_DICTIONARY:
            system_context.getExternalDictionaries().reloadDictionary(query.target_dictionary);
            break;
        case Type::RELOAD_DICTIONARIES:
            executeCommandsAndThrowIfError(
                    [&] () { system_context.getExternalDictionaries().reload(); },
                    [&] () { system_context.getEmbeddedDictionaries().reload(); }
            );
            break;
        case Type::RELOAD_EMBEDDED_DICTIONARIES:
            system_context.getEmbeddedDictionaries().reload();
            break;
        case Type::RELOAD_CONFIG:
            system_context.reloadConfig();
            break;
        case Type::STOP_MERGES:
            startStopAction(context, query, ActionLocks::PartsMerge, false);
            break;
        case Type::START_MERGES:
            startStopAction(context, query, ActionLocks::PartsMerge, true);
            break;
        case Type::STOP_FETCHES:
            startStopAction(context, query, ActionLocks::PartsFetch, false);
            break;
        case Type::START_FETCHES:
            startStopAction(context, query, ActionLocks::PartsFetch, true);
            break;
        case Type::STOP_REPLICATED_SENDS:
            startStopAction(context, query, ActionLocks::PartsSend, false);
            break;
        case Type::START_REPLICATEDS_SENDS:
            startStopAction(context, query, ActionLocks::PartsSend, false);
            break;
        case Type::STOP_REPLICATION_QUEUES:
            startStopAction(context, query, ActionLocks::ReplicationQueue, false);
            break;
        case Type::START_REPLICATION_QUEUES:
            startStopAction(context, query, ActionLocks::ReplicationQueue, true);
            break;
        case Type::SYNC_REPLICA:
            syncReplica(query);
            break;
        case Type::RESTART_REPLICAS:
            restartReplicas(system_context);
            break;
        case Type::RESTART_REPLICA:
            if (!tryRestartReplica(query.target_database, query.target_table, system_context))
                throw Exception("There is no " + query.target_database + "." + query.target_table + " replicated table",
                                ErrorCodes::BAD_ARGUMENTS);
            break;
        case Type::FLUSH_SYSTEM_TABLES:
            executeCommandsAndThrowIfError(
                    [&] () { if (auto query_log = context.getQueryLog(false)) query_log->flush(); },
                    [&] () { if (auto part_log = context.getPartLog("", false)) part_log->flush(); },
                    [&] () { if (auto query_thread_log = context.getQueryThreadLog(false)) query_thread_log->flush(); }
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
    auto database = system_context.getDatabase(database_name);
    auto table_ddl_guard = system_context.getDDLGuard(database_name, table_name, "Table " + database_name + "." + table_name + " is restarting right now");
    ASTPtr create_ast;

    /// Detach actions
    {
        auto table = system_context.tryGetTable(database_name, table_name);

        if (!table || !dynamic_cast<const StorageReplicatedMergeTree *>(table.get()))
            return nullptr;

        table->shutdown();

        /// If table was already dropped by anyone, an exception will be thrown
        auto table_lock = table->lockForAlter(__PRETTY_FUNCTION__);
        create_ast = system_context.getCreateTableQuery(database_name, table_name);

        database->detachTable(table_name);
    }

    /// Attach actions
    {
        /// getCreateTableQuery must return canonical CREATE query representation, there are no need for AST postprocessing
        auto & create = typeid_cast<ASTCreateQuery &>(*create_ast);
        create.attach = true;

        std::string data_path = database->getDataPath();
        auto columns = InterpreterCreateQuery::getColumnsDescription(*create.columns, system_context);

        StoragePtr table = StorageFactory::instance().get(create,
            data_path,
            table_name,
            database_name,
            system_context,
            system_context.getGlobalContext(),
            columns,
            create.attach,
            false);

        database->createTable(system_context, table_name, table, create_ast);

        table->startup();
        return table;
    }
}

void InterpreterSystemQuery::restartReplicas(Context & system_context)
{
    std::vector<std::pair<String, String>> replica_names;

    for (auto & elem : system_context.getDatabases())
    {
        DatabasePtr & database = elem.second;
        const String & database_name = elem.first;

        for (auto iterator = database->getIterator(system_context); iterator->isValid(); iterator->next())
        {
            if (dynamic_cast<const StorageReplicatedMergeTree *>(iterator->table().get()))
                replica_names.emplace_back(database_name, iterator->name());
        }
    }

    if (replica_names.empty())
        return;

    ThreadPool pool(std::min(getNumberOfPhysicalCPUCores(), replica_names.size()));
    for (auto & table : replica_names)
        pool.schedule([&] () { tryRestartReplica(table.first, table.second, system_context); });
    pool.wait();
}

void InterpreterSystemQuery::syncReplica(ASTSystemQuery & query)
{
    String database_name = !query.target_database.empty() ? query.target_database : context.getCurrentDatabase();
    const String & table_name = query.target_table;

    StoragePtr table = context.getTable(database_name, table_name);

    auto table_replicated = dynamic_cast<StorageReplicatedMergeTree *>(table.get());
    if (!table_replicated)
        throw Exception("Table " + database_name + "." + table_name + " is not replicated", ErrorCodes::BAD_ARGUMENTS);

    table_replicated->waitForShrinkingQueueSize(0, context.getSettingsRef().receive_timeout.value.milliseconds());
}


}
