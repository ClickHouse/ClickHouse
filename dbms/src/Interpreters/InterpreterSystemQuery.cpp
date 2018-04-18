#include <Interpreters/InterpreterSystemQuery.h>
#include <Common/DNSCache.h>
#include <Common/ActionLock.h>
#include <Common/typeid_cast.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionaries.h>
#include <Interpreters/EmbeddedDictionaries.h>
#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Databases/IDatabase.h>
#include <Storages/StorageReplicatedMergeTree.h>
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

/// Consequently execute all commands and genreates final exception message for failed commands
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
        : query_ptr(query_ptr_), context(context_), log(&Poco::Logger::get("InterpreterSystemQuery")) {}


BlockIO InterpreterSystemQuery::execute()
{
    auto & query = typeid_cast<ASTSystemQuery &>(*query_ptr);

    using Type = ASTSystemQuery::Type;

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
            DNSCache::instance().drop();
            /// Reinitialize clusters to update their resolved_addresses
            context.reloadClusterConfig();
            break;
        case Type::DROP_MARK_CACHE:
            context.dropMarkCache();
            break;
        case Type::DROP_UNCOMPRESSED_CACHE:
            context.dropUncompressedCache();
            break;
        case Type::RELOAD_DICTIONARY:
            context.getExternalDictionaries().reloadDictionary(query.target_dictionary);
            break;
        case Type::RELOAD_DICTIONARIES:
        {
            auto status = getOverallExecutionStatusOfCommands(
                    [&] { context.getExternalDictionaries().reload(); },
                    [&] { context.getEmbeddedDictionaries().reload(); }
            );
            if (status.code != 0)
                throw Exception(status.message, status.code);
            break;
        }
        case Type::RELOAD_CONFIG:
            context.reloadConfig();
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
            restartReplicas();
            break;
        case Type::STOP_LISTEN_QUERIES:
        case Type::START_LISTEN_QUERIES:
            throw Exception(String(ASTSystemQuery::typeToString(query.type)) + " is not supported yet", ErrorCodes::NOT_IMPLEMENTED);
        default:
            throw Exception("Unknown type of SYSTEM query", ErrorCodes::BAD_ARGUMENTS);
    }

    return BlockIO();
}


void InterpreterSystemQuery::restartReplicas()
{
    for (auto & elem : context.getDatabases())
    {
        DatabasePtr & database = elem.second;
        const String & database_name = elem.first;

        for (auto iterator = database->getIterator(context); iterator->isValid(); iterator->next())
        {
            if (!dynamic_cast<const StorageReplicatedMergeTree *>(iterator->table().get()))
                continue;

            String table_name = iterator->name();

            try
            {
                /// Detaching actions
                {
                    auto ast_detach = std::make_shared<ASTDropQuery>();
                    ast_detach->detach = true;
                    ast_detach->database = database_name;
                    ast_detach->table = table_name;

                    InterpreterDropQuery interpreter(ast_detach, context);
                    interpreter.execute();
                }

                /// Attaching actions
                {
                    auto ast_attach = std::make_shared<ASTCreateQuery>();
                    ast_attach->attach = true;
                    ast_attach->database = database_name;
                    ast_attach->table = table_name;

                    InterpreterCreateQuery interpreter(ast_attach, context);
                    interpreter.execute();
                }
            }
            catch (Exception & e)
            {
                /// Continue attaching detaching in case of "ordinary" errors
                tryLogCurrentException(log);
            }
        }
    }
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
