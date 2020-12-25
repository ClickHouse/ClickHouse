#include <Common/DNSResolver.h>
#include <Common/ActionLock.h>
#include <Common/typeid_cast.h>
#include <Common/thread_local_rng.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/SymbolIndex.h>
#include <Common/ThreadPool.h>
#include <Common/escapeForFileName.h>

#include <Interpreters/InterpreterSystemQuery.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
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
#include <Interpreters/OpenTelemetrySpanLog.h>

#include <Access/ContextAccess.h>
#include <Access/AllowedClientHosts.h>
#include <Databases/IDatabase.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/StorageFactory.h>
#include "Processors/Executors/PullingPipelineExecutor.h"

#include <Parsers/ASTPartition.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTCreateQuery.h>

#include <csignal>
#include <algorithm>
#include <memory>

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
    extern const int TABLE_WAS_NOT_DROPPED;
    extern const int SYSTEM_ERROR;
    extern const int NO_ZOOKEEPER;
    extern const int NO_AVAILABLE_DATA;
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

    if (volume_ptr && action_type == ActionLocks::PartsMerge)
    {
        volume_ptr->setAvoidMergesUserOverride(!start);
    }
    else if (table_id)
    {
        auto table = DatabaseCatalog::instance().tryGetTable(table_id, context);
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
        auto access = context.getAccess();
        for (auto & elem : DatabaseCatalog::instance().getDatabases())
        {
            for (auto iterator = elem.second->getTablesIterator(context); iterator->isValid(); iterator->next())
            {
                StoragePtr table = iterator->table();
                if (!table)
                    continue;

                if (!access->isGranted(getRequiredAccessType(action_type), elem.first, iterator->name()))
                {
                    LOG_INFO(
                        log,
                        "Access {} denied, skipping {}.{}",
                        toString(getRequiredAccessType(action_type)),
                        elem.first,
                        iterator->name());
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

    volume_ptr = {};
    if (!query.storage_policy.empty() && !query.volume.empty())
        volume_ptr = context.getStoragePolicy(query.storage_policy)->getVolumeByName(query.volume);

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
            system_context.getExternalDictionariesLoader().loadOrReload(
                    DatabaseCatalog::instance().resolveDictionaryName(query.target_dictionary));
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
        case Type::RELOAD_SYMBOLS:
#if defined(__ELF__) && !defined(__FreeBSD__)
            context.checkAccess(AccessType::SYSTEM_RELOAD_SYMBOLS);
            (void)SymbolIndex::instance(true);
            break;
#else
            throw Exception("SYSTEM RELOAD SYMBOLS is not supported on current platform", ErrorCodes::NOT_IMPLEMENTED);
#endif
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
        case Type::DROP_REPLICA:
            dropReplica(query);
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
        case Type::RESTORE_REPLICA:
            restoreReplica();
            break;
        case Type::FLUSH_LOGS:
            context.checkAccess(AccessType::SYSTEM_FLUSH_LOGS);
            executeCommandsAndThrowIfError(
                [&] { if (auto query_log = context.getQueryLog()) query_log->flush(true); },
                [&] { if (auto part_log = context.getPartLog("")) part_log->flush(true); },
                [&] { if (auto query_thread_log = context.getQueryThreadLog()) query_thread_log->flush(true); },
                [&] { if (auto trace_log = context.getTraceLog()) trace_log->flush(true); },
                [&] { if (auto text_log = context.getTextLog()) text_log->flush(true); },
                [&] { if (auto metric_log = context.getMetricLog()) metric_log->flush(true); },
                [&] { if (auto asynchronous_metric_log = context.getAsynchronousMetricLog()) asynchronous_metric_log->flush(true); },
                [&] { if (auto opentelemetry_span_log = context.getOpenTelemetrySpanLog()) opentelemetry_span_log->flush(true); }
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

void InterpreterSystemQuery::restoreReplica()
{
    context.checkAccess(AccessType::SYSTEM_RESTORE_REPLICA, table_id);

    auto [db, table] = DatabaseCatalog::instance().getDatabaseAndTable(table_id, context);
    StorageReplicatedMergeTree * storage_replicated;

    if (!table || ((storage_replicated = dynamic_cast<StorageReplicatedMergeTree*>(table.get())) == nullptr))
        throw Exception("There is no replicated table \"" +
            (table_id.database_name.empty() ? "" : (table_id.database_name + ".")) +
            table_id.table_name + "\"",
            ErrorCodes::BAD_ARGUMENTS);

    /// 0. Check if the replica needs to be restored (metadata missing).

    const zkutil::ZooKeeperPtr& zookeeper = context.getZooKeeper();

    if (zookeeper->expired())
        throw Exception(
            "Cannot restore table metadata because ZooKeeper session has expired.",
            ErrorCodes::NO_ZOOKEEPER);

    StorageReplicatedMergeTree::Status status;
    storage_replicated->getStatus(status);

    /// If there exists the path zk_path + /replicas/ *, we have at least one working replica,
    /// no need to restore anything.
    const String& zk_root_path = status.zookeeper_path;
    const String replica_zk_path = zk_root_path + "/replicas";

    if (zookeeper->exists(replica_zk_path))
    {
        Strings replicas_present;
        zookeeper->tryGetChildren(replica_zk_path, replicas_present);

        if (!replicas_present.empty())
            // TODO Add the ZKPATH option to restore the table metadata on a certain replica,
            throw Exception(
                "The metadata for " + replica_zk_path + " is present at some of the replicas -- nothing to restore,"
                " try creating a new replica with the CREATE TABLE query",
                ErrorCodes::NO_AVAILABLE_DATA);
    }

    const UUID uuid = table_id.uuid;
    const String& db_name = table_id.database_name;
    const String& old_table_name = table_id.table_name;
    const String new_table_name = old_table_name + "_tmp_" + std::to_string(thread_local_rng());

    LOG_DEBUG(log, "Restoring " + db_name + "." + old_table_name + ", zk root path at " + zk_root_path);

    /// 1. Create a new replicated table out of current one (CREATE TABLE new AS old).
    {
        ASTPtr create_query_ptr = std::make_shared<ASTCreateQuery>();
        ASTCreateQuery& create_query = create_query_ptr->as<ASTCreateQuery&>();

        create_query.database = db_name;
        create_query.table = new_table_name;
        create_query.uuid = uuid;
        create_query.as_database = db_name;
        create_query.as_table = old_table_name;

        InterpreterCreateQuery interpreter_create(create_query_ptr, context);

        /// catch the exception here
        interpreter_create.execute();

        LOG_DEBUG(log, "Created a new replicated table " + db_name + "." + new_table_name);
    }

    /// 2. Stop replica fetches for the old table (SYSTEM STOP FETCHES old)
    {
        ASTPtr stop_fetches_ptr = std::make_shared<ASTSystemQuery>();
        ASTSystemQuery& stop_fetches_query = stop_fetches_ptr->as<ASTSystemQuery&>();

        stop_fetches_query.database = db_name;
        stop_fetches_query.table = old_table_name;
        stop_fetches_query.type = ASTSystemQuery::Type::STOP_FETCHES;

        InterpreterSystemQuery interpreter_stop_fetches(stop_fetches_ptr, context);

        interpreter_stop_fetches.execute();

        LOG_DEBUG(log, "Stopped replica fetches for " + db_name + "." + old_table_name);
    }

    /// 3. Move parts to a new table that will register them in zookeeper.
    {
        /// 3.1 Form a partitions request (from the old table)
        /// (SELECT partition_id FROM system.parts WHERE database = 'old_db' AND table = 'old' AND active)

        ASTPtr get_parts_ptr = std::make_shared<ASTSelectQuery>();
        auto& get_parts_query = get_parts_ptr->as<ASTSelectQuery&>();

        /// 3.1.1 SELECT partition_id
        const String partition_id{"partition_id"};

        ASTPtr get_parts_select_ptr = std::make_shared<ASTExpressionList>();

        get_parts_select_ptr->as<ASTExpressionList&>().children = {std::make_shared<ASTIdentifier>(partition_id)};

        /// 3.1.2 FROM system.parts

        ASTPtr get_parts_from_ptr = std::make_shared<ASTTablesInSelectQuery>();
        auto& get_parts_from = get_parts_from_ptr->as<ASTTablesInSelectQuery&>();

        ASTPtr get_parts_from_table_ptr = std::make_shared<ASTTableExpression>();
        auto& get_parts_from_table = get_parts_from_table_ptr->as<ASTTableExpression&>();

        get_parts_from_table.database_and_table_name = std::make_shared<ASTIdentifier>(
            std::vector<String>{"system", "parts"});

        get_parts_from.children.emplace_back(std::move(get_parts_from_table_ptr));

        /// 3.1.3 WHERE database = 'db' AND table = 'old' AND active
        /// and(active, and(equals(database, db), equals(table, old_table_name)))

        ASTPtr get_parts_where_ptr = makeASTFunction("and",
            std::make_shared<ASTIdentifier>("active"),
            makeASTFunction("and",
                makeASTFunction("equals",
                    std::make_shared<ASTIdentifier>("database"),
                    std::make_shared<ASTLiteral>(db_name)),
                makeASTFunction("equals",
                    std::make_shared<ASTIdentifier>("table"),
                    std::make_shared<ASTLiteral>(old_table_name))));

        /// 3.1.4 Set main query parts
        get_parts_query.setExpression(ASTSelectQuery::Expression::SELECT, std::move(get_parts_select_ptr));
        get_parts_query.setExpression(ASTSelectQuery::Expression::TABLES, std::move(get_parts_from_ptr));
        get_parts_query.setExpression(ASTSelectQuery::Expression::WHERE, std::move(get_parts_where_ptr));

        /// 3.2 Execute the request and get the resulting pipeline to execute.
        InterpreterSelectQuery get_parts_interpreter(get_parts_ptr, context, {}, Names{});
        BlockIO parts_block = get_parts_interpreter.execute();

        /// 3.3 Get the resulting block index and prepare the pipeline.
        PullingPipelineExecutor get_parts_executor(parts_block.pipeline);
        const size_t parts_result_index = get_parts_executor.getHeader().getPositionByName(partition_id);

        /// 3.4 Form the moving request.
        /// (ALTER TABLE old MOVE PARTITION ID x TO TABLE new).

        ASTPtr move_parts_ptr = std::make_shared<ASTAlterQuery>();
        ASTAlterQuery& move_parts_query = move_parts_ptr->as<ASTAlterQuery&>();

        move_parts_query.database = db_name;
        move_parts_query.table = old_table_name;
        move_parts_query.uuid = uuid;

        ASTPtr move_parts_alter_command_ptr = std::make_shared<ASTAlterCommand>();
        ASTAlterCommand& move_parts_alter_command = move_parts_alter_command_ptr->as<ASTAlterCommand&>();

        move_parts_alter_command.type = ASTAlterCommand::Type::MOVE_PARTITION;
        move_parts_alter_command.move_destination_type = DataDestinationType::TABLE;
        move_parts_alter_command.partition = std::make_shared<ASTPartition>();
        move_parts_alter_command.to_database = db_name;
        move_parts_alter_command.to_table = new_table_name;

        ASTExpressionList move_parts_command_list{};
        move_parts_command_list.children = {move_parts_alter_command_ptr}; // ok storing pointer to stack value as
        move_parts_query.command_list = &move_parts_command_list;       // it will be alive by the executor end.

        InterpreterAlterQuery move_parts_interpreter(move_parts_ptr, context);

        Chunk partition_ids_chunk{};

        while (get_parts_executor.pull(partition_ids_chunk))
        {
            /// 3.5 Get a bunch of partition ids.
            const ColumnString& partition_ids_col = *checkAndGetColumn<ColumnString>(
                    partition_ids_chunk.getColumns()[parts_result_index].get());

            String part_id_str;

            for (size_t i = 0; i < partition_ids_col.size(); ++i)
            {
                /// 3.6 Move each partition to new table
                const StringRef part_id_ref = partition_ids_col.getDataAt(i);

                part_id_str = {part_id_ref.data, part_id_ref.size};
                move_parts_alter_command.partition->as<ASTPartition>()->id = part_id_str;

                move_parts_interpreter.execute();

                LOG_TRACE(log,
                    "Moved partition " + part_id_str +
                    " from table " + db_name + "." + old_table_name +
                    " to table " + db_name + "." + new_table_name);
            }
        }

        LOG_DEBUG(log,
            "Moved all parts from table " + db_name + "." + old_table_name +
            " to table " + db_name + "." + new_table_name);
    }

    /// 4. Rename tables (RENAME TABLE new TO old, old TO new).
    {
        ASTPtr rename_ptr = std::make_shared<ASTRenameQuery>();

        rename_ptr->as<ASTRenameQuery&>().elements =
        {
            {{db_name, old_table_name}, {db_name, new_table_name}}, // old -> new
            {{db_name, new_table_name}, {db_name, new_table_name}}  // new -> old
        };

        InterpreterRenameQuery interpreter_rename(rename_ptr, context);

        interpreter_rename.execute();

        LOG_DEBUG(log,
            "Renamed tables " + db_name + "." + old_table_name +
            " <-> " + db_name + "." + new_table_name);
    }

    /// 5. Detach old table (DETACH TABLE old).
    {
        ASTPtr detach_ptr = std::make_shared<ASTDropQuery>();
        ASTDropQuery& detach_query = detach_ptr->as<ASTDropQuery&>();

        detach_query.kind = ASTDropQuery::Kind::Detach;
        detach_query.database = db_name;
        detach_query.uuid = uuid;
        detach_query.table = old_table_name;

        InterpreterDropQuery interpreter_drop(detach_ptr, context);

        interpreter_drop.execute();

        LOG_DEBUG(log, "Detached table " + db_name + "." + old_table_name);
    }

    /// 6. Delete information about the old table, so it wouldn't be attached after server restart.
    {
        const String old_table_metadata_file = db->getObjectMetadataPath(old_table_name);

        std::error_code file_delete_error;

        if (std::filesystem::remove(old_table_metadata_file, file_delete_error))
        {
            LOG_DEBUG(log,
                "Removed table " + db_name + "." + old_table_name +
                " 's metadata at " + old_table_metadata_file);

            return;
        }

        throw Exception(
            ErrorCodes::SYSTEM_ERROR,
            "Error removing file " + old_table_metadata_file + ": " + file_delete_error.message());
    }
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
                    replica_names.emplace_back(StorageID{iterator->databaseName(), iterator->name()});
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

void InterpreterSystemQuery::dropReplica(ASTSystemQuery & query)
{
    if (query.replica.empty())
        throw Exception("Replica name is empty", ErrorCodes::BAD_ARGUMENTS);

    if (!table_id.empty())
    {
        context.checkAccess(AccessType::SYSTEM_DROP_REPLICA, table_id);
        StoragePtr table = DatabaseCatalog::instance().getTable(table_id, context);

        if (!dropReplicaImpl(query, table))
            throw Exception("Table " + table_id.getNameForLogs() + " is not replicated", ErrorCodes::BAD_ARGUMENTS);
    }
    else if (!query.database.empty())
    {
        context.checkAccess(AccessType::SYSTEM_DROP_REPLICA, query.database);
        DatabasePtr database = DatabaseCatalog::instance().getDatabase(query.database);
        for (auto iterator = database->getTablesIterator(context); iterator->isValid(); iterator->next())
            dropReplicaImpl(query, iterator->table());
        LOG_TRACE(log, "Dropped replica {} from database {}", query.replica, backQuoteIfNeed(database->getDatabaseName()));
    }
    else if (query.is_drop_whole_replica)
    {
        context.checkAccess(AccessType::SYSTEM_DROP_REPLICA);
        auto databases = DatabaseCatalog::instance().getDatabases();

        for (auto & elem : databases)
        {
            DatabasePtr & database = elem.second;
            for (auto iterator = database->getTablesIterator(context); iterator->isValid(); iterator->next())
                dropReplicaImpl(query, iterator->table());
            LOG_TRACE(log, "Dropped replica {} from database {}", query.replica, backQuoteIfNeed(database->getDatabaseName()));
        }
    }
    else if (!query.replica_zk_path.empty())
    {
        context.checkAccess(AccessType::SYSTEM_DROP_REPLICA);
        auto remote_replica_path = query.replica_zk_path  + "/replicas/" + query.replica;

        /// This check is actually redundant, but it may prevent from some user mistakes
        for (auto & elem : DatabaseCatalog::instance().getDatabases())
        {
            DatabasePtr & database = elem.second;
            for (auto iterator = database->getTablesIterator(context); iterator->isValid(); iterator->next())
            {
                if (auto * storage_replicated = dynamic_cast<StorageReplicatedMergeTree *>(iterator->table().get()))
                {
                    StorageReplicatedMergeTree::Status status;
                    storage_replicated->getStatus(status);
                    if (status.zookeeper_path == query.replica_zk_path)
                        throw Exception("There is a local table " + storage_replicated->getStorageID().getNameForLogs() +
                                        ", which has the same table path in ZooKeeper. Please check the path in query. "
                                        "If you want to drop replica of this table, use `DROP TABLE` "
                                        "or `SYSTEM DROP REPLICA 'name' FROM db.table`", ErrorCodes::TABLE_WAS_NOT_DROPPED);
                }
            }
        }

        auto zookeeper = context.getZooKeeper();

        bool looks_like_table_path = zookeeper->exists(query.replica_zk_path + "/replicas") ||
                                     zookeeper->exists(query.replica_zk_path + "/dropped");
        if (!looks_like_table_path)
            throw Exception("Specified path " + query.replica_zk_path + " does not look like a table path",
                            ErrorCodes::TABLE_WAS_NOT_DROPPED);

        if (zookeeper->exists(remote_replica_path + "/is_active"))
            throw Exception("Can't remove replica: " + query.replica + ", because it's active",
                ErrorCodes::TABLE_WAS_NOT_DROPPED);

        StorageReplicatedMergeTree::dropReplica(zookeeper, query.replica_zk_path, query.replica, log);
        LOG_INFO(log, "Dropped replica {}", remote_replica_path);
    }
    else
        throw Exception("Invalid query", ErrorCodes::LOGICAL_ERROR);
}

bool InterpreterSystemQuery::dropReplicaImpl(ASTSystemQuery & query, const StoragePtr & table)
{
    auto * storage_replicated = dynamic_cast<StorageReplicatedMergeTree *>(table.get());
    if (!storage_replicated)
        return false;

    StorageReplicatedMergeTree::Status status;
    auto zookeeper = context.getZooKeeper();
    storage_replicated->getStatus(status);

    /// Do not allow to drop local replicas and active remote replicas
    if (query.replica == status.replica_name)
        throw Exception("We can't drop local replica, please use `DROP TABLE` "
                        "if you want to clean the data and drop this replica", ErrorCodes::TABLE_WAS_NOT_DROPPED);

    /// NOTE it's not atomic: replica may become active after this check, but before dropReplica(...)
    /// However, the main usecase is to drop dead replica, which cannot become active.
    /// This check prevents only from accidental drop of some other replica.
    if (zookeeper->exists(status.zookeeper_path + "/replicas/" + query.replica + "/is_active"))
        throw Exception("Can't drop replica: " + query.replica + ", because it's active",
                        ErrorCodes::TABLE_WAS_NOT_DROPPED);

    storage_replicated->dropReplica(zookeeper, status.zookeeper_path, query.replica, log);
    LOG_TRACE(log, "Dropped replica {} of {}", query.replica, table->getStorageID().getNameForLogs());

    return true;
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
        case Type::RELOAD_SYMBOLS:
        {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_SYMBOLS);
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
        case Type::DROP_REPLICA:
        {
            required_access.emplace_back(AccessType::SYSTEM_DROP_REPLICA, query.database, query.table);
            break;
        }
        case Type::RESTORE_REPLICA:
        {
            required_access.emplace_back(AccessType::SYSTEM_RESTORE_REPLICA, query.database, query.table);
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
