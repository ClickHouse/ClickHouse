#include <Interpreters/InterpreterSystemQuery.h>
#include <Common/DNSResolver.h>
#include <Common/ActionLock.h>
#include <Common/typeid_cast.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/SymbolIndex.h>
#include <Common/ThreadPool.h>
#include <Common/escapeForFileName.h>
#include <Common/ShellCommand.h>
#include <Common/FileCacheFactory.h>
#include <Common/IFileCache.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/ExternalModelsLoader.h>
#include <Interpreters/ExternalUserDefinedExecutableFunctionsLoader.h>
#include <Interpreters/EmbeddedDictionaries.h>
#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/QueryThreadLog.h>
#include <Interpreters/QueryViewsLog.h>
#include <Interpreters/SessionLog.h>
#include <Interpreters/TraceLog.h>
#include <Interpreters/TextLog.h>
#include <Interpreters/MetricLog.h>
#include <Interpreters/AsynchronousMetricLog.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/ZooKeeperLog.h>
#include <Interpreters/FilesystemCacheLog.h>
#include <Interpreters/TransactionsInfoLog.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Interpreters/TransactionLog.h>
#include <Access/ContextAccess.h>
#include <Access/Common/AllowedClientHosts.h>
#include <Databases/IDatabase.h>
#include <Databases/DatabaseReplicated.h>
#include <Disks/DiskRestartProxy.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/Freeze.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageFile.h>
#include <Storages/StorageS3.h>
#include <Storages/StorageURL.h>
#include <Storages/HDFS/StorageHDFS.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Common/ThreadFuzzer.h>
#include <csignal>
#include <algorithm>

#include "config_core.h"

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

constexpr std::string_view table_is_not_replicated = "Table {} is not replicated";

}

/// Implements SYSTEM [START|STOP] <something action from ActionLocks>
void InterpreterSystemQuery::startStopAction(StorageActionBlockType action_type, bool start)
{
    auto manager = getContext()->getActionLocksManager();
    manager->cleanExpired();

    auto access = getContext()->getAccess();
    auto required_access_type = getRequiredAccessType(action_type);

    if (volume_ptr && action_type == ActionLocks::PartsMerge)
    {
        access->checkAccess(required_access_type);
        volume_ptr->setAvoidMergesUserOverride(!start);
    }
    else if (table_id)
    {
        access->checkAccess(required_access_type, table_id.database_name, table_id.table_name);
        auto table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
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
        for (auto & elem : DatabaseCatalog::instance().getDatabases())
        {
            for (auto iterator = elem.second->getTablesIterator(getContext()); iterator->isValid(); iterator->next())
            {
                StoragePtr table = iterator->table();
                if (!table)
                    continue;

                if (!access->isGranted(required_access_type, elem.first, iterator->name()))
                {
                    LOG_INFO(log, "Access {} denied, skipping {}.{}", toString(required_access_type), elem.first, iterator->name());
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


InterpreterSystemQuery::InterpreterSystemQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_->clone()), log(&Poco::Logger::get("InterpreterSystemQuery"))
{
}


BlockIO InterpreterSystemQuery::execute()
{
    auto & query = query_ptr->as<ASTSystemQuery &>();

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = getRequiredAccessForDDLOnCluster();
        return executeDDLQueryOnCluster(query_ptr, getContext(), params);
    }

    using Type = ASTSystemQuery::Type;

    /// Use global context with fresh system profile settings
    auto system_context = Context::createCopy(getContext()->getGlobalContext());
    system_context->setSetting("profile", getContext()->getSystemProfileName());

    /// Make canonical query for simpler processing
    if (query.type == Type::RELOAD_DICTIONARY)
    {
        if (query.database)
            query.setTable(query.getDatabase() + "." + query.getTable());
    }
    else if (query.table)
    {
        table_id = getContext()->resolveStorageID(StorageID(query.getDatabase(), query.getTable()), Context::ResolveOrdinary);
    }


    BlockIO result;

    volume_ptr = {};
    if (!query.storage_policy.empty() && !query.volume.empty())
        volume_ptr = getContext()->getStoragePolicy(query.storage_policy)->getVolumeByName(query.volume);

    switch (query.type)
    {
        case Type::SHUTDOWN:
        {
            getContext()->checkAccess(AccessType::SYSTEM_SHUTDOWN);
            if (kill(0, SIGTERM))
                throwFromErrno("System call kill(0, SIGTERM) failed", ErrorCodes::CANNOT_KILL);
            break;
        }
        case Type::KILL:
        {
            getContext()->checkAccess(AccessType::SYSTEM_SHUTDOWN);
            /// Exit with the same code as it is usually set by shell when process is terminated by SIGKILL.
            /// It's better than doing 'raise' or 'kill', because they have no effect for 'init' process (with pid = 0, usually in Docker).
            LOG_INFO(log, "Exit immediately as the SYSTEM KILL command has been issued.");
            _exit(128 + SIGKILL);
            // break; /// unreachable
        }
        case Type::SUSPEND:
        {
            getContext()->checkAccess(AccessType::SYSTEM_SHUTDOWN);
            auto command = fmt::format("kill -STOP {0} && sleep {1} && kill -CONT {0}", getpid(), query.seconds);
            LOG_DEBUG(log, "Will run {}", command);
            auto res = ShellCommand::execute(command);
            res->in.close();
            WriteBufferFromOwnString out;
            copyData(res->out, out);
            copyData(res->err, out);
            if (!out.str().empty())
                LOG_DEBUG(log, "The command returned output: {}", command, out.str());
            res->wait();
            break;
        }
        case Type::DROP_DNS_CACHE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_DROP_DNS_CACHE);
            DNSResolver::instance().dropCache();
            /// Reinitialize clusters to update their resolved_addresses
            system_context->reloadClusterConfig();
            break;
        }
        case Type::DROP_MARK_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_MARK_CACHE);
            system_context->dropMarkCache();
            break;
        case Type::DROP_UNCOMPRESSED_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_UNCOMPRESSED_CACHE);
            system_context->dropUncompressedCache();
            break;
        case Type::DROP_INDEX_MARK_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_MARK_CACHE);
            system_context->dropIndexMarkCache();
            break;
        case Type::DROP_INDEX_UNCOMPRESSED_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_UNCOMPRESSED_CACHE);
            system_context->dropIndexUncompressedCache();
            break;
        case Type::DROP_MMAP_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_MMAP_CACHE);
            system_context->dropMMappedFileCache();
            break;
#if USE_EMBEDDED_COMPILER
        case Type::DROP_COMPILED_EXPRESSION_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_COMPILED_EXPRESSION_CACHE);
            if (auto * cache = CompiledExpressionCacheFactory::instance().tryGetCache())
                cache->reset();
            break;
#endif
        case Type::DROP_FILESYSTEM_CACHE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_DROP_FILESYSTEM_CACHE);
            if (query.filesystem_cache_path.empty())
            {
                auto caches = FileCacheFactory::instance().getAll();
                for (const auto & [_, cache_data] : caches)
                    cache_data->cache->removeIfReleasable(/* remove_persistent_files */false);
            }
            else
            {
                auto cache = FileCacheFactory::instance().get(query.filesystem_cache_path);
                cache->removeIfReleasable(/* remove_persistent_files */false);
            }
            break;
        }
        case Type::DROP_SCHEMA_CACHE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_DROP_SCHEMA_CACHE);
            std::unordered_set<String> caches_to_drop;
            if (query.schema_cache_storage.empty())
                caches_to_drop = {"FILE", "S3", "HDFS", "URL"};
            else
                caches_to_drop = {query.schema_cache_storage};

            if (caches_to_drop.contains("FILE"))
                StorageFile::getSchemaCache(getContext()).clear();
#if USE_AWS_S3
            if (caches_to_drop.contains("S3"))
                StorageS3::getSchemaCache(getContext()).clear();
#endif
#if USE_HDFS
            if (caches_to_drop.contains("HDFS"))
                StorageHDFS::getSchemaCache(getContext()).clear();
#endif
            if (caches_to_drop.contains("URL"))
                StorageURL::getSchemaCache(getContext()).clear();
            break;
        }
        case Type::RELOAD_DICTIONARY:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_DICTIONARY);

            auto & external_dictionaries_loader = system_context->getExternalDictionariesLoader();
            external_dictionaries_loader.reloadDictionary(query.getTable(), getContext());

            ExternalDictionariesLoader::resetAll();
            break;
        }
        case Type::RELOAD_DICTIONARIES:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_DICTIONARY);
            executeCommandsAndThrowIfError(
                [&] { system_context->getExternalDictionariesLoader().reloadAllTriedToLoad(); },
                [&] { system_context->getEmbeddedDictionaries().reload(); }
            );
            ExternalDictionariesLoader::resetAll();
            break;
        }
        case Type::RELOAD_MODEL:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_MODEL);

            auto & external_models_loader = system_context->getExternalModelsLoader();
            external_models_loader.reloadModel(query.target_model);
            break;
        }
        case Type::RELOAD_MODELS:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_MODEL);

            auto & external_models_loader = system_context->getExternalModelsLoader();
            external_models_loader.reloadAllTriedToLoad();
            break;
        }
        case Type::RELOAD_FUNCTION:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_FUNCTION);

            auto & external_user_defined_executable_functions_loader = system_context->getExternalUserDefinedExecutableFunctionsLoader();
            external_user_defined_executable_functions_loader.reloadFunction(query.target_function);
            break;
        }
        case Type::RELOAD_FUNCTIONS:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_FUNCTION);

            auto & external_user_defined_executable_functions_loader = system_context->getExternalUserDefinedExecutableFunctionsLoader();
            external_user_defined_executable_functions_loader.reloadAllTriedToLoad();
            break;
        }
        case Type::RELOAD_EMBEDDED_DICTIONARIES:
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_EMBEDDED_DICTIONARIES);
            system_context->getEmbeddedDictionaries().reload();
            break;
        case Type::RELOAD_CONFIG:
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_CONFIG);
            system_context->reloadConfig();
            break;
        case Type::RELOAD_SYMBOLS:
        {
#if defined(__ELF__) && !defined(OS_FREEBSD)
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_SYMBOLS);
            SymbolIndex::reload();
            break;
#else
            throw Exception("SYSTEM RELOAD SYMBOLS is not supported on current platform", ErrorCodes::NOT_IMPLEMENTED);
#endif
        }
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
        case Type::SYNC_DATABASE_REPLICA:
            syncReplicatedDatabase(query);
            break;
        case Type::SYNC_TRANSACTION_LOG:
            syncTransactionLog();
            break;
        case Type::FLUSH_DISTRIBUTED:
            flushDistributed(query);
            break;
        case Type::RESTART_REPLICAS:
            restartReplicas(system_context);
            break;
        case Type::RESTART_REPLICA:
            restartReplica(table_id, system_context);
            break;
        case Type::RESTORE_REPLICA:
            restoreReplica();
            break;
        case Type::RESTART_DISK:
            restartDisk(query.disk);
            break;
        case Type::FLUSH_LOGS:
        {
            getContext()->checkAccess(AccessType::SYSTEM_FLUSH_LOGS);
            executeCommandsAndThrowIfError(
                [&] { if (auto query_log = getContext()->getQueryLog()) query_log->flush(true); },
                [&] { if (auto part_log = getContext()->getPartLog("")) part_log->flush(true); },
                [&] { if (auto query_thread_log = getContext()->getQueryThreadLog()) query_thread_log->flush(true); },
                [&] { if (auto trace_log = getContext()->getTraceLog()) trace_log->flush(true); },
                [&] { if (auto text_log = getContext()->getTextLog()) text_log->flush(true); },
                [&] { if (auto metric_log = getContext()->getMetricLog()) metric_log->flush(true); },
                [&] { if (auto asynchronous_metric_log = getContext()->getAsynchronousMetricLog()) asynchronous_metric_log->flush(true); },
                [&] { if (auto opentelemetry_span_log = getContext()->getOpenTelemetrySpanLog()) opentelemetry_span_log->flush(true); },
                [&] { if (auto query_views_log = getContext()->getQueryViewsLog()) query_views_log->flush(true); },
                [&] { if (auto zookeeper_log = getContext()->getZooKeeperLog()) zookeeper_log->flush(true); },
                [&] { if (auto session_log = getContext()->getSessionLog()) session_log->flush(true); },
                [&] { if (auto transactions_info_log = getContext()->getTransactionsInfoLog()) transactions_info_log->flush(true); },
                [&] { if (auto processors_profile_log = getContext()->getProcessorsProfileLog()) processors_profile_log->flush(true); },
                [&] { if (auto cache_log = getContext()->getFilesystemCacheLog()) cache_log->flush(true); }
            );
            break;
        }
        case Type::STOP_LISTEN_QUERIES:
        case Type::START_LISTEN_QUERIES:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not supported yet", query.type);
        case Type::STOP_THREAD_FUZZER:
            getContext()->checkAccess(AccessType::SYSTEM_THREAD_FUZZER);
            ThreadFuzzer::stop();
            break;
        case Type::START_THREAD_FUZZER:
            getContext()->checkAccess(AccessType::SYSTEM_THREAD_FUZZER);
            ThreadFuzzer::start();
            break;
        case Type::UNFREEZE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_UNFREEZE);
            /// The result contains information about deleted parts as a table. It is for compatibility with ALTER TABLE UNFREEZE query.
            result = Unfreezer().unfreeze(query.backup_name, getContext());
            break;
        }
        default:
            throw Exception("Unknown type of SYSTEM query", ErrorCodes::BAD_ARGUMENTS);
    }

    return result;
}

void InterpreterSystemQuery::restoreReplica()
{
    getContext()->checkAccess(AccessType::SYSTEM_RESTORE_REPLICA, table_id);

    const StoragePtr table_ptr = DatabaseCatalog::instance().getTable(table_id, getContext());

    auto * const table_replicated_ptr = dynamic_cast<StorageReplicatedMergeTree *>(table_ptr.get());

    if (table_replicated_ptr == nullptr)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, table_is_not_replicated.data(), table_id.getNameForLogs());

    table_replicated_ptr->restoreMetadataInZooKeeper();
}

StoragePtr InterpreterSystemQuery::tryRestartReplica(const StorageID & replica, ContextMutablePtr system_context, bool need_ddl_guard)
{
    auto table_ddl_guard = need_ddl_guard
        ? DatabaseCatalog::instance().getDDLGuard(replica.getDatabaseName(), replica.getTableName())
        : nullptr;

    auto [database, table] = DatabaseCatalog::instance().tryGetDatabaseAndTable(replica, getContext());
    ASTPtr create_ast;

    /// Detach actions
    if (!table || !dynamic_cast<const StorageReplicatedMergeTree *>(table.get()))
        return nullptr;

    table->flushAndShutdown();
    {
        /// If table was already dropped by anyone, an exception will be thrown
        auto table_lock = table->lockExclusively(getContext()->getCurrentQueryId(), getContext()->getSettingsRef().lock_acquire_timeout);
        create_ast = database->getCreateTableQuery(replica.table_name, getContext());

        database->detachTable(system_context, replica.table_name);
    }
    table.reset();

    /// Attach actions
    /// getCreateTableQuery must return canonical CREATE query representation, there are no need for AST postprocessing
    auto & create = create_ast->as<ASTCreateQuery &>();
    create.attach = true;

    auto columns = InterpreterCreateQuery::getColumnsDescription(*create.columns_list->columns, system_context, true);
    auto constraints = InterpreterCreateQuery::getConstraintsDescription(create.columns_list->constraints);
    auto data_path = database->getTableDataPath(create);

    table = StorageFactory::instance().get(create,
        data_path,
        system_context,
        system_context->getGlobalContext(),
        columns,
        constraints,
        false);

    database->attachTable(system_context, replica.table_name, table, data_path);

    table->startup();
    return table;
}

void InterpreterSystemQuery::restartReplica(const StorageID & replica, ContextMutablePtr system_context)
{
    getContext()->checkAccess(AccessType::SYSTEM_RESTART_REPLICA, replica);
    if (!tryRestartReplica(replica, system_context))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, table_is_not_replicated.data(), replica.getNameForLogs());
}

void InterpreterSystemQuery::restartReplicas(ContextMutablePtr system_context)
{
    std::vector<StorageID> replica_names;
    auto & catalog = DatabaseCatalog::instance();

    auto access = getContext()->getAccess();
    bool access_is_granted_globally = access->isGranted(AccessType::SYSTEM_RESTART_REPLICA);

    for (auto & elem : catalog.getDatabases())
    {
        for (auto it = elem.second->getTablesIterator(getContext()); it->isValid(); it->next())
        {
            if (dynamic_cast<const StorageReplicatedMergeTree *>(it->table().get()))
            {
                if (!access_is_granted_globally && !access->isGranted(AccessType::SYSTEM_RESTART_REPLICA, elem.first, it->name()))
                {
                    LOG_INFO(log, "Access {} denied, skipping {}.{}", "SYSTEM RESTART REPLICA", elem.first, it->name());
                    continue;
                }
                replica_names.emplace_back(it->databaseName(), it->name());
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

    ThreadPool pool(std::min(static_cast<size_t>(getNumberOfPhysicalCPUCores()), replica_names.size()));

    for (auto & replica : replica_names)
    {
        LOG_TRACE(log, "Restarting replica on {}", replica.getNameForLogs());
        pool.scheduleOrThrowOnError([&]() { tryRestartReplica(replica, system_context, false); });
    }
    pool.wait();
}

void InterpreterSystemQuery::dropReplica(ASTSystemQuery & query)
{
    if (query.replica.empty())
        throw Exception("Replica name is empty", ErrorCodes::BAD_ARGUMENTS);

    if (!table_id.empty())
    {
        getContext()->checkAccess(AccessType::SYSTEM_DROP_REPLICA, table_id);
        StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());

        if (!dropReplicaImpl(query, table))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, table_is_not_replicated.data(), table_id.getNameForLogs());
    }
    else if (query.database)
    {
        getContext()->checkAccess(AccessType::SYSTEM_DROP_REPLICA, query.getDatabase());
        DatabasePtr database = DatabaseCatalog::instance().getDatabase(query.getDatabase());
        for (auto iterator = database->getTablesIterator(getContext()); iterator->isValid(); iterator->next())
            dropReplicaImpl(query, iterator->table());
        LOG_TRACE(log, "Dropped replica {} from database {}", query.replica, backQuoteIfNeed(database->getDatabaseName()));
    }
    else if (query.is_drop_whole_replica)
    {
        auto databases = DatabaseCatalog::instance().getDatabases();
        auto access = getContext()->getAccess();
        bool access_is_granted_globally = access->isGranted(AccessType::SYSTEM_DROP_REPLICA);

        for (auto & elem : databases)
        {
            DatabasePtr & database = elem.second;
            for (auto iterator = database->getTablesIterator(getContext()); iterator->isValid(); iterator->next())
            {
                if (!access_is_granted_globally && !access->isGranted(AccessType::SYSTEM_DROP_REPLICA, elem.first, iterator->name()))
                {
                    LOG_INFO(log, "Access {} denied, skipping {}.{}", "SYSTEM DROP REPLICA", elem.first, iterator->name());
                    continue;
                }
                dropReplicaImpl(query, iterator->table());
            }
            LOG_TRACE(log, "Dropped replica {} from database {}", query.replica, backQuoteIfNeed(database->getDatabaseName()));
        }
    }
    else if (!query.replica_zk_path.empty())
    {
        getContext()->checkAccess(AccessType::SYSTEM_DROP_REPLICA);
        String remote_replica_path = fs::path(query.replica_zk_path)  / "replicas" / query.replica;

        /// This check is actually redundant, but it may prevent from some user mistakes
        for (auto & elem : DatabaseCatalog::instance().getDatabases())
        {
            DatabasePtr & database = elem.second;
            for (auto iterator = database->getTablesIterator(getContext()); iterator->isValid(); iterator->next())
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

        auto zookeeper = getContext()->getZooKeeper();

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
    auto zookeeper = getContext()->getZooKeeper();
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
    getContext()->checkAccess(AccessType::SYSTEM_SYNC_REPLICA, table_id);
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());

    if (auto * storage_replicated = dynamic_cast<StorageReplicatedMergeTree *>(table.get()))
    {
        LOG_TRACE(log, "Synchronizing entries in replica's queue with table's log and waiting for it to become empty");
        if (!storage_replicated->waitForShrinkingQueueSize(0, getContext()->getSettingsRef().receive_timeout.totalMilliseconds()))
        {
            LOG_ERROR(log, "SYNC REPLICA {}: Timed out!", table_id.getNameForLogs());
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "SYNC REPLICA {}: command timed out. " \
                    "See the 'receive_timeout' setting", table_id.getNameForLogs());
        }
        LOG_TRACE(log, "SYNC REPLICA {}: OK", table_id.getNameForLogs());
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, table_is_not_replicated.data(), table_id.getNameForLogs());
}


void InterpreterSystemQuery::syncReplicatedDatabase(ASTSystemQuery & query)
{
    const auto database_name = query.getDatabase();
    auto database = DatabaseCatalog::instance().getDatabase(database_name);

    if (auto * ptr = typeid_cast<DatabaseReplicated *>(database.get()))
    {
        LOG_TRACE(log, "Synchronizing entries in the database replica's (name: {}) queue with the log", database_name);
        if (!ptr->waitForReplicaToProcessAllEntries(getContext()->getSettingsRef().receive_timeout.totalMilliseconds()))
        {
            LOG_ERROR(log, "SYNC DATABASE REPLICA {}: Timed out!", database_name);
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "SYNC DATABASE REPLICA {}: command timed out. " \
                    "See the 'receive_timeout' setting", database_name);
        }
        LOG_TRACE(log, "SYNC DATABASE REPLICA {}: OK", database_name);
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SYSTEM SYNC DATABASE REPLICA query is intended to work only with Replicated engine");
}


void InterpreterSystemQuery::syncTransactionLog()
{
    getContext()->checkTransactionsAreAllowed(/* explicit_tcl_query */ true);
    TransactionLog::instance().sync();
}


void InterpreterSystemQuery::flushDistributed(ASTSystemQuery &)
{
    getContext()->checkAccess(AccessType::SYSTEM_FLUSH_DISTRIBUTED, table_id);

    if (auto * storage_distributed = dynamic_cast<StorageDistributed *>(DatabaseCatalog::instance().getTable(table_id, getContext()).get()))
        storage_distributed->flushClusterNodesAllData(getContext());
    else
        throw Exception("Table " + table_id.getNameForLogs() + " is not distributed", ErrorCodes::BAD_ARGUMENTS);
}

void InterpreterSystemQuery::restartDisk(String & name)
{
    getContext()->checkAccess(AccessType::SYSTEM_RESTART_DISK);

    auto disk = getContext()->getDisk(name);

    if (DiskRestartProxy * restart_proxy = dynamic_cast<DiskRestartProxy*>(disk.get()))
        restart_proxy->restart(getContext());
    else
        throw Exception("Disk " + name + " doesn't have possibility to restart", ErrorCodes::BAD_ARGUMENTS);
}


AccessRightsElements InterpreterSystemQuery::getRequiredAccessForDDLOnCluster() const
{
    const auto & query = query_ptr->as<const ASTSystemQuery &>();
    using Type = ASTSystemQuery::Type;
    AccessRightsElements required_access;

    switch (query.type)
    {
        case Type::SHUTDOWN:
        case Type::KILL:
        case Type::SUSPEND:
        {
            required_access.emplace_back(AccessType::SYSTEM_SHUTDOWN);
            break;
        }
        case Type::DROP_DNS_CACHE:
        case Type::DROP_MARK_CACHE:
        case Type::DROP_MMAP_CACHE:
#if USE_EMBEDDED_COMPILER
        case Type::DROP_COMPILED_EXPRESSION_CACHE:
#endif
        case Type::DROP_UNCOMPRESSED_CACHE:
        case Type::DROP_INDEX_MARK_CACHE:
        case Type::DROP_INDEX_UNCOMPRESSED_CACHE:
        case Type::DROP_FILESYSTEM_CACHE:
        case Type::DROP_SCHEMA_CACHE:
        {
            required_access.emplace_back(AccessType::SYSTEM_DROP_CACHE);
            break;
        }
        case Type::RELOAD_DICTIONARY:
        case Type::RELOAD_DICTIONARIES:
        case Type::RELOAD_EMBEDDED_DICTIONARIES:
        {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_DICTIONARY);
            break;
        }
        case Type::RELOAD_MODEL:
        case Type::RELOAD_MODELS:
        {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_MODEL);
            break;
        }
        case Type::RELOAD_FUNCTION:
        case Type::RELOAD_FUNCTIONS:
        {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_FUNCTION);
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
        case Type::STOP_MERGES:
        case Type::START_MERGES:
        {
            if (!query.table)
                required_access.emplace_back(AccessType::SYSTEM_MERGES);
            else
                required_access.emplace_back(AccessType::SYSTEM_MERGES, query.getDatabase(), query.getTable());
            break;
        }
        case Type::STOP_TTL_MERGES:
        case Type::START_TTL_MERGES:
        {
            if (!query.table)
                required_access.emplace_back(AccessType::SYSTEM_TTL_MERGES);
            else
                required_access.emplace_back(AccessType::SYSTEM_TTL_MERGES, query.getDatabase(), query.getTable());
            break;
        }
        case Type::STOP_MOVES:
        case Type::START_MOVES:
        {
            if (!query.table)
                required_access.emplace_back(AccessType::SYSTEM_MOVES);
            else
                required_access.emplace_back(AccessType::SYSTEM_MOVES, query.getDatabase(), query.getTable());
            break;
        }
        case Type::STOP_FETCHES:
        case Type::START_FETCHES:
        {
            if (!query.table)
                required_access.emplace_back(AccessType::SYSTEM_FETCHES);
            else
                required_access.emplace_back(AccessType::SYSTEM_FETCHES, query.getDatabase(), query.getTable());
            break;
        }
        case Type::STOP_DISTRIBUTED_SENDS:
        case Type::START_DISTRIBUTED_SENDS:
        {
            if (!query.table)
                required_access.emplace_back(AccessType::SYSTEM_DISTRIBUTED_SENDS);
            else
                required_access.emplace_back(AccessType::SYSTEM_DISTRIBUTED_SENDS, query.getDatabase(), query.getTable());
            break;
        }
        case Type::STOP_REPLICATED_SENDS:
        case Type::START_REPLICATED_SENDS:
        {
            if (!query.table)
                required_access.emplace_back(AccessType::SYSTEM_REPLICATED_SENDS);
            else
                required_access.emplace_back(AccessType::SYSTEM_REPLICATED_SENDS, query.getDatabase(), query.getTable());
            break;
        }
        case Type::STOP_REPLICATION_QUEUES:
        case Type::START_REPLICATION_QUEUES:
        {
            if (!query.table)
                required_access.emplace_back(AccessType::SYSTEM_REPLICATION_QUEUES);
            else
                required_access.emplace_back(AccessType::SYSTEM_REPLICATION_QUEUES, query.getDatabase(), query.getTable());
            break;
        }
        case Type::DROP_REPLICA:
        {
            required_access.emplace_back(AccessType::SYSTEM_DROP_REPLICA, query.getDatabase(), query.getTable());
            break;
        }
        case Type::RESTORE_REPLICA:
        {
            required_access.emplace_back(AccessType::SYSTEM_RESTORE_REPLICA, query.getDatabase(), query.getTable());
            break;
        }
        case Type::SYNC_REPLICA:
        {
            required_access.emplace_back(AccessType::SYSTEM_SYNC_REPLICA, query.getDatabase(), query.getTable());
            break;
        }
        case Type::RESTART_REPLICA:
        {
            required_access.emplace_back(AccessType::SYSTEM_RESTART_REPLICA, query.getDatabase(), query.getTable());
            break;
        }
        case Type::RESTART_REPLICAS:
        {
            required_access.emplace_back(AccessType::SYSTEM_RESTART_REPLICA);
            break;
        }
        case Type::SYNC_DATABASE_REPLICA:
        {
            required_access.emplace_back(AccessType::SYSTEM_SYNC_DATABASE_REPLICA, query.getDatabase());
            break;
        }
        case Type::SYNC_TRANSACTION_LOG:
        {
            required_access.emplace_back(AccessType::SYSTEM_SYNC_TRANSACTION_LOG);
            break;
        }
        case Type::FLUSH_DISTRIBUTED:
        {
            required_access.emplace_back(AccessType::SYSTEM_FLUSH_DISTRIBUTED, query.getDatabase(), query.getTable());
            break;
        }
        case Type::FLUSH_LOGS:
        {
            required_access.emplace_back(AccessType::SYSTEM_FLUSH_LOGS);
            break;
        }
        case Type::RESTART_DISK:
        {
            required_access.emplace_back(AccessType::SYSTEM_RESTART_DISK);
            break;
        }
        case Type::UNFREEZE:
        {
            required_access.emplace_back(AccessType::SYSTEM_UNFREEZE);
            break;
        }
        case Type::STOP_LISTEN_QUERIES:
        case Type::START_LISTEN_QUERIES:
        case Type::STOP_THREAD_FUZZER:
        case Type::START_THREAD_FUZZER:
        case Type::UNKNOWN:
        case Type::END: break;
    }
    return required_access;
}

void InterpreterSystemQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & /*ast*/, ContextPtr) const
{
    elem.query_kind = "System";
}

}
