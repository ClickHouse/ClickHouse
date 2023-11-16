#include <Interpreters/InterpreterSystemQuery.h>
#include <Common/DNSResolver.h>
#include <Common/ActionLock.h>
#include <Common/typeid_cast.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/SymbolIndex.h>
#include <Common/ThreadPool.h>
#include <Common/escapeForFileName.h>
#include <Common/ShellCommand.h>
#include <Common/CurrentMetrics.h>
#include <Common/FailPoint.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Functions/UserDefined/ExternalUserDefinedExecutableFunctionsLoader.h>
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
#include <Interpreters/AsynchronousInsertLog.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Interpreters/TransactionLog.h>
#include <Interpreters/AsynchronousInsertQueue.h>
#include <BridgeHelper/CatBoostLibraryBridgeHelper.h>
#include <Access/AccessControl.h>
#include <Access/ContextAccess.h>
#include <Access/Common/AllowedClientHosts.h>
#include <Databases/IDatabase.h>
#include <Databases/DatabaseReplicated.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/Freeze.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageFile.h>
#include <Storages/StorageS3.h>
#include <Storages/StorageURL.h>
#include <Storages/StorageAzureBlob.h>
#include <Storages/HDFS/StorageHDFS.h>
#include <Storages/System/StorageSystemFilesystemCache.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Common/ThreadFuzzer.h>
#include <csignal>
#include <algorithm>
#include <unistd.h>

#if USE_AWS_S3
#include <IO/S3/Client.h>
#endif

#include "config.h"

namespace CurrentMetrics
{
    extern const Metric RestartReplicaThreads;
    extern const Metric RestartReplicaThreadsActive;
}

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
    extern const StorageActionBlockType PartsMerge;
    extern const StorageActionBlockType PartsFetch;
    extern const StorageActionBlockType PartsSend;
    extern const StorageActionBlockType ReplicationQueue;
    extern const StorageActionBlockType DistributedSend;
    extern const StorageActionBlockType PartsTTLMerge;
    extern const StorageActionBlockType PartsMove;
    extern const StorageActionBlockType PullReplicationLog;
}


namespace
{

/// Sequentially tries to execute all commands and throws exception with info about failed commands
void executeCommandsAndThrowIfError(std::vector<std::function<void()>> commands)
{
    ExecutionStatus result(0);
    for (auto & command : commands)
    {
        try
        {
            command();
        }
        catch (...)
        {
            ExecutionStatus current_result = ExecutionStatus::fromCurrentException();

            if (result.code == 0)
                result.code = current_result.code;

            if (!current_result.message.empty())
            {
                if (!result.message.empty())
                    result.message += '\n';
                result.message += current_result.message;
            }
        }
    }

    if (result.code != 0)
        throw Exception::createDeprecated(result.message, result.code);
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
    else if (action_type == ActionLocks::PullReplicationLog)
        return AccessType::SYSTEM_PULLING_REPLICATION_LOG;
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown action type: {}", std::to_string(action_type));
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
            startStopActionInDatabase(action_type, start, elem.first, elem.second, getContext(), log);
        }
    }
}

void InterpreterSystemQuery::startStopActionInDatabase(StorageActionBlockType action_type, bool start,
                                                       const String & database_name, const DatabasePtr & database,
                                                       const ContextPtr & local_context, Poco::Logger * log)
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
                LOG_DEBUG(log, "The command {} returned output: {}", command, out.str());
            res->wait();
            break;
        }
        case Type::SYNC_FILE_CACHE:
        {
            LOG_DEBUG(log, "Will perform 'sync' syscall (it can take time).");
            sync();
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
            system_context->clearMarkCache();
            break;
        case Type::DROP_UNCOMPRESSED_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_UNCOMPRESSED_CACHE);
            system_context->clearUncompressedCache();
            break;
        case Type::DROP_INDEX_MARK_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_MARK_CACHE);
            system_context->clearIndexMarkCache();
            break;
        case Type::DROP_INDEX_UNCOMPRESSED_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_UNCOMPRESSED_CACHE);
            system_context->clearIndexUncompressedCache();
            break;
        case Type::DROP_MMAP_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_MMAP_CACHE);
            system_context->clearMMappedFileCache();
            break;
        case Type::DROP_QUERY_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_QUERY_CACHE);
            getContext()->clearQueryCache();
            break;
#if USE_EMBEDDED_COMPILER
        case Type::DROP_COMPILED_EXPRESSION_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_COMPILED_EXPRESSION_CACHE);
            if (auto * cache = CompiledExpressionCacheFactory::instance().tryGetCache())
                cache->clear();
            break;
#endif
#if USE_AWS_S3
        case Type::DROP_S3_CLIENT_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_S3_CLIENT_CACHE);
            S3::ClientCacheRegistry::instance().clearCacheForAll();
            break;
#endif

        case Type::DROP_FILESYSTEM_CACHE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_DROP_FILESYSTEM_CACHE);

            if (query.filesystem_cache_name.empty())
            {
                auto caches = FileCacheFactory::instance().getAll();
                for (const auto & [_, cache_data] : caches)
                    cache_data->cache->removeAllReleasable();
            }
            else
            {
                auto cache = FileCacheFactory::instance().getByName(query.filesystem_cache_name).cache;
                if (query.key_to_drop.empty())
                {
                    cache->removeAllReleasable();
                }
                else
                {
                    auto key = FileCacheKey::fromKeyString(query.key_to_drop);
                    if (query.offset_to_drop.has_value())
                        cache->removeFileSegment(key, query.offset_to_drop.value());
                    else
                        cache->removeKey(key);
                }
            }
            break;
        }
        case Type::SYNC_FILESYSTEM_CACHE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_SYNC_FILESYSTEM_CACHE);

            ColumnsDescription columns{NamesAndTypesList{
                {"cache_name", std::make_shared<DataTypeString>()},
                {"path", std::make_shared<DataTypeString>()},
                {"size", std::make_shared<DataTypeUInt64>()},
            }};
            Block sample_block;
            for (const auto & column : columns)
                sample_block.insert({column.type->createColumn(), column.type, column.name});

            MutableColumns res_columns = sample_block.cloneEmptyColumns();

            auto fill_data = [&](const std::string & cache_name, const FileCachePtr & cache, const FileSegments & file_segments)
            {
                for (const auto & file_segment : file_segments)
                {
                    size_t i = 0;
                    const auto path = cache->getPathInLocalCache(file_segment->key(), file_segment->offset(), file_segment->getKind());
                    res_columns[i++]->insert(cache_name);
                    res_columns[i++]->insert(path);
                    res_columns[i++]->insert(file_segment->getDownloadedSize());
                }
            };

            if (query.filesystem_cache_name.empty())
            {
                auto caches = FileCacheFactory::instance().getAll();
                for (const auto & [cache_name, cache_data] : caches)
                {
                    auto file_segments = cache_data->cache->sync();
                    fill_data(cache_name, cache_data->cache, file_segments);
                }
            }
            else
            {
                auto cache = FileCacheFactory::instance().getByName(query.filesystem_cache_name).cache;
                auto file_segments = cache->sync();
                fill_data(query.filesystem_cache_name, cache, file_segments);
            }

            size_t num_rows = res_columns[0]->size();
            auto source = std::make_shared<SourceFromSingleChunk>(sample_block, Chunk(std::move(res_columns), num_rows));
            result.pipeline = QueryPipeline(std::move(source));
            break;
        }
        case Type::DROP_SCHEMA_CACHE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_DROP_SCHEMA_CACHE);
            std::unordered_set<String> caches_to_drop;
            if (query.schema_cache_storage.empty())
                caches_to_drop = {"FILE", "S3", "HDFS", "URL", "AZURE"};
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
#if USE_AZURE_BLOB_STORAGE
            if (caches_to_drop.contains("AZURE"))
                StorageAzureBlob::getSchemaCache(getContext()).clear();
#endif
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
            executeCommandsAndThrowIfError({
                [&] { system_context->getExternalDictionariesLoader().reloadAllTriedToLoad(); },
                [&] { system_context->getEmbeddedDictionaries().reload(); }
            });
            ExternalDictionariesLoader::resetAll();
            break;
        }
        case Type::RELOAD_MODEL:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_MODEL);
            auto bridge_helper = std::make_unique<CatBoostLibraryBridgeHelper>(getContext(), query.target_model);
            bridge_helper->removeModel();
            break;
        }
        case Type::RELOAD_MODELS:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_MODEL);
            auto bridge_helper = std::make_unique<CatBoostLibraryBridgeHelper>(getContext());
            bridge_helper->removeAllModels();
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
        case Type::RELOAD_USERS:
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_USERS);
            system_context->getAccessControl().reload(AccessControl::ReloadMode::ALL);
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
        case Type::STOP_PULLING_REPLICATION_LOG:
            startStopAction(ActionLocks::PullReplicationLog, false);
            break;
        case Type::START_PULLING_REPLICATION_LOG:
            startStopAction(ActionLocks::PullReplicationLog, true);
            break;
        case Type::DROP_REPLICA:
            dropReplica(query);
            break;
        case Type::DROP_DATABASE_REPLICA:
            dropDatabaseReplica(query);
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
        case Type::WAIT_LOADING_PARTS:
            waitLoadingParts();
            break;
        case Type::RESTART_DISK:
            restartDisk(query.disk);
        case Type::FLUSH_LOGS:
        {
            getContext()->checkAccess(AccessType::SYSTEM_FLUSH_LOGS);

            auto logs = getContext()->getSystemLogs();
            std::vector<std::function<void()>> commands;
            commands.reserve(logs.size());
            for (auto * system_log : logs)
                commands.emplace_back([system_log] { system_log->flush(true); });

            executeCommandsAndThrowIfError(commands);
            break;
        }
        case Type::STOP_LISTEN:
            getContext()->checkAccess(AccessType::SYSTEM_LISTEN);
            getContext()->stopServers(query.server_type);
            break;
        case Type::START_LISTEN:
            getContext()->checkAccess(AccessType::SYSTEM_LISTEN);
            getContext()->startServers(query.server_type);
            break;
        case Type::FLUSH_ASYNC_INSERT_QUEUE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_FLUSH_ASYNC_INSERT_QUEUE);
            auto * queue = getContext()->getAsynchronousInsertQueue();
            if (!queue)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Cannot flush asynchronous insert queue because it is not initialized");

            queue->flushAll();
            break;
        }
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
            result = Unfreezer(getContext()).systemUnfreeze(query.backup_name);
            break;
        }
        case Type::ENABLE_FAILPOINT:
        {
            getContext()->checkAccess(AccessType::SYSTEM_FAILPOINT);
            FailPointInjection::enableFailPoint(query.fail_point_name);
            break;
        }
        case Type::DISABLE_FAILPOINT:
        {
            getContext()->checkAccess(AccessType::SYSTEM_FAILPOINT);
            FailPointInjection::disableFailPoint(query.fail_point_name);
            break;
        }
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown type of SYSTEM query");
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
    LOG_TRACE(log, "Restarting replica {}", replica);
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
    UUID uuid = table->getStorageID().uuid;
    table.reset();
    database->waitDetachedTableNotInUse(uuid);

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
    LOG_TRACE(log, "Restarted replica {}", replica);
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

    size_t threads = std::min(static_cast<size_t>(getNumberOfPhysicalCPUCores()), replica_names.size());
    LOG_DEBUG(log, "Will restart {} replicas using {} threads", replica_names.size(), threads);
    ThreadPool pool(CurrentMetrics::RestartReplicaThreads, CurrentMetrics::RestartReplicaThreadsActive, threads);

    for (auto & replica : replica_names)
    {
        pool.scheduleOrThrowOnError([&]() { tryRestartReplica(replica, system_context, false); });
    }
    pool.wait();
}

void InterpreterSystemQuery::dropReplica(ASTSystemQuery & query)
{
    if (query.replica.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Replica name is empty");

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
                    ReplicatedTableStatus status;
                    storage_replicated->getStatus(status);
                    if (status.zookeeper_path == query.replica_zk_path)
                        throw Exception(ErrorCodes::TABLE_WAS_NOT_DROPPED,
                                        "There is a local table {}, which has the same table path in ZooKeeper. "
                                        "Please check the path in query. "
                                        "If you want to drop replica "
                                        "of this table, use `DROP TABLE` "
                                        "or `SYSTEM DROP REPLICA 'name' FROM db.table`",
                                        storage_replicated->getStorageID().getNameForLogs());
                }
            }
        }

        auto zookeeper = getContext()->getZooKeeper();

        bool looks_like_table_path = zookeeper->exists(query.replica_zk_path + "/replicas") ||
                                     zookeeper->exists(query.replica_zk_path + "/dropped");
        if (!looks_like_table_path)
            throw Exception(ErrorCodes::TABLE_WAS_NOT_DROPPED, "Specified path {} does not look like a table path",
                            query.replica_zk_path);

        if (zookeeper->exists(remote_replica_path + "/is_active"))
            throw Exception(ErrorCodes::TABLE_WAS_NOT_DROPPED, "Can't remove replica: {}, because it's active", query.replica);

        StorageReplicatedMergeTree::dropReplica(zookeeper, query.replica_zk_path, query.replica, log);
        LOG_INFO(log, "Dropped replica {}", remote_replica_path);
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid query");
}

bool InterpreterSystemQuery::dropReplicaImpl(ASTSystemQuery & query, const StoragePtr & table)
{
    auto * storage_replicated = dynamic_cast<StorageReplicatedMergeTree *>(table.get());
    if (!storage_replicated)
        return false;

    ReplicatedTableStatus status;
    storage_replicated->getStatus(status);

    /// Do not allow to drop local replicas and active remote replicas
    if (query.replica == status.replica_name)
        throw Exception(ErrorCodes::TABLE_WAS_NOT_DROPPED,
                        "We can't drop local replica, please use `DROP TABLE` if you want "
                        "to clean the data and drop this replica");

    storage_replicated->dropReplica(status.zookeeper_path, query.replica, log);
    LOG_TRACE(log, "Dropped replica {} of {}", query.replica, table->getStorageID().getNameForLogs());

    return true;
}

void InterpreterSystemQuery::dropDatabaseReplica(ASTSystemQuery & query)
{
    if (query.replica.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Replica name is empty");

    auto check_not_local_replica = [](const DatabaseReplicated * replicated, const ASTSystemQuery & query_)
    {
        if (!query_.replica_zk_path.empty() && fs::path(replicated->getZooKeeperPath()) != fs::path(query_.replica_zk_path))
            return;
        String full_replica_name = query_.shard.empty() ? query_.replica
                                                        : DatabaseReplicated::getFullReplicaName(query_.shard, query_.replica);
        if (replicated->getFullReplicaName() != full_replica_name)
            return;

        throw Exception(ErrorCodes::TABLE_WAS_NOT_DROPPED, "There is a local database {}, which has the same path in ZooKeeper "
                        "and the same replica name. Please check the path in query. "
                        "If you want to drop replica of this database, use `DROP DATABASE`", replicated->getDatabaseName());
    };

    if (query.database)
    {
        getContext()->checkAccess(AccessType::SYSTEM_DROP_REPLICA, query.getDatabase());
        DatabasePtr database = DatabaseCatalog::instance().getDatabase(query.getDatabase());
        if (auto * replicated = dynamic_cast<DatabaseReplicated *>(database.get()))
        {
            check_not_local_replica(replicated, query);
            DatabaseReplicated::dropReplica(replicated, replicated->getZooKeeperPath(), query.shard, query.replica);
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database {} is not Replicated, cannot drop replica", query.getDatabase());
        LOG_TRACE(log, "Dropped replica {} of Replicated database {}", query.replica, backQuoteIfNeed(database->getDatabaseName()));
    }
    else if (query.is_drop_whole_replica)
    {
        auto databases = DatabaseCatalog::instance().getDatabases();
        auto access = getContext()->getAccess();
        bool access_is_granted_globally = access->isGranted(AccessType::SYSTEM_DROP_REPLICA);

        for (auto & elem : databases)
        {
            DatabasePtr & database = elem.second;
            auto * replicated = dynamic_cast<DatabaseReplicated *>(database.get());
            if (!replicated)
                continue;
            if (!access_is_granted_globally && !access->isGranted(AccessType::SYSTEM_DROP_REPLICA, elem.first))
            {
                LOG_INFO(log, "Access {} denied, skipping database {}", "SYSTEM DROP REPLICA", elem.first);
                continue;
            }

            check_not_local_replica(replicated, query);
            DatabaseReplicated::dropReplica(replicated, replicated->getZooKeeperPath(), query.shard, query.replica);
            LOG_TRACE(log, "Dropped replica {} of Replicated database {}", query.replica, backQuoteIfNeed(database->getDatabaseName()));
        }
    }
    else if (!query.replica_zk_path.empty())
    {
        getContext()->checkAccess(AccessType::SYSTEM_DROP_REPLICA);

        /// This check is actually redundant, but it may prevent from some user mistakes
        for (auto & elem : DatabaseCatalog::instance().getDatabases())
            if (auto * replicated = dynamic_cast<DatabaseReplicated *>(elem.second.get()))
                check_not_local_replica(replicated, query);

        DatabaseReplicated::dropReplica(nullptr, query.replica_zk_path, query.shard, query.replica);
        LOG_INFO(log, "Dropped replica {} of Replicated database with path {}", query.replica, query.replica_zk_path);
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid query");
}

void InterpreterSystemQuery::syncReplica(ASTSystemQuery & query)
{
    getContext()->checkAccess(AccessType::SYSTEM_SYNC_REPLICA, table_id);
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());

    if (auto * storage_replicated = dynamic_cast<StorageReplicatedMergeTree *>(table.get()))
    {
        LOG_TRACE(log, "Synchronizing entries in replica's queue with table's log and waiting for current last entry to be processed");
        auto sync_timeout = getContext()->getSettingsRef().receive_timeout.totalMilliseconds();
        if (!storage_replicated->waitForProcessingQueue(sync_timeout, query.sync_replica_mode))
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

void InterpreterSystemQuery::waitLoadingParts()
{
    getContext()->checkAccess(AccessType::SYSTEM_WAIT_LOADING_PARTS, table_id);
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());

    if (auto * merge_tree = dynamic_cast<MergeTreeData *>(table.get()))
    {
        LOG_TRACE(log, "Waiting for loading of parts of table {}", table_id.getFullTableName());
        merge_tree->waitForOutdatedPartsToBeLoaded();
        LOG_TRACE(log, "Finished waiting for loading of parts of table {}", table_id.getFullTableName());
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Command WAIT LOADING PARTS is supported only for MergeTree table, but got: {}", table->getName());
    }
}

void InterpreterSystemQuery::syncReplicatedDatabase(ASTSystemQuery & query)
{
    const auto database_name = query.getDatabase();
    auto guard = DatabaseCatalog::instance().getDDLGuard(database_name, "");
    auto database = DatabaseCatalog::instance().getDatabase(database_name);

    if (auto * ptr = typeid_cast<DatabaseReplicated *>(database.get()))
    {
        LOG_TRACE(log, "Synchronizing entries in the database replica's (name: {}) queue with the log", database_name);
        if (!ptr->waitForReplicaToProcessAllEntries(getContext()->getSettingsRef().receive_timeout.totalMilliseconds()))
        {
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "SYNC DATABASE REPLICA {}: database is readonly or command timed out. " \
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
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table {} is not distributed", table_id.getNameForLogs());
}

[[noreturn]] void InterpreterSystemQuery::restartDisk(String &)
{
    getContext()->checkAccess(AccessType::SYSTEM_RESTART_DISK);
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "SYSTEM RESTART DISK is not supported");
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
        case Type::DROP_QUERY_CACHE:
#if USE_EMBEDDED_COMPILER
        case Type::DROP_COMPILED_EXPRESSION_CACHE:
#endif
        case Type::DROP_UNCOMPRESSED_CACHE:
        case Type::DROP_INDEX_MARK_CACHE:
        case Type::DROP_INDEX_UNCOMPRESSED_CACHE:
        case Type::DROP_FILESYSTEM_CACHE:
        case Type::SYNC_FILESYSTEM_CACHE:
        case Type::DROP_SCHEMA_CACHE:
#if USE_AWS_S3
        case Type::DROP_S3_CLIENT_CACHE:
#endif
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
        case Type::RELOAD_USERS:
        {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_USERS);
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
        case Type::STOP_PULLING_REPLICATION_LOG:
        case Type::START_PULLING_REPLICATION_LOG:
        {
            if (!query.table)
                required_access.emplace_back(AccessType::SYSTEM_PULLING_REPLICATION_LOG);
            else
                required_access.emplace_back(AccessType::SYSTEM_PULLING_REPLICATION_LOG, query.getDatabase(), query.getTable());
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
        case Type::DROP_DATABASE_REPLICA:
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
        case Type::WAIT_LOADING_PARTS:
        {
            required_access.emplace_back(AccessType::SYSTEM_WAIT_LOADING_PARTS, query.getDatabase(), query.getTable());
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
        case Type::FLUSH_ASYNC_INSERT_QUEUE:
        {
            required_access.emplace_back(AccessType::SYSTEM_FLUSH_ASYNC_INSERT_QUEUE);
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
        case Type::SYNC_FILE_CACHE:
        {
            required_access.emplace_back(AccessType::SYSTEM_SYNC_FILE_CACHE);
            break;
        }
        case Type::STOP_LISTEN:
        case Type::START_LISTEN:
        {
            required_access.emplace_back(AccessType::SYSTEM_LISTEN);
            break;
        }
        case Type::STOP_THREAD_FUZZER:
        case Type::START_THREAD_FUZZER:
        case Type::ENABLE_FAILPOINT:
        case Type::DISABLE_FAILPOINT:
        case Type::UNKNOWN:
        case Type::END: break;
    }
    return required_access;
}

}
