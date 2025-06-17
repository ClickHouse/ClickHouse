#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterSystemQuery.h>
#include <Common/DNSResolver.h>
#include <Common/ActionLock.h>
#include <Common/typeid_cast.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/SymbolIndex.h>
#include <Common/ThreadPool.h>
#include <Common/escapeForFileName.h>
#include <Common/ShellCommand.h>
#include <Common/CurrentMetrics.h>
#include <Common/FailPoint.h>
#include <Common/PageCache.h>
#include <Common/HostResolvePool.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Functions/UserDefined/ExternalUserDefinedExecutableFunctionsLoader.h>
#include <Interpreters/EmbeddedDictionaries.h>
#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
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
#include <Interpreters/BackupLog.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Interpreters/TransactionLog.h>
#include <Interpreters/AsynchronousInsertQueue.h>
#include <BridgeHelper/CatBoostLibraryBridgeHelper.h>
#include <Access/AccessControl.h>
#include <Access/ContextAccess.h>
#include <Access/Common/AllowedClientHosts.h>
#include <Databases/DatabaseReplicated.h>
#include <DataTypes/DataTypeString.h>
#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/Freeze.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageFile.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageURL.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Storages/MaterializedView/RefreshTask.h>
#include <Storages/System/StorageSystemFilesystemCache.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Common/ThreadFuzzer.h>
#include <base/coverage.h>
#include <csignal>
#include <algorithm>
#include <unistd.h>

#if USE_PROTOBUF
#include <Formats/ProtobufSchemas.h>
#endif

#if USE_AWS_S3
#include <IO/S3/Client.h>
#endif

#if USE_JEMALLOC
#include <Common/Jemalloc.h>
#endif

#include "config.h"

namespace CurrentMetrics
{
    extern const Metric RestartReplicaThreads;
    extern const Metric RestartReplicaThreadsActive;
    extern const Metric RestartReplicaThreadsScheduled;
    extern const Metric MergeTreePartsLoaderThreads;
    extern const Metric MergeTreePartsLoaderThreadsActive;
    extern const Metric MergeTreePartsLoaderThreadsScheduled;
}

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 keeper_max_retries;
    extern const SettingsUInt64 keeper_retry_initial_backoff_ms;
    extern const SettingsUInt64 keeper_retry_max_backoff_ms;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsSeconds receive_timeout;
    extern const SettingsMaxThreads max_threads;
}

namespace ServerSetting
{
    extern const ServerSettingsDouble cannot_allocate_thread_fault_injection_probability;
}

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_KILL;
    extern const int NOT_IMPLEMENTED;
    extern const int TIMEOUT_EXCEEDED;
    extern const int TABLE_WAS_NOT_DROPPED;
    extern const int ABORTED;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TOO_DEEP_RECURSION;
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


InterpreterSystemQuery::InterpreterSystemQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_->clone()), log(getLogger("InterpreterSystemQuery"))
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
    /// Don't check for constraints when changing profile. It was accepted before (for example it might include
    /// some experimental settings)
    bool check_constraints = false;
    system_context->setCurrentProfile(getContext()->getSystemProfileName(), check_constraints);

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
                throw ErrnoException(ErrorCodes::CANNOT_KILL, "System call kill(0, SIGTERM) failed");
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
            HostResolversPool::instance().dropCache();
            /// Reinitialize clusters to update their resolved_addresses
            system_context->reloadClusterConfig();
            break;
        }
        case Type::DROP_CONNECTIONS_CACHE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_DROP_CONNECTIONS_CACHE);
            HTTPConnectionPools::instance().dropCache();
            break;
        }
        case Type::PREWARM_MARK_CACHE:
        {
            prewarmMarkCache();
            break;
        }
        case Type::PREWARM_PRIMARY_INDEX_CACHE:
        {
            prewarmPrimaryIndexCache();
            break;
        }
        case Type::DROP_MARK_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_MARK_CACHE);
            system_context->clearMarkCache();
            break;
        case Type::DROP_ICEBERG_METADATA_CACHE:
#if USE_AVRO
            getContext()->checkAccess(AccessType::SYSTEM_DROP_ICEBERG_METADATA_CACHE);
            system_context->clearIcebergMetadataFilesCache();
            break;
#else
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "The server was compiled without the support for AVRO");
#endif
        case Type::DROP_PRIMARY_INDEX_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_PRIMARY_INDEX_CACHE);
            system_context->clearPrimaryIndexCache();
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
        case Type::DROP_VECTOR_SIMILARITY_INDEX_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_VECTOR_SIMILARITY_INDEX_CACHE);
            system_context->clearVectorSimilarityIndexCache();
            break;
        case Type::DROP_MMAP_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_MMAP_CACHE);
            system_context->clearMMappedFileCache();
            break;
        case Type::DROP_QUERY_CONDITION_CACHE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_DROP_QUERY_CONDITION_CACHE);
            getContext()->clearQueryConditionCache();
            break;
        }
        case Type::DROP_QUERY_CACHE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_DROP_QUERY_CACHE);
            getContext()->clearQueryResultCache(query.query_result_cache_tag);
            break;
        }
        case Type::DROP_COMPILED_EXPRESSION_CACHE:
#if USE_EMBEDDED_COMPILER
            getContext()->checkAccess(AccessType::SYSTEM_DROP_COMPILED_EXPRESSION_CACHE);
            if (auto * cache = CompiledExpressionCacheFactory::instance().tryGetCache())
                cache->clear();
            break;
#else
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "The server was compiled without the support for JIT compilation");
#endif
        case Type::DROP_S3_CLIENT_CACHE:
#if USE_AWS_S3
            getContext()->checkAccess(AccessType::SYSTEM_DROP_S3_CLIENT_CACHE);
            S3::ClientCacheRegistry::instance().clearCacheForAll();
            break;
#else
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "The server was compiled without the support for AWS S3");
#endif

        case Type::DROP_FILESYSTEM_CACHE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_DROP_FILESYSTEM_CACHE);
            const auto user_id = FileCache::getCommonUser().user_id;

            if (query.filesystem_cache_name.empty())
            {
                auto caches = FileCacheFactory::instance().getAll();
                for (const auto & [_, cache_data] : caches)
                {
                    if (!cache_data->cache->isInitialized())
                        continue;

                    cache_data->cache->removeAllReleasable(user_id);
                }
            }
            else
            {
                auto cache = FileCacheFactory::instance().getByName(query.filesystem_cache_name)->cache;

                if (cache->isInitialized())
                {
                    if (query.key_to_drop.empty())
                    {
                        cache->removeAllReleasable(user_id);
                    }
                    else
                    {
                        auto key = FileCacheKey::fromKeyString(query.key_to_drop);
                        if (query.offset_to_drop.has_value())
                            cache->removeFileSegment(key, query.offset_to_drop.value(), user_id);
                        else
                            cache->removeKey(key, user_id);
                    }
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

            auto fill_data = [&](const std::string & cache_name, const FileCachePtr & cache, const std::vector<FileSegment::Info> & file_segments)
            {
                for (const auto & file_segment : file_segments)
                {
                    size_t i = 0;
                    const auto path = cache->getFileSegmentPath(
                        file_segment.key, file_segment.offset, file_segment.kind,
                        FileCache::UserInfo(file_segment.user_id, file_segment.user_weight));
                    res_columns[i++]->insert(cache_name);
                    res_columns[i++]->insert(path);
                    res_columns[i++]->insert(file_segment.downloaded_size);
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
                auto cache = FileCacheFactory::instance().getByName(query.filesystem_cache_name)->cache;
                auto file_segments = cache->sync();
                fill_data(query.filesystem_cache_name, cache, file_segments);
            }

            size_t num_rows = res_columns[0]->size();
            auto source = std::make_shared<SourceFromSingleChunk>(sample_block, Chunk(std::move(res_columns), num_rows));
            result.pipeline = QueryPipeline(std::move(source));
            break;
        }
        case Type::DROP_DISK_METADATA_CACHE:
        {
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Not implemented");
        }
        case Type::DROP_PAGE_CACHE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_DROP_PAGE_CACHE);
            system_context->clearPageCache();
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
                StorageObjectStorage::getSchemaCache(getContext(), StorageS3Configuration::type_name).clear();
#endif
#if USE_HDFS
            if (caches_to_drop.contains("HDFS"))
                StorageObjectStorage::getSchemaCache(getContext(), StorageHDFSConfiguration::type_name).clear();
#endif
            if (caches_to_drop.contains("URL"))
                StorageURL::getSchemaCache(getContext()).clear();
#if USE_AZURE_BLOB_STORAGE
            if (caches_to_drop.contains("AZURE"))
                StorageObjectStorage::getSchemaCache(getContext(), StorageAzureConfiguration::type_name).clear();
#endif
            break;
        }
        case Type::DROP_FORMAT_SCHEMA_CACHE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_DROP_FORMAT_SCHEMA_CACHE);
            std::unordered_set<String> caches_to_drop;
            if (query.schema_cache_format.empty())
                caches_to_drop = {"Protobuf"};
            else
                caches_to_drop = {query.schema_cache_format};
#if USE_PROTOBUF
            if (caches_to_drop.contains("Protobuf"))
                ProtobufSchemas::instance().clear();
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
        case Type::RELOAD_ASYNCHRONOUS_METRICS:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_ASYNCHRONOUS_METRICS);
            auto * asynchronous_metrics = system_context->getAsynchronousMetrics();
            if (asynchronous_metrics)
                asynchronous_metrics->update(std::chrono::system_clock::now(), /*force_update*/ true);
            break;
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
        case Type::STOP_PULLING_REPLICATION_LOG:
            startStopAction(ActionLocks::PullReplicationLog, false);
            break;
        case Type::START_PULLING_REPLICATION_LOG:
            startStopAction(ActionLocks::PullReplicationLog, true);
            break;
        case Type::STOP_CLEANUP:
            startStopAction(ActionLocks::Cleanup, false);
            break;
        case Type::START_CLEANUP:
            startStopAction(ActionLocks::Cleanup, true);
            break;
        case Type::START_VIEW:
        case Type::START_VIEWS:
            startStopAction(ActionLocks::ViewRefresh, true);
            break;
        case Type::STOP_VIEW:
        case Type::STOP_VIEWS:
            startStopAction(ActionLocks::ViewRefresh, false);
            break;
        case Type::START_REPLICATED_VIEW:
            for (const auto & task : getRefreshTasks())
                task->startReplicated();
            break;
        case Type::STOP_REPLICATED_VIEW:
            for (const auto & task : getRefreshTasks())
                task->stopReplicated("SYSTEM STOP REPLICATED VIEW");
            break;
        case Type::REFRESH_VIEW:
            for (const auto & task : getRefreshTasks())
                task->run();
            break;
        case Type::WAIT_VIEW:
            for (const auto & task : getRefreshTasks())
                task->wait();
            break;
        case Type::CANCEL_VIEW:
            for (const auto & task : getRefreshTasks())
                task->cancel();
            break;
        case Type::TEST_VIEW:
            for (const auto & task : getRefreshTasks())
                task->setFakeTime(query.fake_time_for_view);
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
        case Type::REPLICA_UNREADY:
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Not implemented");
        case Type::REPLICA_READY:
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Not implemented");
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
            auto system_logs = getContext()->getSystemLogs();
            system_logs.flush(true, query.logs);
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
            auto * queue = getContext()->tryGetAsynchronousInsertQueue();
            if (!queue)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Cannot flush asynchronous insert queue because it is not initialized");

            queue->flushAll();
            break;
        }
        case Type::STOP_THREAD_FUZZER:
            getContext()->checkAccess(AccessType::SYSTEM_THREAD_FUZZER);
            ThreadFuzzer::stop();
            CannotAllocateThreadFaultInjector::setFaultProbability(0);
            break;
        case Type::START_THREAD_FUZZER:
            getContext()->checkAccess(AccessType::SYSTEM_THREAD_FUZZER);
            ThreadFuzzer::start();
            CannotAllocateThreadFaultInjector::setFaultProbability(getContext()->getServerSettings()[ServerSetting::cannot_allocate_thread_fault_injection_probability]);
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
        case Type::WAIT_FAILPOINT:
        {
            getContext()->checkAccess(AccessType::SYSTEM_FAILPOINT);
            LOG_TRACE(log, "Waiting for failpoint {}", query.fail_point_name);
            FailPointInjection::pauseFailPoint(query.fail_point_name);
            LOG_TRACE(log, "Finished waiting for failpoint {}", query.fail_point_name);
            break;
        }
        case Type::RESET_COVERAGE:
        {
            getContext()->checkAccess(AccessType::SYSTEM);
            resetCoverage();
            break;
        }
        case Type::LOAD_PRIMARY_KEY: {
            loadPrimaryKeys();
            break;
        }
        case Type::UNLOAD_PRIMARY_KEY:
        {
            unloadPrimaryKeys();
            break;
        }

#if USE_JEMALLOC
        case Type::JEMALLOC_PURGE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_JEMALLOC);
            purgeJemallocArenas();
            break;
        }
        case Type::JEMALLOC_ENABLE_PROFILE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_JEMALLOC);
            setJemallocProfileActive(true);
            break;
        }
        case Type::JEMALLOC_DISABLE_PROFILE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_JEMALLOC);
            setJemallocProfileActive(false);
            break;
        }
        case Type::JEMALLOC_FLUSH_PROFILE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_JEMALLOC);
            flushJemallocProfile("/tmp/jemalloc_clickhouse");
            break;
        }
#else
        case Type::JEMALLOC_PURGE:
        case Type::JEMALLOC_ENABLE_PROFILE:
        case Type::JEMALLOC_DISABLE_PROFILE:
        case Type::JEMALLOC_FLUSH_PROFILE:
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "The server was compiled without JEMalloc");
#endif
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

    const auto & settings = getContext()->getSettingsRef();

    table_replicated_ptr->restoreMetadataInZooKeeper(
        ZooKeeperRetriesInfo{
            settings[Setting::keeper_max_retries],
            settings[Setting::keeper_retry_initial_backoff_ms],
            settings[Setting::keeper_retry_max_backoff_ms],
            getContext()->getProcessListElementSafe()},
        false);
}

StoragePtr InterpreterSystemQuery::doRestartReplica(const StorageID & replica, ContextMutablePtr system_context, bool throw_on_error)
{
    LOG_TRACE(log, "Restarting replica {}", replica);
    auto table_ddl_guard = DatabaseCatalog::instance().getDDLGuard(replica.getDatabaseName(), replica.getTableName());

    auto restart_replica_lock = DatabaseCatalog::instance().tryGetLockForRestartReplica(replica.getDatabaseName());
    if (!restart_replica_lock)
        throw Exception(ErrorCodes::ABORTED, "Database {} is being dropped or detached, will not restart replica {}",
                        backQuoteIfNeed(replica.getDatabaseName()), replica.getNameForLogs());

    std::optional<Exception> exception;
    auto [database, table] = DatabaseCatalog::instance().getTableImpl(replica, getContext(), &exception);
    ASTPtr create_ast;

    if (!table)
    {
        if (throw_on_error)
            throw Exception(*exception);
        LOG_WARNING(getLogger("InterpreterSystemQuery"), "Cannot RESTART REPLICA {}: {}", replica.getNameForLogs(), exception->message());
        return nullptr;
    }
    const StorageID replica_table_id = table->getStorageID();
    if (!dynamic_cast<const StorageReplicatedMergeTree *>(table.get()))
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, table_is_not_replicated.data(), replica.getNameForLogs());
        LOG_WARNING(getLogger("InterpreterSystemQuery"), "Cannot RESTART REPLICA {}: not a ReplicatedMergeTree anymore", replica.getNameForLogs());
        return nullptr;
    }

    SCOPE_EXIT({
        if (table)
            table->is_being_restarted = false;
    });
    table->is_being_restarted = true;
    table->flushAndShutdown();
    {
        /// If table was already dropped by anyone, an exception will be thrown
        auto table_lock = table->lockExclusively(getContext()->getCurrentQueryId(), getContext()->getSettingsRef()[Setting::lock_acquire_timeout]);
        create_ast = database->getCreateTableQuery(replica.table_name, getContext());

        database->detachTable(system_context, replica.table_name);
    }
    table.reset();
    database->waitDetachedTableNotInUse(replica_table_id.uuid);

    /// Attach actions
    /// getCreateTableQuery must return canonical CREATE query representation, there are no need for AST postprocessing
    auto & create = create_ast->as<ASTCreateQuery &>();
    create.attach = true;

    auto columns = InterpreterCreateQuery::getColumnsDescription(*create.columns_list->columns, system_context, LoadingStrictnessLevel::ATTACH);
    auto constraints = InterpreterCreateQuery::getConstraintsDescription(create.columns_list->constraints, columns, system_context);
    auto data_path = database->getTableDataPath(create);

    auto new_table = StorageFactory::instance().get(create,
        data_path,
        system_context,
        system_context->getGlobalContext(),
        columns,
        constraints,
        LoadingStrictnessLevel::ATTACH);

    database->attachTable(system_context, replica.table_name, new_table, data_path);
    if (new_table->getStorageID().uuid != replica_table_id.uuid)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Tables UUID does not match after RESTART REPLICA (old: {}, new: {})", replica_table_id.uuid, new_table->getStorageID().uuid);

    new_table->startup();
    LOG_TRACE(log, "Restarted replica {}", new_table->getStorageID().getNameForLogs());
    return new_table;
}

void InterpreterSystemQuery::restartReplica(const StorageID & replica, ContextMutablePtr system_context)
{
    getContext()->checkAccess(AccessType::SYSTEM_RESTART_REPLICA, replica);
    doRestartReplica(replica, system_context, /*throw_on_error=*/ true);
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

    size_t threads = std::min(static_cast<size_t>(getNumberOfCPUCoresToUse()), replica_names.size());
    LOG_DEBUG(log, "Will restart {} replicas using {} threads", replica_names.size(), threads);
    ThreadPool pool(CurrentMetrics::RestartReplicaThreads, CurrentMetrics::RestartReplicaThreadsActive, CurrentMetrics::RestartReplicaThreadsScheduled, threads);

    for (auto & replica : replica_names)
    {
        pool.scheduleOrThrowOnError([&]() { doRestartReplica(replica, system_context, /*throw_on_error=*/ false); });
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

        /// Instead of silently failing, check the permissions to delete all databases in advance.
        /// Throw an exception to user if the user doesn't have enough privileges to drop the replica.
        /// Include the databases that the user needs privileges for in the exception
        std::vector<String> required_access;
        for (auto & elem : databases)
        {
            if (!access_is_granted_globally && !access->isGranted(AccessType::SYSTEM_DROP_REPLICA, elem.first))
            {
                required_access.emplace_back(elem.first);
                LOG_INFO(log, "? Access {} denied, skipping database {}", "SYSTEM DROP REPLICA", elem.first);
            }
        }

        if (!required_access.empty())
            throw Exception(
                ErrorCodes::ACCESS_DENIED,
                "Access denied for {}. Not enough permissions to drop these databases: {}",
                "SYSTEM DROP REPLICA",
                fmt::join(required_access, ", "));

        /// If we are here, then the user has the necassary access to drop the replica, continue with the operation.
        for (auto & elem : databases)
        {
            DatabasePtr & database = elem.second;
            for (auto iterator = database->getTablesIterator(getContext()); iterator->isValid(); iterator->next())
            {
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
                    if (status.replica_path == remote_replica_path)
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

        TableZnodeInfo info;
        info.path = query.replica_zk_path;
        info.replica_name = query.replica;
        StorageReplicatedMergeTree::dropReplica(zookeeper, info, log);
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
    if (query.replica == status.zookeeper_info.replica_name)
        throw Exception(ErrorCodes::TABLE_WAS_NOT_DROPPED,
                        "We can't drop local replica, please use `DROP TABLE` if you want "
                        "to clean the data and drop this replica");

    storage_replicated->dropReplica(query.replica, log);
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
            DatabaseReplicated::dropReplica(replicated, replicated->getZooKeeperPath(), query.shard, query.replica, /*throw_if_noop*/ true);
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
            DatabaseReplicated::dropReplica(replicated, replicated->getZooKeeperPath(), query.shard, query.replica, /*throw_if_noop*/ false);
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

        DatabaseReplicated::dropReplica(nullptr, query.replica_zk_path, query.shard, query.replica, /*throw_if_noop*/ true);
        LOG_INFO(log, "Dropped replica {} of Replicated database with path {}", query.replica, query.replica_zk_path);
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid query");
}

bool InterpreterSystemQuery::trySyncReplica(StoragePtr table, SyncReplicaMode sync_replica_mode, const std::unordered_set<String> & src_replicas, ContextPtr context_)
 {
    auto table_id_ = table->getStorageID();

    /// If materialized view, sync its target table.
    for (int i = 0;; ++i)
    {
        if (i >= 100)
            throw Exception(ErrorCodes::TOO_DEEP_RECURSION, "Materialized view targets form a cycle or a very long chain");

        if (auto * storage_mv = dynamic_cast<StorageMaterializedView *>(table.get()))
            table = storage_mv->getTargetTable();
        else
            break;
    }

    if (auto * storage_replicated = dynamic_cast<StorageReplicatedMergeTree *>(table.get()))
    {
        auto log = getLogger("InterpreterSystemQuery");
        LOG_TRACE(log, "Synchronizing entries in replica's queue with table's log and waiting for current last entry to be processed");
        auto sync_timeout = context_->getSettingsRef()[Setting::receive_timeout].totalMilliseconds();
        if (!storage_replicated->waitForProcessingQueue(sync_timeout, sync_replica_mode, src_replicas))
        {
            LOG_ERROR(log, "SYNC REPLICA {}: Timed out.", table_id_.getNameForLogs());
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "SYNC REPLICA {}: command timed out. " \
                    "See the 'receive_timeout' setting", table_id_.getNameForLogs());
        }
        LOG_TRACE(log, "SYNC REPLICA {}: OK", table_id_.getNameForLogs());
    }
    else
        return false;

    return true;
}

void InterpreterSystemQuery::syncReplica(ASTSystemQuery & query)
{
    getContext()->checkAccess(AccessType::SYSTEM_SYNC_REPLICA, table_id);
    StoragePtr table;

    if (query.if_exists)
    {
        table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
        if (!table)
            return;
    }
    else
    {
        table = DatabaseCatalog::instance().getTable(table_id, getContext());
    }

    std::unordered_set<std::string> replicas(query.src_replicas.begin(), query.src_replicas.end());
    if (!trySyncReplica(table, query.sync_replica_mode, replicas, getContext()))
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

void InterpreterSystemQuery::loadPrimaryKeys()
{
    loadOrUnloadPrimaryKeysImpl(true);
};

void InterpreterSystemQuery::unloadPrimaryKeys()
{
    loadOrUnloadPrimaryKeysImpl(false);
}

void InterpreterSystemQuery::loadOrUnloadPrimaryKeysImpl(bool load)
{
    if (!table_id.empty())
    {
        getContext()->checkAccess(load ? AccessType::SYSTEM_LOAD_PRIMARY_KEY : AccessType::SYSTEM_UNLOAD_PRIMARY_KEY, table_id.database_name, table_id.table_name);
        StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());

        if (auto * merge_tree = dynamic_cast<MergeTreeData *>(table.get()))
        {
            LOG_TRACE(log, "{} primary keys for table {}", load ? "Loading" : "Unloading", table_id.getFullTableName());
            load ? merge_tree->loadPrimaryKeys() : merge_tree->unloadPrimaryKeys();
        }
        else
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Command {} PRIMARY KEY is supported only for MergeTree tables, but got: {}", load ? "LOAD" : "UNLOAD", table->getName());
        }
    }
    else
    {
        getContext()->checkAccess(load ? AccessType::SYSTEM_LOAD_PRIMARY_KEY : AccessType::SYSTEM_UNLOAD_PRIMARY_KEY);
        LOG_TRACE(log, "{} primary keys for all tables", load ? "Loading" : "Unloading");

        for (auto & database : DatabaseCatalog::instance().getDatabases())
        {
            for (auto it = database.second->getTablesIterator(getContext()); it->isValid(); it->next())
            {
                if (auto * merge_tree = dynamic_cast<MergeTreeData *>(it->table().get()))
                {
                    load ? merge_tree->loadPrimaryKeys() : merge_tree->unloadPrimaryKeys();
                }
            }
        }
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
        if (!ptr->waitForReplicaToProcessAllEntries(getContext()->getSettingsRef()[Setting::receive_timeout].totalMilliseconds()))
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


void InterpreterSystemQuery::flushDistributed(ASTSystemQuery & query)
{
    getContext()->checkAccess(AccessType::SYSTEM_FLUSH_DISTRIBUTED, table_id);

    SettingsChanges settings_changes;
    if (query.query_settings)
        settings_changes = query.query_settings->as<ASTSetQuery>()->changes;

    if (auto * storage_distributed = dynamic_cast<StorageDistributed *>(DatabaseCatalog::instance().getTable(table_id, getContext()).get()))
        storage_distributed->flushClusterNodesAllData(getContext(), settings_changes);
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table {} is not distributed", table_id.getNameForLogs());
}

[[noreturn]] void InterpreterSystemQuery::restartDisk(String &)
{
    getContext()->checkAccess(AccessType::SYSTEM_RESTART_DISK);
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "SYSTEM RESTART DISK is not supported");
}

RefreshTaskList InterpreterSystemQuery::getRefreshTasks()
{
    auto ctx = getContext();
    ctx->checkAccess(AccessType::SYSTEM_VIEWS, table_id);
    auto tasks = ctx->getRefreshSet().findTasks(table_id);
    if (tasks.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Refreshable view {} doesn't exist", table_id.getNameForLogs());
    return tasks;
}

void InterpreterSystemQuery::prewarmMarkCache()
{
    if (table_id.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table is not specified for PREWARM MARK CACHE command");

    getContext()->checkAccess(AccessType::SYSTEM_PREWARM_MARK_CACHE, table_id);

    auto table_ptr = DatabaseCatalog::instance().getTable(table_id, getContext());
    auto * merge_tree = dynamic_cast<MergeTreeData *>(table_ptr.get());
    if (!merge_tree)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Command PREWARM MARK CACHE is supported only for MergeTree table, but got: {}", table_ptr->getName());

    auto mark_cache = getContext()->getMarkCache();
    if (!mark_cache)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mark cache is not configured");

    ThreadPool pool(
        CurrentMetrics::MergeTreePartsLoaderThreads,
        CurrentMetrics::MergeTreePartsLoaderThreadsActive,
        CurrentMetrics::MergeTreePartsLoaderThreadsScheduled,
        getContext()->getSettingsRef()[Setting::max_threads]);

    merge_tree->prewarmCaches(pool, std::move(mark_cache), nullptr);
}

void InterpreterSystemQuery::prewarmPrimaryIndexCache()
{
    if (table_id.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table is not specified for PREWARM PRIMARY INDEX CACHE command");

    getContext()->checkAccess(AccessType::SYSTEM_PREWARM_PRIMARY_INDEX_CACHE, table_id);

    auto table_ptr = DatabaseCatalog::instance().getTable(table_id, getContext());
    auto * merge_tree = dynamic_cast<MergeTreeData *>(table_ptr.get());
    if (!merge_tree)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Command PREWARM PRIMARY INDEX CACHE is supported only for MergeTree table, but got: {}", table_ptr->getName());

    auto index_cache = merge_tree->getPrimaryIndexCache();
    if (!index_cache)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Primary index cache is not configured or is not enabled for table {}", table_id.getFullTableName());

    ThreadPool pool(
        CurrentMetrics::MergeTreePartsLoaderThreads,
        CurrentMetrics::MergeTreePartsLoaderThreadsActive,
        CurrentMetrics::MergeTreePartsLoaderThreadsScheduled,
        getContext()->getSettingsRef()[Setting::max_threads]);

    merge_tree->prewarmCaches(pool, nullptr, std::move(index_cache));
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
        case Type::DROP_CONNECTIONS_CACHE:
        case Type::DROP_MARK_CACHE:
        case Type::DROP_ICEBERG_METADATA_CACHE:
        case Type::DROP_PRIMARY_INDEX_CACHE:
        case Type::DROP_MMAP_CACHE:
        case Type::DROP_QUERY_CONDITION_CACHE:
        case Type::DROP_QUERY_CACHE:
        case Type::DROP_COMPILED_EXPRESSION_CACHE:
        case Type::DROP_UNCOMPRESSED_CACHE:
        case Type::DROP_INDEX_MARK_CACHE:
        case Type::DROP_INDEX_UNCOMPRESSED_CACHE:
        case Type::DROP_VECTOR_SIMILARITY_INDEX_CACHE:
        case Type::DROP_FILESYSTEM_CACHE:
        case Type::DROP_DISTRIBUTED_CACHE_CONNECTIONS:
        case Type::DROP_DISTRIBUTED_CACHE:
        case Type::SYNC_FILESYSTEM_CACHE:
        case Type::DROP_PAGE_CACHE:
        case Type::DROP_SCHEMA_CACHE:
        case Type::DROP_FORMAT_SCHEMA_CACHE:
        case Type::DROP_S3_CLIENT_CACHE:
        {
            required_access.emplace_back(AccessType::SYSTEM_DROP_CACHE);
            break;
        }
        case Type::DROP_DISK_METADATA_CACHE:
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Not implemented");
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
        case Type::RELOAD_ASYNCHRONOUS_METRICS:
        {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_ASYNCHRONOUS_METRICS);
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
        case Type::STOP_CLEANUP:
        case Type::START_CLEANUP:
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
        case Type::STOP_REPLICATED_DDL_QUERIES:
        case Type::START_REPLICATED_DDL_QUERIES:
        {
            required_access.emplace_back(AccessType::SYSTEM_REPLICATION_QUEUES);
            break;
        }
        case Type::STOP_VIRTUAL_PARTS_UPDATE:
        case Type::START_VIRTUAL_PARTS_UPDATE:
        {
            if (!query.table)
                required_access.emplace_back(AccessType::SYSTEM_PULLING_REPLICATION_LOG);
            else
                required_access.emplace_back(AccessType::SYSTEM_PULLING_REPLICATION_LOG, query.getDatabase(), query.getTable());
            break;
        }
        case Type::STOP_REDUCE_BLOCKING_PARTS:
        case Type::START_REDUCE_BLOCKING_PARTS:
        {
            if (!query.table)
                required_access.emplace_back(AccessType::SYSTEM_REDUCE_BLOCKING_PARTS);
            else
                required_access.emplace_back(AccessType::SYSTEM_REDUCE_BLOCKING_PARTS, query.getDatabase(), query.getTable());
            break;
        }
        case Type::REFRESH_VIEW:
        case Type::WAIT_VIEW:
        case Type::START_VIEW:
        case Type::START_VIEWS:
        case Type::START_REPLICATED_VIEW:
        case Type::STOP_VIEW:
        case Type::STOP_VIEWS:
        case Type::STOP_REPLICATED_VIEW:
        case Type::CANCEL_VIEW:
        case Type::TEST_VIEW:
        {
            if (!query.table)
                required_access.emplace_back(AccessType::SYSTEM_VIEWS);
            else
                required_access.emplace_back(AccessType::SYSTEM_VIEWS, query.getDatabase(), query.getTable());
            break;
        }
        case Type::DROP_REPLICA:
        case Type::DROP_DATABASE_REPLICA:
        {
            required_access.emplace_back(AccessType::SYSTEM_DROP_REPLICA, query.getDatabase(), query.getTable());
            break;
        }
        case Type::DROP_CATALOG_REPLICA:
        {
            required_access.emplace_back(AccessType::SYSTEM_DROP_REPLICA);
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
        case Type::REPLICA_READY:
        case Type::REPLICA_UNREADY:
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Not implemented");
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
        case Type::PREWARM_MARK_CACHE:
        {
            required_access.emplace_back(AccessType::SYSTEM_PREWARM_MARK_CACHE, query.getDatabase(), query.getTable());
            break;
        }
        case Type::PREWARM_PRIMARY_INDEX_CACHE:
        {
            required_access.emplace_back(AccessType::SYSTEM_PREWARM_MARK_CACHE, query.getDatabase(), query.getTable());
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
        case Type::JEMALLOC_PURGE:
        case Type::JEMALLOC_ENABLE_PROFILE:
        case Type::JEMALLOC_DISABLE_PROFILE:
        case Type::JEMALLOC_FLUSH_PROFILE:
        {
            required_access.emplace_back(AccessType::SYSTEM_JEMALLOC);
            break;
        }
        case Type::LOAD_PRIMARY_KEY:
        {
            if (!query.table)
                required_access.emplace_back(AccessType::SYSTEM_LOAD_PRIMARY_KEY);
            else
                required_access.emplace_back(AccessType::SYSTEM_LOAD_PRIMARY_KEY, query.getDatabase(), query.getTable());
            break;
        }
        case Type::UNLOAD_PRIMARY_KEY:
        {
            if (!query.table)
                required_access.emplace_back(AccessType::SYSTEM_UNLOAD_PRIMARY_KEY);
            else
                required_access.emplace_back(AccessType::SYSTEM_UNLOAD_PRIMARY_KEY, query.getDatabase(), query.getTable());
            break;
        }
        case Type::UNLOCK_SNAPSHOT:
        {
            required_access.emplace_back(AccessType::BACKUP);
            break;
        }
        case Type::STOP_THREAD_FUZZER:
        case Type::START_THREAD_FUZZER:
        case Type::ENABLE_FAILPOINT:
        case Type::WAIT_FAILPOINT:
        case Type::DISABLE_FAILPOINT:
        case Type::RESET_COVERAGE:
        case Type::UNKNOWN:
        case Type::END: break;
    }
    return required_access;
}

void registerInterpreterSystemQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterSystemQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterSystemQuery", create_fn);
}

}
