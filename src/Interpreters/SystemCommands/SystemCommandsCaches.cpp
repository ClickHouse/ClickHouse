#include <Interpreters/SystemCommands/SystemCommandHelpers.h>

#include <Columns/IColumn.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Formats/FormatSchemaInfo.h>
#include <Functions/pointInPolygon.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/FileCacheFactory.h>
#include <Interpreters/JIT/CHJIT.h>
#include <Interpreters/JIT/CompileRegexp.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/StorageFile.h>
#include <Storages/StorageProxy.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/StorageURL.h>
#include <Common/CurrentMetrics.h>
#include <Common/DNSResolver.h>
#include <Common/HTTPConnectionPool.h>
#include <Common/HostResolvePool.h>
#include <Common/JemallocJITArena.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>

#include <filesystem>
#include <unistd.h>

#include "config.h"

#if USE_PROTOBUF
#include <Formats/ProtobufSchemas.h>
#endif

#if USE_AVRO
#include <Processors/Formats/Impl/AvroConfluentSchemaRegistry.h>
#endif

#if USE_AWS_S3
#include <IO/S3/Client.h>
#endif

#if ENABLE_DISTRIBUTED_CACHE
#include <DistributedCache/Utils.h>
#endif

namespace CurrentMetrics
{
    extern const Metric MergeTreePartsLoaderThreads;
    extern const Metric MergeTreePartsLoaderThreadsActive;
    extern const Metric MergeTreePartsLoaderThreadsScheduled;
}

namespace DB
{

namespace Setting
{
    extern const SettingsMaxThreads max_threads;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

/// Commands that work with the caches of a table.
class TableCacheCommands : public SystemCommandContext
{
public:
    explicit TableCacheCommands(const SystemCommandContext & command_context) : SystemCommandContext(command_context) {}

    void clearTimeSeriesCaches();
    void prewarmMarkCache();
    void prewarmPrimaryIndexCache();
};

void TableCacheCommands::clearTimeSeriesCaches()
{
    if (table_id.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table is not specified for CLEAR TIME SERIES CACHES command");

    getContext()->checkAccess(AccessType::SYSTEM_DROP_TIME_SERIES_CACHES, table_id);

    auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
    storagePtrToTimeSeries(table)->clearCaches();
}

void TableCacheCommands::prewarmMarkCache()
{
    if (table_id.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table is not specified for PREWARM MARK CACHE command");

    getContext()->checkAccess(AccessType::SYSTEM_PREWARM_MARK_CACHE, table_id);

    auto table_ptr = resolveStorageProxyLoading(DatabaseCatalog::instance().getTable(table_id, getContext()));
    auto * merge_tree = castStorage<MergeTreeData>(table_ptr, DeferredTable::Load).get();
    if (!merge_tree)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Command PREWARM MARK CACHE is supported only for MergeTree table, but got: {}", table_ptr->getName());

    getContext()->onQueryExecutionStarted();
    ThreadPool pool(
        CurrentMetrics::MergeTreePartsLoaderThreads,
        CurrentMetrics::MergeTreePartsLoaderThreadsActive,
        CurrentMetrics::MergeTreePartsLoaderThreadsScheduled,
        getContext()->getSettingsRef()[Setting::max_threads]);

    MergeTreeData::CachesToPrewarm caches;
    caches.mark_cache = getContext()->getMarkCache();
    caches.index_mark_cache = getContext()->getIndexMarkCache();
    merge_tree->prewarmCaches(pool, caches);
}

void TableCacheCommands::prewarmPrimaryIndexCache()
{
    if (table_id.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table is not specified for PREWARM PRIMARY INDEX CACHE command");

    getContext()->checkAccess(AccessType::SYSTEM_PREWARM_PRIMARY_INDEX_CACHE, table_id);

    auto table_ptr = resolveStorageProxyLoading(DatabaseCatalog::instance().getTable(table_id, getContext()));
    auto * merge_tree = castStorage<MergeTreeData>(table_ptr, DeferredTable::Load).get();
    if (!merge_tree)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Command PREWARM PRIMARY INDEX CACHE is supported only for MergeTree table, but got: {}", table_ptr->getName());

    auto index_cache = merge_tree->getPrimaryIndexCache();
    if (!index_cache)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Primary index cache is not configured or is not enabled for table {}", table_id.getFullTableName());

    getContext()->onQueryExecutionStarted();
    ThreadPool pool(
        CurrentMetrics::MergeTreePartsLoaderThreads,
        CurrentMetrics::MergeTreePartsLoaderThreadsActive,
        CurrentMetrics::MergeTreePartsLoaderThreadsScheduled,
        getContext()->getSettingsRef()[Setting::max_threads]);

    MergeTreeData::CachesToPrewarm caches;
    caches.primary_index_cache = index_cache;
    merge_tree->prewarmCaches(pool, caches);
}

}

void registerSystemCommandsCaches(SystemCommandFactory & factory)
{
    using Type = ASTSystemQuery::Type;

    factory.registerSimpleCommand(Type::SYNC_FILE_CACHE, AccessType::SYSTEM_SYNC_FILE_CACHE, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        LOG_DEBUG(context.log, "Will perform 'sync' syscall (it can take time).");
        sync();
    });

    factory.registerSimpleCommand(Type::CLEAR_DNS_CACHE, AccessType::SYSTEM_DROP_DNS_CACHE, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        DNSResolver::instance().dropCache();
        HostResolversPool::instance().dropCache();
        /// Reinitialize clusters to update their resolved_addresses
        context.system_context->reloadClusterConfig();
    });

    factory.registerSimpleCommand(Type::CLEAR_CONNECTIONS_CACHE, AccessType::SYSTEM_DROP_CONNECTIONS_CACHE, [](ASTSystemQuery &, SystemCommandContext &)
    {
        HTTPConnectionPools::instance().dropCache();
    });

    factory.registerCommand(
        Type::PREWARM_MARK_CACHE,
        tableAccessOnCluster(AccessType::SYSTEM_PREWARM_MARK_CACHE),
        [](ASTSystemQuery &, SystemCommandContext & context)
        {
            TableCacheCommands(context).prewarmMarkCache();
            return BlockIO{};
        });

    factory.registerCommand(
        Type::PREWARM_PRIMARY_INDEX_CACHE,
        tableAccessOnCluster(AccessType::SYSTEM_PREWARM_PRIMARY_INDEX_CACHE),
        [](ASTSystemQuery &, SystemCommandContext & context)
        {
            TableCacheCommands(context).prewarmPrimaryIndexCache();
            return BlockIO{};
        });

    factory.registerCommand(
        Type::CLEAR_TIME_SERIES_CACHES,
        tableAccessOnCluster(AccessType::SYSTEM_DROP_TIME_SERIES_CACHES),
        [](ASTSystemQuery &, SystemCommandContext & context)
        {
            TableCacheCommands(context).clearTimeSeriesCaches();
            return BlockIO{};
        });

    factory.registerSimpleCommand(Type::CLEAR_MARK_CACHE, AccessType::SYSTEM_DROP_MARK_CACHE, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        context.system_context->clearMarkCache();
    });

#if USE_AVRO
    factory.registerSimpleCommand(Type::CLEAR_ICEBERG_METADATA_CACHE, AccessType::SYSTEM_DROP_ICEBERG_METADATA_CACHE, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        context.system_context->clearIcebergMetadataFilesCache();
    });
    factory.registerSimpleCommand(Type::CLEAR_PAIMON_METADATA_CACHE, AccessType::SYSTEM_DROP_PAIMON_METADATA_CACHE, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        context.system_context->clearPaimonMetadataFilesCache();
    });
    factory.registerSimpleCommand(Type::CLEAR_AVRO_SCHEMA_CACHE, AccessType::SYSTEM_DROP_AVRO_SCHEMA_CACHE, [](ASTSystemQuery &, SystemCommandContext &)
    {
        clearConfluentSchemaRegistryCache();
    });
#else
    factory.registerCommand(Type::CLEAR_ICEBERG_METADATA_CACHE, globalAccessOnCluster(AccessType::SYSTEM_DROP_ICEBERG_METADATA_CACHE), [](ASTSystemQuery &, SystemCommandContext &) -> BlockIO
    {
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "The server was compiled without the support for AVRO");
    });
    factory.registerCommand(Type::CLEAR_PAIMON_METADATA_CACHE, globalAccessOnCluster(AccessType::SYSTEM_DROP_PAIMON_METADATA_CACHE), [](ASTSystemQuery &, SystemCommandContext &) -> BlockIO
    {
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "The server was compiled without the support for Paimon");
    });
    factory.registerCommand(Type::CLEAR_AVRO_SCHEMA_CACHE, globalAccessOnCluster(AccessType::SYSTEM_DROP_AVRO_SCHEMA_CACHE), [](ASTSystemQuery &, SystemCommandContext &) -> BlockIO
    {
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "The server was compiled without the support for AVRO");
    });
#endif

#if USE_PARQUET
    factory.registerSimpleCommand(Type::CLEAR_PARQUET_METADATA_CACHE, AccessType::SYSTEM_DROP_PARQUET_METADATA_CACHE, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        context.system_context->clearParquetMetadataCache();
    });
#else
    factory.registerCommand(Type::CLEAR_PARQUET_METADATA_CACHE, globalAccessOnCluster(AccessType::SYSTEM_DROP_PARQUET_METADATA_CACHE), [](ASTSystemQuery &, SystemCommandContext &) -> BlockIO
    {
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "The server was compiled without the support for Parquet");
    });
#endif

    factory.registerSimpleCommand(Type::CLEAR_POINT_IN_POLYGON_CACHE, AccessType::SYSTEM_DROP_POINT_IN_POLYGON_CACHE, [](ASTSystemQuery &, SystemCommandContext &)
    {
        clearPointInPolygonCache();
    });

    factory.registerSimpleCommand(Type::CLEAR_PRIMARY_INDEX_CACHE, AccessType::SYSTEM_DROP_PRIMARY_INDEX_CACHE, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        context.system_context->clearPrimaryIndexCache();
    });

    factory.registerSimpleCommand(Type::CLEAR_STATISTICS_CACHE, AccessType::SYSTEM_DROP_STATISTICS_CACHE, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        context.system_context->clearStatisticsCache();
    });

    factory.registerSimpleCommand(Type::CLEAR_UNCOMPRESSED_CACHE, AccessType::SYSTEM_DROP_UNCOMPRESSED_CACHE, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        context.system_context->clearUncompressedCache();
    });

    factory.registerSimpleCommand(Type::CLEAR_COLUMNS_CACHE, AccessType::SYSTEM_DROP_COLUMNS_CACHE, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        context.system_context->clearColumnsCache();
    });

    /// CLEAR INDEX MARK CACHE and CLEAR INDEX UNCOMPRESSED CACHE have no privilege of their own
    /// and reuse the one of the cache they clear.
    factory.registerSimpleCommand(Type::CLEAR_INDEX_MARK_CACHE, AccessType::SYSTEM_DROP_MARK_CACHE, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        context.system_context->clearIndexMarkCache();
    });

    factory.registerSimpleCommand(Type::CLEAR_INDEX_UNCOMPRESSED_CACHE, AccessType::SYSTEM_DROP_UNCOMPRESSED_CACHE, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        context.system_context->clearIndexUncompressedCache();
    });

    factory.registerSimpleCommand(Type::CLEAR_VECTOR_SIMILARITY_INDEX_CACHE, AccessType::SYSTEM_DROP_VECTOR_SIMILARITY_INDEX_CACHE, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        context.system_context->clearVectorSimilarityIndexCache();
    });

    factory.registerSimpleCommand(Type::CLEAR_TEXT_INDEX_TOKENS_CACHE, AccessType::SYSTEM_DROP_TEXT_INDEX_TOKENS_CACHE, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        context.system_context->clearTextIndexTokensCache();
    });

    factory.registerSimpleCommand(Type::CLEAR_TEXT_INDEX_HEADER_CACHE, AccessType::SYSTEM_DROP_TEXT_INDEX_HEADER_CACHE, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        context.system_context->clearTextIndexHeaderCache();
    });

    factory.registerSimpleCommand(Type::CLEAR_TEXT_INDEX_POSTINGS_CACHE, AccessType::SYSTEM_DROP_TEXT_INDEX_POSTINGS_CACHE, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        context.system_context->clearTextIndexPostingsCache();
    });

    factory.registerSimpleCommand(Type::CLEAR_TEXT_INDEX_CACHES, AccessType::SYSTEM_DROP_TEXT_INDEX_CACHES, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        context.system_context->clearTextIndexTokensCache();
        context.system_context->clearTextIndexHeaderCache();
        context.system_context->clearTextIndexPostingsCache();
    });

    factory.registerSimpleCommand(Type::CLEAR_MMAP_CACHE, AccessType::SYSTEM_DROP_MMAP_CACHE, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        context.system_context->clearMMappedFileCache();
    });

    factory.registerSimpleCommand(Type::CLEAR_QUERY_CONDITION_CACHE, AccessType::SYSTEM_DROP_QUERY_CONDITION_CACHE, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        context.getContext()->clearQueryConditionCache();
    });

    factory.registerSimpleCommand(Type::CLEAR_ENCRYPTION_HEADERS_CACHE, AccessType::SYSTEM_DROP_ENCRYPTION_HEADERS_CACHE, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        context.getContext()->clearEncryptionHeaderCache();
    });

    factory.registerSimpleCommand(Type::CLEAR_QUERY_CACHE, AccessType::SYSTEM_DROP_QUERY_CACHE, [](ASTSystemQuery & query, SystemCommandContext & context)
    {
        context.getContext()->clearQueryResultCache(query.query_result_cache_tag);
    });

#if USE_EMBEDDED_COMPILER
    factory.registerSimpleCommand(Type::CLEAR_COMPILED_EXPRESSION_CACHE, AccessType::SYSTEM_DROP_COMPILED_EXPRESSION_CACHE, [](ASTSystemQuery &, SystemCommandContext &)
    {
        if (auto * cache = CompiledExpressionCacheFactory::instance().tryGetCache())
            cache->clear();
        /// Drop the static CHJIT slots so persistent LLVM state (`TargetMachine`, `Subtarget`,
        /// `LLVMContext`-interned types/constants accumulated across compiles) is reclaimed too.
        /// Each in-flight compile and each cache holder retains its own `shared_ptr<CHJIT>`, so
        /// concurrent operations are not affected: the actual CHJIT survives until every user
        /// has released its handle. After the cache->clear() above, the only remaining
        /// references are from any concurrent in-flight compile (which will produce a holder
        /// pinned to the old instance) — that holder will be evicted normally later, releasing
        /// the old instance at that point.
        resetExpressionJITInstance();
        resetAggregatorJITInstance();
        resetSortDescriptionJITInstance();
        resetRegexpJITInstance();
        /// Clearing the cache invokes `~JITModuleMemoryManager` for every entry, which runs LLVM's
        /// per-module destructors and frees their bookkeeping into the dedicated JIT arena. Purge dirty
        /// pages from that arena so the freed memory is returned to the OS without waiting for the
        /// arena's decay timer.
        JemallocJITArena::purge();
    });
#else
    factory.registerCommand(Type::CLEAR_COMPILED_EXPRESSION_CACHE, globalAccessOnCluster(AccessType::SYSTEM_DROP_COMPILED_EXPRESSION_CACHE), [](ASTSystemQuery &, SystemCommandContext &) -> BlockIO
    {
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "The server was compiled without the support for JIT compilation");
    });
#endif

#if USE_AWS_S3
    factory.registerSimpleCommand(Type::CLEAR_S3_CLIENT_CACHE, AccessType::SYSTEM_DROP_S3_CLIENT_CACHE, [](ASTSystemQuery &, SystemCommandContext &)
    {
        S3::ClientCacheRegistry::instance().clearCacheForAll();
    });
#else
    factory.registerCommand(Type::CLEAR_S3_CLIENT_CACHE, globalAccessOnCluster(AccessType::SYSTEM_DROP_S3_CLIENT_CACHE), [](ASTSystemQuery &, SystemCommandContext &) -> BlockIO
    {
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "The server was compiled without the support for AWS S3");
    });
#endif

    factory.registerSimpleCommand(Type::CLEAR_FILESYSTEM_CACHE, AccessType::SYSTEM_DROP_FILESYSTEM_CACHE, [](ASTSystemQuery & query, [[maybe_unused]] SystemCommandContext & context)
    {
#if ENABLE_DISTRIBUTED_CACHE
        const auto user_id = DistributedCache::getFilesystemCacheUserId(context.getContext());
#else
        const auto user_id = FileCache::getCommonOrigin().user_id;
#endif

        if (query.filesystem_cache_name.empty())
        {
            for (const auto & cache_data : FileCacheFactory::instance().getUniqueInstances())
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
    });

#if ENABLE_DISTRIBUTED_CACHE
    factory.registerSimpleCommand(Type::CLEAR_DISTRIBUTED_CACHE, AccessType::SYSTEM_DROP_DISTRIBUTED_CACHE, [](ASTSystemQuery & query, SystemCommandContext & context)
    {
        DistributedCache::clearDistributedCache(context.getContext(), query, context.log);
    });
#else
    factory.registerCommand(Type::CLEAR_DISTRIBUTED_CACHE, globalAccessOnCluster(AccessType::SYSTEM_DROP_DISTRIBUTED_CACHE), [](ASTSystemQuery &, SystemCommandContext &) -> BlockIO
    {
        throwUnknownSystemQueryType();
    });
#endif

    factory.registerCommand(
        Type::SYNC_FILESYSTEM_CACHE,
        globalAccessOnCluster(AccessType::SYSTEM_SYNC_FILESYSTEM_CACHE),
        [](ASTSystemQuery & query, SystemCommandContext & context)
        {
            context.getContext()->checkAccess(AccessType::SYSTEM_SYNC_FILESYSTEM_CACHE);

            ColumnsDescription columns{NamesAndTypesList{
                {"cache_name", std::make_shared<DataTypeString>()},
                {"path", std::make_shared<DataTypeString>()},
                {"size", std::make_shared<DataTypeUInt64>()},
            }};
            Block sample_block;
            for (const auto & column : columns)
                sample_block.insert({column.type->createColumn(), column.type, column.name});

            MutableColumns res_columns = sample_block.cloneEmptyColumns();

            auto fill_data = [&](const std::string & cache_name, const std::vector<FileSegment::Info> & file_segments)
            {
                for (const auto & file_segment : file_segments)
                {
                    size_t i = 0;
                    /// `file_segment.path` already reflects the real on-disk name (including the
                    /// size suffix for downloaded segments); do not recompute it from the offset.
                    res_columns[i++]->insert(cache_name);
                    res_columns[i++]->insert(file_segment.path);
                    res_columns[i++]->insert(file_segment.downloaded_size);
                }
            };

            if (query.filesystem_cache_name.empty())
            {
                for (const auto & cache_data : FileCacheFactory::instance().getUniqueInstances())
                {
                    auto file_segments = cache_data->cache->sync();
                    fill_data(cache_data->cache->getName(), file_segments);
                }
            }
            else
            {
                auto cache = FileCacheFactory::instance().getByName(query.filesystem_cache_name)->cache;
                auto file_segments = cache->sync();
                fill_data(query.filesystem_cache_name, file_segments);
            }

            size_t num_rows = res_columns[0]->size();
            auto source = std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(std::move(sample_block)), Chunk(std::move(res_columns), num_rows));
            BlockIO result;
            result.pipeline = QueryPipeline(std::move(source));
            return result;
        });

    factory.registerCommand(
        Type::CLEAR_DISK_METADATA_CACHE,
#if CLICKHOUSE_CLOUD
        globalAccessOnCluster(AccessType::SYSTEM_DROP_FILESYSTEM_CACHE),
#else
        [](const ASTSystemQuery &) -> AccessRightsElements
        {
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Not implemented");
        },
#endif
        [](ASTSystemQuery & query, SystemCommandContext & context)
        {
            context.getContext()->checkAccess(AccessType::SYSTEM_DROP_FILESYSTEM_CACHE);

            auto metadata = context.getContext()->getDisk(query.disk)->getMetadataStorage();
            if (metadata)
                metadata->dropCache();

            return BlockIO{};
        });

    factory.registerSimpleCommand(Type::CLEAR_PAGE_CACHE, AccessType::SYSTEM_DROP_PAGE_CACHE, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        context.system_context->clearPageCache();
    });

    factory.registerSimpleCommand(Type::CLEAR_SCHEMA_CACHE, AccessType::SYSTEM_DROP_SCHEMA_CACHE, [](ASTSystemQuery & query, SystemCommandContext & context)
    {
        std::unordered_set<String> caches_to_drop;
        if (query.schema_cache_storage.empty())
            caches_to_drop = {"FILE", "S3", "HDFS", "URL", "AZURE"};
        else
            caches_to_drop = {query.schema_cache_storage};

        if (caches_to_drop.contains("FILE"))
            StorageFile::getSchemaCache(context.getContext()).clear();
#if USE_AWS_S3
        if (caches_to_drop.contains("S3"))
            StorageObjectStorage::getSchemaCache(context.getContext(), StorageS3Configuration::type_name).clear();
#endif
#if USE_HDFS
        if (caches_to_drop.contains("HDFS"))
            StorageObjectStorage::getSchemaCache(context.getContext(), StorageHDFSConfiguration::type_name).clear();
#endif
        if (caches_to_drop.contains("URL"))
            StorageURL::getSchemaCache(context.getContext()).clear();
#if USE_AZURE_BLOB_STORAGE
        if (caches_to_drop.contains("AZURE"))
            StorageObjectStorage::getSchemaCache(context.getContext(), StorageAzureConfiguration::type_name).clear();
#endif
    });

    factory.registerSimpleCommand(Type::CLEAR_FORMAT_SCHEMA_CACHE, AccessType::SYSTEM_DROP_FORMAT_SCHEMA_CACHE, [](ASTSystemQuery & query, SystemCommandContext & context)
    {
        std::unordered_set<String> caches_to_drop;
        if (query.schema_cache_format.empty())
            caches_to_drop = {"Protobuf", "Files"};
        else
            caches_to_drop = {query.schema_cache_format};
#if USE_PROTOBUF
        if (caches_to_drop.contains("Protobuf"))
            ProtobufSchemas::instance().clear();
#endif
        if (caches_to_drop.contains("Files"))
        {
            fs::path format_schema_cached_dir = fs::path(context.system_context->getFormatSchemaPath()) / FormatSchemaInfo::CACHE_DIR_NAME;
            if (fs::exists(format_schema_cached_dir))
            {
                size_t count = 0;
                for (const auto & entry : fs::directory_iterator(format_schema_cached_dir))
                {
                    if (entry.is_regular_file())
                    {
                        fs::remove(entry.path());
                        count++;
                    }
                }
                LOG_INFO(context.log, "Cleared format schema cache files {}", count);
            }
        }
    });
}

}
