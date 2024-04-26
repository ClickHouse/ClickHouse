#include <Storages/ObjectStorage/StorageObjectStorage.h>

#include <Formats/FormatFactory.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Formats/ReadSchemaUtils.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Transforms/ExtractColumnsTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/StorageFactory.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Storages/ObjectStorage/StorageObjectStorageSink.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/ReadFromObjectStorageStep.h>
#include <Storages/ObjectStorage/ReadBufferIterator.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/Cache/SchemaCache.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int DATABASE_ACCESS_DENIED;
    extern const int NOT_IMPLEMENTED;
}

StorageObjectStorage::StorageObjectStorage(
    ConfigurationPtr configuration_,
    ObjectStoragePtr object_storage_,
    ContextPtr context,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    std::optional<FormatSettings> format_settings_,
    bool distributed_processing_,
    ASTPtr partition_by_)
    : IStorage(table_id_)
    , configuration(configuration_)
    , object_storage(object_storage_)
    , format_settings(format_settings_)
    , partition_by(partition_by_)
    , distributed_processing(distributed_processing_)
    , log(getLogger(fmt::format("Storage{}({})", configuration->getEngineName(), table_id_.getFullTableName())))
{
    ColumnsDescription columns{columns_};
    resolveSchemaAndFormat(columns, configuration->format, object_storage, configuration, format_settings, context);
    configuration->check(context);

    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns);
    metadata.setConstraints(constraints_);
    metadata.setComment(comment);

    StoredObjects objects;
    for (const auto & key : configuration->getPaths())
        objects.emplace_back(key);

    setVirtuals(VirtualColumnUtils::getVirtualsForFileLikeStorage(metadata.getColumns()));
    setInMemoryMetadata(metadata);
}

String StorageObjectStorage::getName() const
{
    return configuration->getEngineName();
}

bool StorageObjectStorage::prefersLargeBlocks() const
{
    return FormatFactory::instance().checkIfOutputFormatPrefersLargeBlocks(configuration->format);
}

bool StorageObjectStorage::parallelizeOutputAfterReading(ContextPtr context) const
{
    return FormatFactory::instance().checkParallelizeOutputAfterReading(configuration->format, context);
}

bool StorageObjectStorage::supportsSubsetOfColumns(const ContextPtr & context) const
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(configuration->format, context, format_settings);
}

void StorageObjectStorage::updateConfiguration(ContextPtr context)
{
    if (!configuration->isStaticConfiguration())
        object_storage->applyNewSettings(context->getConfigRef(), "s3.", context);
}

void StorageObjectStorage::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    updateConfiguration(local_context);
    if (partition_by && configuration->withWildcard())
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Reading from a partitioned {} storage is not implemented yet",
                        getName());
    }

    const auto read_from_format_info = prepareReadingFromFormat(
        column_names, storage_snapshot, supportsSubsetOfColumns(local_context));
    const bool need_only_count = (query_info.optimize_trivial_count || read_from_format_info.requested_columns.empty())
        && local_context->getSettingsRef().optimize_count_from_files;

    auto read_step = std::make_unique<ReadFromObjectStorageStep>(
        object_storage,
        configuration,
        getName(),
        column_names,
        getVirtualsList(),
        query_info,
        storage_snapshot,
        format_settings,
        distributed_processing,
        std::move(read_from_format_info),
        getSchemaCache(local_context),
        need_only_count,
        local_context,
        max_block_size,
        num_streams);

    query_plan.addStep(std::move(read_step));
}

SinkToStoragePtr StorageObjectStorage::write(
    const ASTPtr & query,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr local_context,
    bool /* async_insert */)
{
    updateConfiguration(local_context);
    const auto sample_block = metadata_snapshot->getSampleBlock();
    const auto & settings = configuration->getQuerySettings(local_context);

    if (configuration->withWildcard())
    {
        ASTPtr partition_by_ast = nullptr;
        if (auto insert_query = std::dynamic_pointer_cast<ASTInsertQuery>(query))
        {
            if (insert_query->partition_by)
                partition_by_ast = insert_query->partition_by;
            else
                partition_by_ast = partition_by;
        }

        if (partition_by_ast)
        {
            return std::make_shared<PartitionedStorageObjectStorageSink>(
                object_storage, configuration, format_settings, sample_block, local_context, partition_by_ast);
        }
    }

    if (configuration->withGlobs())
    {
        throw Exception(
            ErrorCodes::DATABASE_ACCESS_DENIED,
            "{} key '{}' contains globs, so the table is in readonly mode",
            getName(), configuration->getPath());
    }

    auto & paths = configuration->getPaths();
    if (auto new_key = checkAndGetNewFileOnInsertIfNeeded(
            *object_storage, *configuration, settings, paths.front(), paths.size()))
    {
        paths.push_back(*new_key);
    }

    return std::make_shared<StorageObjectStorageSink>(
        object_storage,
        configuration->clone(),
        format_settings,
        sample_block,
        local_context);
}

void StorageObjectStorage::truncate(
    const ASTPtr &,
    const StorageMetadataPtr &,
    ContextPtr,
    TableExclusiveLockHolder &)
{
    if (configuration->withGlobs())
    {
        throw Exception(
            ErrorCodes::DATABASE_ACCESS_DENIED,
            "{} key '{}' contains globs, so the table is in readonly mode and cannot be truncated",
            getName(), configuration->getPath());
    }

    StoredObjects objects;
    for (const auto & key : configuration->getPaths())
        objects.emplace_back(key);

    object_storage->removeObjectsIfExist(objects);
}

std::unique_ptr<ReadBufferIterator> StorageObjectStorage::createReadBufferIterator(
    const ObjectStoragePtr & object_storage,
    const ConfigurationPtr & configuration,
    const std::optional<FormatSettings> & format_settings,
    ObjectInfos & read_keys,
    const ContextPtr & context)
{
    auto file_iterator = StorageObjectStorageSource::createFileIterator(
        configuration,
        object_storage,
        false/* distributed_processing */,
        context,
        {}/* predicate */,
        {}/* virtual_columns */,
        &read_keys);

    return std::make_unique<ReadBufferIterator>(
        object_storage, configuration, file_iterator,
        format_settings, getSchemaCache(context, configuration->getTypeName()), read_keys, context);
}

ColumnsDescription StorageObjectStorage::resolveSchemaFromData(
    const ObjectStoragePtr & object_storage,
    const ConfigurationPtr & configuration,
    const std::optional<FormatSettings> & format_settings,
    const ContextPtr & context)
{
    ObjectInfos read_keys;
    auto read_buffer_iterator = createReadBufferIterator(
        object_storage, configuration, format_settings, read_keys, context);
    return readSchemaFromFormat(
        configuration->format, format_settings, *read_buffer_iterator, context);
}

std::string StorageObjectStorage::resolveFormatFromData(
    const ObjectStoragePtr & object_storage,
    const ConfigurationPtr & configuration,
    const std::optional<FormatSettings> & format_settings,
    const ContextPtr & context)
{
    ObjectInfos read_keys;
    auto read_buffer_iterator = createReadBufferIterator(
        object_storage, configuration, format_settings, read_keys, context);
    return detectFormatAndReadSchema(
        format_settings, *read_buffer_iterator, context).second;
}

std::pair<ColumnsDescription, std::string> StorageObjectStorage::resolveSchemaAndFormatFromData(
    const ObjectStoragePtr & object_storage,
    const ConfigurationPtr & configuration,
    const std::optional<FormatSettings> & format_settings,
    const ContextPtr & context)
{
    ObjectInfos read_keys;
    auto read_buffer_iterator = createReadBufferIterator(
        object_storage, configuration, format_settings, read_keys, context);

    auto [columns, format] = detectFormatAndReadSchema(format_settings, *read_buffer_iterator, context);
    configuration->format = format;
    return std::pair(columns, format);
}

SchemaCache & StorageObjectStorage::getSchemaCache(const ContextPtr & context)
{
    return getSchemaCache(context, configuration->getTypeName());
}

SchemaCache & StorageObjectStorage::getSchemaCache(const ContextPtr & context, const std::string & storage_type_name)
{
    if (storage_type_name == "s3")
    {
        static SchemaCache schema_cache(
            context->getConfigRef().getUInt(
                "schema_inference_cache_max_elements_for_s3",
                DEFAULT_SCHEMA_CACHE_ELEMENTS));
        return schema_cache;
    }
    else if (storage_type_name == "hdfs")
    {
        static SchemaCache schema_cache(
            context->getConfigRef().getUInt(
                "schema_inference_cache_max_elements_for_hdfs",
                DEFAULT_SCHEMA_CACHE_ELEMENTS));
        return schema_cache;
    }
    else if (storage_type_name == "azure")
    {
        static SchemaCache schema_cache(
            context->getConfigRef().getUInt(
                "schema_inference_cache_max_elements_for_azure",
                DEFAULT_SCHEMA_CACHE_ELEMENTS));
        return schema_cache;
    }
    else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported storage type: {}", storage_type_name);
}

}
