#include <Storages/ObjectStorage/StorageObjectStorage.h>

#include <Formats/FormatFactory.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Transforms/ExtractColumnsTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/StorageFactory.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/ObjectStorage/Configuration.h>
#include <Storages/ObjectStorage/Settings.h>
#include <Storages/ObjectStorage/StorageObjectStorageSink.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/ReadBufferIterator.h>
#include <Storages/ObjectStorage/ReadFromObjectStorage.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DATABASE_ACCESS_DENIED;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;

}

template <typename StorageSettings>
std::unique_ptr<StorageInMemoryMetadata> getStorageMetadata(
    ObjectStoragePtr object_storage,
    const StorageObjectStorageConfigurationPtr & configuration,
    const ColumnsDescription & columns,
    const ConstraintsDescription & constraints,
    std::optional<FormatSettings> format_settings,
    const String & comment,
    const std::string & engine_name,
    const ContextPtr & context)
{
    auto storage_metadata = std::make_unique<StorageInMemoryMetadata>();
    if (columns.empty())
    {
        auto fetched_columns = StorageObjectStorage<StorageSettings>::getTableStructureFromData(
            object_storage, configuration, format_settings, context);
        storage_metadata->setColumns(fetched_columns);
    }
    else
    {
        /// We don't allow special columns.
        if (!columns.hasOnlyOrdinary())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Table engine {} doesn't support special columns "
                            "like MATERIALIZED, ALIAS or EPHEMERAL",
                            engine_name);

        storage_metadata->setColumns(columns);
    }

    storage_metadata->setConstraints(constraints);
    storage_metadata->setComment(comment);
    return storage_metadata;
}

template <typename StorageSettings>
StorageObjectStorage<StorageSettings>::StorageObjectStorage(
    ConfigurationPtr configuration_,
    ObjectStoragePtr object_storage_,
    const String & engine_name_,
    ContextPtr context,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    std::optional<FormatSettings> format_settings_,
    bool distributed_processing_,
    ASTPtr partition_by_)
    : IStorage(table_id_, getStorageMetadata<StorageSettings>(
                   object_storage_, configuration_, columns_, constraints_, format_settings_,
                   comment, engine_name, context))
    , engine_name(engine_name_)
    , virtual_columns(VirtualColumnUtils::getPathFileAndSizeVirtualsForStorage(
                          getInMemoryMetadataPtr()->getSampleBlock().getNamesAndTypesList()))
    , format_settings(format_settings_)
    , partition_by(partition_by_)
    , distributed_processing(distributed_processing_)
    , object_storage(object_storage_)
    , configuration(configuration_)
{
    FormatFactory::instance().checkFormatName(configuration->format);
    configuration->check(context);

    StoredObjects objects;
    for (const auto & key : configuration->getPaths())
        objects.emplace_back(key);
}

template <typename StorageSettings>
Names StorageObjectStorage<StorageSettings>::getVirtualColumnNames()
{
    return VirtualColumnUtils::getPathFileAndSizeVirtualsForStorage({}).getNames();
}

template <typename StorageSettings>
bool StorageObjectStorage<StorageSettings>::supportsSubsetOfColumns(const ContextPtr & context) const
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(configuration->format, context, format_settings);
}

template <typename StorageSettings>
bool StorageObjectStorage<StorageSettings>::prefersLargeBlocks() const
{
    return FormatFactory::instance().checkIfOutputFormatPrefersLargeBlocks(configuration->format);
}

template <typename StorageSettings>
bool StorageObjectStorage<StorageSettings>::parallelizeOutputAfterReading(ContextPtr context) const
{
    return FormatFactory::instance().checkParallelizeOutputAfterReading(configuration->format, context);
}

template <typename StorageSettings>
std::pair<StorageObjectStorageConfigurationPtr, ObjectStoragePtr>
StorageObjectStorage<StorageSettings>::updateConfigurationAndGetCopy(ContextPtr local_context)
{
    std::lock_guard lock(configuration_update_mutex);
    auto new_object_storage = configuration->createOrUpdateObjectStorage(local_context);
    if (new_object_storage)
        object_storage = new_object_storage;
    return {configuration, object_storage};
}

template <typename StorageSettings>
SchemaCache & StorageObjectStorage<StorageSettings>::getSchemaCache(const ContextPtr & context)
{
    static SchemaCache schema_cache(
        context->getConfigRef().getUInt(
            StorageSettings::SCHEMA_CACHE_MAX_ELEMENTS_CONFIG_SETTING,
            DEFAULT_SCHEMA_CACHE_ELEMENTS));
    return schema_cache;
}

template <typename StorageSettings>
void StorageObjectStorage<StorageSettings>::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    if (partition_by && configuration->withWildcard())
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Reading from a partitioned {} storage is not implemented yet",
                        getName());
    }

    auto this_ptr = std::static_pointer_cast<StorageObjectStorage>(shared_from_this());
    auto read_from_format_info = prepareReadingFromFormat(
        column_names, storage_snapshot, supportsSubsetOfColumns(local_context), getVirtuals());
    bool need_only_count = (query_info.optimize_trivial_count || read_from_format_info.requested_columns.empty())
        && local_context->getSettingsRef().optimize_count_from_files;

    auto [query_configuration, query_object_storage] = updateConfigurationAndGetCopy(local_context);
    auto reading = std::make_unique<ReadFromStorageObejctStorage<StorageSettings>>(
        query_object_storage,
        query_configuration,
        getName(),
        virtual_columns,
        format_settings,
        distributed_processing,
        std::move(read_from_format_info),
        need_only_count,
        local_context,
        max_block_size,
        num_streams);

    query_plan.addStep(std::move(reading));
}

template <typename StorageSettings>
SinkToStoragePtr StorageObjectStorage<StorageSettings>::write(
    const ASTPtr & query,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr local_context,
    bool /* async_insert */)
{
    auto insert_query = std::dynamic_pointer_cast<ASTInsertQuery>(query);
    auto partition_by_ast = insert_query
        ? (insert_query->partition_by ? insert_query->partition_by : partition_by)
        : nullptr;
    bool is_partitioned_implementation = partition_by_ast && configuration->withWildcard();

    auto sample_block = metadata_snapshot->getSampleBlock();
    auto storage_settings = StorageSettings::create(local_context->getSettingsRef());

    if (is_partitioned_implementation)
    {
        return std::make_shared<PartitionedStorageObjectStorageSink>(
            object_storage, configuration, format_settings, sample_block, local_context, partition_by_ast);
    }

    if (configuration->isPathWithGlobs() || configuration->isNamespaceWithGlobs())
    {
        throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED,
                        "{} key '{}' contains globs, so the table is in readonly mode",
                        getName(), configuration->getPath());
    }

    if (!storage_settings.truncate_on_insert
        && object_storage->exists(StoredObject(configuration->getPath())))
    {
        if (storage_settings.create_new_file_on_insert)
        {
            size_t index = configuration->getPaths().size();
            const auto & first_key = configuration->getPaths()[0];
            auto pos = first_key.find_first_of('.');
            String new_key;

            do
            {
                new_key = first_key.substr(0, pos)
                    + "."
                    + std::to_string(index)
                    + (pos == std::string::npos ? "" : first_key.substr(pos));
                ++index;
            }
            while (object_storage->exists(StoredObject(new_key)));

            configuration->getPaths().push_back(new_key);
        }
        else
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Object in bucket {} with key {} already exists. "
                "If you want to overwrite it, enable setting [engine_name]_truncate_on_insert, if you "
                "want to create a new file on each insert, enable setting [engine_name]_create_new_file_on_insert",
                configuration->getNamespace(), configuration->getPaths().back());
        }
    }

    return std::make_shared<StorageObjectStorageSink>(
        object_storage, configuration, format_settings, sample_block, local_context);
}

template <typename StorageSettings>
void StorageObjectStorage<StorageSettings>::truncate(
    const ASTPtr &,
    const StorageMetadataPtr &,
    ContextPtr,
    TableExclusiveLockHolder &)
{
    if (configuration->isPathWithGlobs() || configuration->isNamespaceWithGlobs())
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

template <typename StorageSettings>
ColumnsDescription StorageObjectStorage<StorageSettings>::getTableStructureFromData(
    ObjectStoragePtr object_storage,
    const ConfigurationPtr & configuration,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr context)
{
    using Source = StorageObjectStorageSource<StorageSettings>;

    ObjectInfos read_keys;
    auto file_iterator = Source::createFileIterator(
        configuration, object_storage, /* distributed_processing */false,
        context, /* predicate */{}, /* virtual_columns */{}, &read_keys);

    ReadBufferIterator<StorageSettings> read_buffer_iterator(
        object_storage, configuration, file_iterator,
        format_settings, read_keys, context);

    const bool retry = configuration->isPathWithGlobs() || configuration->isNamespaceWithGlobs();
    return readSchemaFromFormat(
        configuration->format, format_settings,
        read_buffer_iterator, retry, context);
}

template class StorageObjectStorage<S3StorageSettings>;
template class StorageObjectStorage<AzureStorageSettings>;
template class StorageObjectStorage<HDFSStorageSettings>;

}
