#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>

#include <Storages/NamedCollectionsHelpers.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <Storages/ObjectStorage/StorageObjectStorageSink.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

bool StorageObjectStorageConfiguration::update( ///NOLINT
    ObjectStoragePtr object_storage_ptr,
    ContextPtr context,
    bool /* if_not_updated_before */,
    bool /* check_consistent_with_previous_metadata */)
{
    IObjectStorage::ApplyNewSettingsOptions options{.allow_client_change = !isStaticConfiguration()};
    object_storage_ptr->applyNewSettings(context->getConfigRef(), getTypeName() + ".", context, options);
    return true;
}

void StorageObjectStorageConfiguration::create( ///NOLINT
    ObjectStoragePtr object_storage_ptr,
    ContextPtr context,
    const std::optional<ColumnsDescription> & /*columns*/,
    ASTPtr /*partition_by*/,
    bool /*if_not_exists*/)
{
    IObjectStorage::ApplyNewSettingsOptions options{.allow_client_change = !isStaticConfiguration()};
    object_storage_ptr->applyNewSettings(context->getConfigRef(), getTypeName() + ".", context, options);
}

ReadFromFormatInfo StorageObjectStorageConfiguration::prepareReadingFromFormat(
    ObjectStoragePtr,
    const Strings & requested_columns,
    const StorageSnapshotPtr & storage_snapshot,
    bool supports_subset_of_columns,
    ContextPtr local_context)
{
    return DB::prepareReadingFromFormat(requested_columns, storage_snapshot, local_context, supports_subset_of_columns);
}

std::optional<ColumnsDescription> StorageObjectStorageConfiguration::tryGetTableStructureFromMetadata() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method tryGetTableStructureFromMetadata is not implemented for basic configuration");
}

void StorageObjectStorageConfiguration::initialize(
    StorageObjectStorageConfiguration & configuration_to_initialize,
    ASTs & engine_args,
    ContextPtr local_context,
    bool with_table_structure)
{
    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, local_context))
        configuration_to_initialize.fromNamedCollection(*named_collection, local_context);
    else
        configuration_to_initialize.fromAST(engine_args, local_context, with_table_structure);

    if (configuration_to_initialize.format == "auto")
    {
        if (configuration_to_initialize.isDataLakeConfiguration())
        {
            configuration_to_initialize.format = "Parquet";
        }
        else
        {
            configuration_to_initialize.format
                = FormatFactory::instance()
                      .tryGetFormatFromFileName(configuration_to_initialize.isArchive() ? configuration_to_initialize.getPathInArchive() : configuration_to_initialize.getPath())
                      .value_or("auto");
        }
    }
    else
        FormatFactory::instance().checkFormatName(configuration_to_initialize.format);

    configuration_to_initialize.initialized = true;
}

void StorageObjectStorageConfiguration::check(ContextPtr) const
{
    FormatFactory::instance().checkFormatName(format);
}

bool StorageObjectStorageConfiguration::withPartitionWildcard() const
{
    static const String PARTITION_ID_WILDCARD = "{_partition_id}";
    return getPath().find(PARTITION_ID_WILDCARD) != String::npos
        || getNamespace().find(PARTITION_ID_WILDCARD) != String::npos;
}

bool StorageObjectStorageConfiguration::withGlobsIgnorePartitionWildcard() const
{
    if (!withPartitionWildcard())
        return withGlobs();
    return PartitionedSink::replaceWildcards(getPath(), "").find_first_of("*?{") != std::string::npos;
}

bool StorageObjectStorageConfiguration::isPathWithGlobs() const
{
    return getPath().find_first_of("*?{") != std::string::npos;
}

bool StorageObjectStorageConfiguration::isNamespaceWithGlobs() const
{
    return getNamespace().find_first_of("*?{") != std::string::npos;
}

std::string StorageObjectStorageConfiguration::getPathWithoutGlobs() const
{
    return getPath().substr(0, getPath().find_first_of("*?{"));
}

bool StorageObjectStorageConfiguration::isPathInArchiveWithGlobs() const
{
    return getPathInArchive().find_first_of("*?{") != std::string::npos;
}

std::string StorageObjectStorageConfiguration::getPathInArchive() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Path {} is not archive", getPath());
}

void StorageObjectStorageConfiguration::assertInitialized() const
{
    if (!initialized)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Configuration was not initialized before usage");
    }
}


}
