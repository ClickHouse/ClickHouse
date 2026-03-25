#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Storages/ObjectStorage/StorageObjectStorageTableOptions.h>

#include <Disks/IDisk.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/ObjectStorage/StorageObjectStorageSink.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/Common.h>

#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/Local/Configuration.h>

#include <boost/algorithm/string/replace.hpp>

namespace DB
{

namespace DataLakeStorageSetting
{
    extern const DataLakeStorageSettingsString disk;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

void ObjectStorageConnectionConfiguration::update( ///NOLINT
    ObjectStoragePtr object_storage_ptr,
    ContextPtr context)
{
    IObjectStorage::ApplyNewSettingsOptions options{.allow_client_change = !isStaticConfiguration()};
    object_storage_ptr->applyNewSettings(context->getConfigRef(), getTypeName() + ".", context, options);
}

void ObjectStorageConnectionConfiguration::lazyInitializeIfNeeded(
    ObjectStoragePtr object_storage_ptr,
    ContextPtr context)
{
    update(object_storage_ptr, context);
}

ObjectStoragePtr ObjectStorageConnectionConfiguration::createObjectStorage(
    ContextPtr context, bool is_readonly, CredentialsConfigurationCallback refresh_credentials_callback)
{
    if (ready_object_storage)
        return ready_object_storage;
    return createObjectStorageImpl(context, is_readonly, refresh_credentials_callback);
}

ReadFromFormatInfo ObjectStorageConnectionConfiguration::prepareReadingFromFormat(
    ObjectStoragePtr,
    const Strings & requested_columns,
    const StorageSnapshotPtr & storage_snapshot,
    bool supports_subset_of_columns,
    bool supports_tuple_elements,
    ContextPtr local_context,
    const PrepareReadingFromFormatHiveParams & hive_parameters)
{
    return DB::prepareReadingFromFormat(requested_columns, storage_snapshot, local_context, supports_subset_of_columns, supports_tuple_elements, hive_parameters);
}

ObjectStorageConnectionConfigurationPtr ObjectStorageConnectionConfiguration::createByType(ObjectStorageType type)
{
    switch (type)
    {
#if USE_AWS_S3
        case ObjectStorageType::S3:
            return std::make_shared<StorageS3Configuration>();
#endif
#if USE_AZURE_BLOB_STORAGE
        case ObjectStorageType::Azure:
            return std::make_shared<StorageAzureConfiguration>();
#endif
#if USE_HDFS
        case ObjectStorageType::HDFS:
            return std::make_shared<StorageHDFSConfiguration>();
#endif
        case ObjectStorageType::Local:
            return std::make_shared<StorageLocalConfiguration>();
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported object storage type: {}", static_cast<int>(type));
    }
}

StorageObjectStorageTableOptions ObjectStorageConnectionConfiguration::postInitializeExisting(
    ObjectStorageConnectionConfiguration & configuration,
    StorageObjectStorageTableOptions & table_options,
    ContextPtr local_context,
    const String & disk_name)
{
    if (configuration.isNamespaceWithGlobs())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Expression can not have wildcards inside {} name", configuration.getNamespaceType());

    if (table_options.partition_strategy_type == PartitionStrategyFactory::StrategyType::NONE)
    {
        if (configuration.getRawPath().hasPartitionWildcard())
        {
            // Promote to wildcard in case it is not data lake to make it backwards compatible
            table_options.partition_strategy_type = PartitionStrategyFactory::StrategyType::WILDCARD;
        }
    }

    if (table_options.format == "auto")
    {
        table_options.format
            = FormatFactory::instance()
                  .tryGetFormatFromFileName(configuration.isArchive() ? configuration.getPathInArchive() : configuration.getRawPath().path)
                  .value_or("auto");
    }
    else
        FormatFactory::instance().checkFormatName(table_options.format);

    /// We shouldn't set path for disk setup because path prefix is already set in used object_storage.
    if (disk_name.empty())
        table_options.setPathForRead(configuration.getRawPath());

    configuration.initialized = true;

    if (!disk_name.empty())
    {
        auto disk = local_context->getDisk(disk_name);
        configuration.ready_object_storage = disk->getObjectStorage();
    }

    return table_options;
}

std::pair<ObjectStorageConnectionConfigurationPtr, StorageObjectStorageTableOptions>
ObjectStorageConnectionConfiguration::initialize(
    ObjectStorageType type,
    ASTs & engine_args,
    ContextPtr local_context,
    bool with_table_structure,
    const StorageID * table_id,
    const String & disk_name)
{
    if (!disk_name.empty())
    {
        if (!Context::getGlobalContextInstance()->getAllowedDisksForTableEngines().contains(disk_name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Disk {} is not allowed for usage in storage engines. "
                "The list of allowed disks is defined by `allowed_disks_for_table_engines`", disk_name);
    }

    using FromAST = std::function<ConfigWithOptions(ASTs &, ContextPtr, bool)>;
    using FromNamedCollection = std::function<ConfigWithOptions(const NamedCollection &, ContextPtr)>;
    using FromDisk = std::function<ConfigWithOptions(const String &, ASTs &, ContextPtr, bool)>;

    FromAST from_ast;
    FromNamedCollection from_named_collection;
    FromDisk from_disk;

    switch (type)
    {
#if USE_AWS_S3
        case ObjectStorageType::S3:
            from_ast = fromS3AST;
            from_named_collection = fromS3NamedCollection;
            from_disk = fromS3Disk;
            break;
#endif
#if USE_AZURE_BLOB_STORAGE
        case ObjectStorageType::Azure:
            from_ast = fromAzureAST;
            from_named_collection = fromAzureNamedCollection;
            from_disk = fromAzureDisk;
            break;
#endif
#if USE_HDFS
        case ObjectStorageType::HDFS:
            from_ast = fromHDFSAST;
            from_named_collection = fromHDFSNamedCollection;
            from_disk = fromHDFSDisk;
            break;
#endif
        case ObjectStorageType::Local:
            from_ast = fromLocalAST;
            from_named_collection = fromLocalNamedCollection;
            from_disk = fromLocalDisk;
            break;
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported object storage type: {}", static_cast<int>(type));
    }

    auto [configuration, table_options] = [&]() -> ConfigWithOptions
    {
        if (!disk_name.empty())
            return from_disk(disk_name, engine_args, local_context, with_table_structure);
        else if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, local_context, true, nullptr, table_id))
            return from_named_collection(*named_collection, local_context);
        else
            return from_ast(engine_args, local_context, with_table_structure);
    }();

    postInitializeExisting(*configuration, table_options, local_context, disk_name);
    return {configuration, std::move(table_options)};
}

bool ObjectStorageConnectionConfiguration::Path::hasPartitionWildcard() const
{
    static const String PARTITION_ID_WILDCARD = "{_partition_id}";
    return path.find(PARTITION_ID_WILDCARD) != String::npos;
}

bool ObjectStorageConnectionConfiguration::Path::hasSchemaHashWildcard() const
{
    return path.find(ObjectStorageConnectionConfiguration::SCHEMA_HASH_WILDCARD) != String::npos;
}

bool ObjectStorageConnectionConfiguration::Path::hasGlobsIgnorePlaceholders() const
{
    if (!hasPartitionWildcard() && !hasSchemaHashWildcard())
        return hasGlobs();
    String cleaned = PartitionedSink::replaceWildcards(path, "");
    boost::replace_all(cleaned, ObjectStorageConnectionConfiguration::SCHEMA_HASH_WILDCARD, "");
    return cleaned.find_first_of("*?{") != std::string::npos;
}

bool ObjectStorageConnectionConfiguration::Path::hasGlobs() const
{
    return path.find_first_of("*?{") != std::string::npos;
}

std::string ObjectStorageConnectionConfiguration::Path::cutGlobs(bool supports_partial_prefix) const
{
    if (supports_partial_prefix)
    {
        return path.substr(0, path.find_first_of("*?{"));
    }

    auto first_glob_pos = path.find_first_of("*?{");
    auto end_of_path_without_globs = path.substr(0, first_glob_pos).rfind('/');
    if (end_of_path_without_globs == std::string::npos || end_of_path_without_globs == 0)
        return "/";
    return path.substr(0, end_of_path_without_globs);
}

bool ObjectStorageConnectionConfiguration::isNamespaceWithGlobs() const
{
    return getNamespace().find_first_of("*?{") != std::string::npos;
}

bool ObjectStorageConnectionConfiguration::isPathInArchiveWithGlobs() const
{
    return getPathInArchive().find_first_of("*?{") != std::string::npos;
}

std::string ObjectStorageConnectionConfiguration::getPathInArchive() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Path {} is not archive", getRawPath().path);
}

void ObjectStorageConnectionConfiguration::assertInitialized() const
{
    if (!initialized)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Configuration was not initialized before usage");
    }
}

}
