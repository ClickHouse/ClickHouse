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

void StorageObjectStorageConfiguration::update( ///NOLINT
    ObjectStoragePtr object_storage_ptr,
    ContextPtr context)
{
    IObjectStorage::ApplyNewSettingsOptions options{.allow_client_change = !isStaticConfiguration()};
    object_storage_ptr->applyNewSettings(context->getConfigRef(), getTypeName() + ".", context, options);
}

void StorageObjectStorageConfiguration::lazyInitializeIfNeeded(
    ObjectStoragePtr object_storage_ptr,
    ContextPtr context)
{
    update(object_storage_ptr, context);
}

ObjectStoragePtr StorageObjectStorageConfiguration::createObjectStorage(
    ContextPtr context, bool is_readonly, CredentialsConfigurationCallback refresh_credentials_callback)
{
    if (ready_object_storage)
        return ready_object_storage;
    return doCreateObjectStorage(context, is_readonly, refresh_credentials_callback);
}

ReadFromFormatInfo StorageObjectStorageConfiguration::prepareReadingFromFormat(
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

namespace
{

/// Dispatch a static factory call to the concrete configuration type via a generic lambda.
/// The lambda receives a `std::type_identity<ConcreteConfig>` tag and must call
/// the appropriate static `from*` method, returning a pair of (config, table_options).
template <typename Func>
std::pair<StorageObjectStorageConfigurationPtr, StorageObjectStorageTableOptions>
dispatchByStorageType(ObjectStorageType type, Func && func)
{
    switch (type)
    {
#if USE_AWS_S3
        case ObjectStorageType::S3:
            return func(std::type_identity<StorageS3Configuration>{});
#endif
#if USE_AZURE_BLOB_STORAGE
        case ObjectStorageType::Azure:
            return func(std::type_identity<StorageAzureConfiguration>{});
#endif
#if USE_HDFS
        case ObjectStorageType::HDFS:
            return func(std::type_identity<StorageHDFSConfiguration>{});
#endif
        case ObjectStorageType::Local:
            return func(std::type_identity<StorageLocalConfiguration>{});
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported object storage type: {}", static_cast<int>(type));
    }
}

}

StorageObjectStorageConfigurationPtr StorageObjectStorageConfiguration::createByType(ObjectStorageType type)
{
    auto [config, _] = dispatchByStorageType(type, [](auto tag)
    {
        using ConfigType = typename decltype(tag)::type;
        return std::pair<StorageObjectStorageConfigurationPtr, StorageObjectStorageTableOptions>{std::make_shared<ConfigType>(), {}};
    });
    return config;
}

StorageObjectStorageTableOptions StorageObjectStorageConfiguration::postInitializeExisting(
    StorageObjectStorageConfiguration & configuration,
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

std::pair<StorageObjectStorageConfigurationPtr, StorageObjectStorageTableOptions>
StorageObjectStorageConfiguration::initialize(
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

    auto [configuration, table_options] = [&]
    {
        if (!disk_name.empty())
        {
            return dispatchByStorageType(type, [&](auto tag)
            {
                using ConfigType = typename decltype(tag)::type;
                return ConfigType::fromDisk(disk_name, engine_args, local_context, with_table_structure);
            });
        }
        else if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, local_context, true, nullptr, table_id))
        {
            return dispatchByStorageType(type, [&](auto tag)
            {
                using ConfigType = typename decltype(tag)::type;
                return ConfigType::fromNamedCollection(*named_collection, local_context);
            });
        }
        else
        {
            return dispatchByStorageType(type, [&](auto tag)
            {
                using ConfigType = typename decltype(tag)::type;
                return ConfigType::fromAST(engine_args, local_context, with_table_structure);
            });
        }
    }();

    postInitializeExisting(*configuration, table_options, local_context, disk_name);
    return {configuration, std::move(table_options)};
}

bool StorageObjectStorageConfiguration::Path::hasPartitionWildcard() const
{
    static const String PARTITION_ID_WILDCARD = "{_partition_id}";
    return path.find(PARTITION_ID_WILDCARD) != String::npos;
}

bool StorageObjectStorageConfiguration::Path::hasSchemaHashWildcard() const
{
    return path.find(StorageObjectStorageConfiguration::SCHEMA_HASH_WILDCARD) != String::npos;
}

bool StorageObjectStorageConfiguration::Path::hasGlobsIgnorePlaceholders() const
{
    if (!hasPartitionWildcard() && !hasSchemaHashWildcard())
        return hasGlobs();
    String cleaned = PartitionedSink::replaceWildcards(path, "");
    boost::replace_all(cleaned, StorageObjectStorageConfiguration::SCHEMA_HASH_WILDCARD, "");
    return cleaned.find_first_of("*?{") != std::string::npos;
}

bool StorageObjectStorageConfiguration::Path::hasGlobs() const
{
    return path.find_first_of("*?{") != std::string::npos;
}

std::string StorageObjectStorageConfiguration::Path::cutGlobs(bool supports_partial_prefix) const
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

bool StorageObjectStorageConfiguration::isNamespaceWithGlobs() const
{
    return getNamespace().find_first_of("*?{") != std::string::npos;
}

bool StorageObjectStorageConfiguration::isPathInArchiveWithGlobs() const
{
    return getPathInArchive().find_first_of("*?{") != std::string::npos;
}

std::string StorageObjectStorageConfiguration::getPathInArchive() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Path {} is not archive", getRawPath().path);
}

void StorageObjectStorageConfiguration::assertInitialized() const
{
    if (!initialized)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Configuration was not initialized before usage");
    }
}

}
