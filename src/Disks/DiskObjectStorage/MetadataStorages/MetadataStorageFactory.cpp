#include <Common/assert_cast.h>
#include <Common/Macros.h>
#include <Disks/DiskObjectStorage/MetadataStorages/MetadataStorageFactory.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Local/MetadataStorageFromDisk.h>
#if CLICKHOUSE_CLOUD
    #include <Disks/DiskObjectStorage/MetadataStorages/Keeper/MetadataStorageFromKeeper.h>
#endif
#include <Disks/DiskObjectStorage/MetadataStorages/Plain/MetadataStorageFromPlainObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Web/MetadataStorageFromStaticFilesWebServer.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Memory/MetadataStorageInMemory.h>
#include <Disks/DiskLocal.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int LOGICAL_ERROR;
}

namespace
{

void checkSingleLocation(const ClusterConfigurationPtr & cluster)
{
    if (cluster->getConfiguration().size() > 1)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Disk supports only single location clusters");
}

std::string getObjectKeyCompatiblePrefix(
    const ObjectStoragePtr & object_storage,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix)
{
    std::string prefix = config.getString(config_prefix + ".key_compatibility_prefix", object_storage->getCommonKeyPrefix());
    Macros::MacroExpansionInfo info;
    info.ignore_unknown = true;
    info.expand_special_macros_only = true;
    info.replica = Context::getGlobalContextInstance()->getMacros()->tryGetValue("replica");
    return Context::getGlobalContextInstance()->getMacros()->expand(prefix, info);
}

}

MetadataStorageFactory & MetadataStorageFactory::instance()
{
    static MetadataStorageFactory factory;
    return factory;
}

void MetadataStorageFactory::registerMetadataStorageType(const std::string & metadata_type, Creator creator)
{
    if (!registry.emplace(metadata_type, creator).second)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "MetadataStorageFactory: the metadata type '{}' is not unique",
                        metadata_type);
    }
}

std::string MetadataStorageFactory::getCompatibilityMetadataTypeHint(
    const ClusterConfigurationPtr & cluster,
    const ObjectStorageRouterPtr & object_storages)
{
    switch (object_storages->takePointingTo(cluster->getLocalLocation())->getType())
    {
        case ObjectStorageType::S3:
        case ObjectStorageType::HDFS:
        case ObjectStorageType::Local:
        case ObjectStorageType::Azure:
            return "local";
        case ObjectStorageType::Web:
            return "web";
        case ObjectStorageType::BorrowFromCache:
            return "memory";
        default:
            return "";
    }
}

std::string MetadataStorageFactory::getMetadataType(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const std::string & compatibility_type_hint)
{
    if (compatibility_type_hint.empty() && !config.has(config_prefix + ".metadata_type"))
    {
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Expected `metadata_type` in config");
    }

    return config.getString(config_prefix + ".metadata_type", compatibility_type_hint);
}

MetadataStoragePtr MetadataStorageFactory::create(
    const std::string & name,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const ClusterConfigurationPtr & cluster,
    const ObjectStorageRouterPtr & object_storages,
    const std::string & compatibility_type_hint) const
{
    const auto type = getMetadataType(config, config_prefix, compatibility_type_hint);

    /// `borrow_from_cache` object storage loses all data on restart,
    /// so it must use in-memory metadata; any persistent metadata type would leave stale entries.
    /// The check is based on the actual object storage type rather than the compatibility hint,
    /// because the hint is only set when `metadata_type` is missing from the config.
    const auto object_storage_type = object_storages->takePointingTo(cluster->getLocalLocation())->getType();
    if (object_storage_type == ObjectStorageType::BorrowFromCache && type != "memory")
    {
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                        "Object storage type `borrow_from_cache` requires metadata_type='memory', got '{}'", type);
    }

    /// The inverse direction: in-memory metadata is lost on restart, so it is only sound with an
    /// object storage that is also non-durable. With a durable backend (`s3`, `azure_blob_storage`,
    /// `local_blob_storage`, etc.) a restart would make the data inaccessible while leaking the
    /// underlying blobs, which have no remaining metadata path for cleanup. Fail close instead.
    if (type == "memory" && object_storage_type != ObjectStorageType::BorrowFromCache)
    {
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                        "Metadata type `memory` requires object_storage_type='borrow_from_cache', "
                        "because in-memory metadata is lost on restart and would orphan blobs in durable object storage");
    }

    const auto it = registry.find(type);

    if (it == registry.end())
    {
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG,
                        "MetadataStorageFactory: unknown metadata storage type: {}", type);
    }

    return it->second(name, config, config_prefix, cluster, object_storages);
}

static void registerMetadataStorageFromDisk(MetadataStorageFactory & factory)
{
    factory.registerMetadataStorageType("local", [](
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ClusterConfigurationPtr & cluster,
        const ObjectStorageRouterPtr & object_storages) -> MetadataStoragePtr
    {
        checkSingleLocation(cluster);

        auto metadata_path = config.getString(config_prefix + ".metadata_path",
                                              fs::path(Context::getGlobalContextInstance()->getPath()) / "disks" / name / "");
        auto metadata_keep_free_space_bytes = config.getUInt64(config_prefix + ".metadata_keep_free_space_bytes", 0);

        fs::create_directories(metadata_path);
        const auto db_disk = std::make_shared<DiskLocal>(name + "-metadata", metadata_path, metadata_keep_free_space_bytes, config, config_prefix);
        const auto local_object_storage = object_storages->takePointingTo(cluster->getLocalLocation());
        auto key_compatibility_prefix = getObjectKeyCompatiblePrefix(local_object_storage, config, config_prefix);
        auto key_generator = local_object_storage->createKeyGenerator();

        bool persistent_removal_log = config.getBool(config_prefix + ".persistent_removal_log", false);
        size_t metadata_removal_log_compaction_threshold = config.getUInt64(config_prefix + ".metadata_removal_log_compaction_threshold", 1000);
        return std::make_shared<MetadataStorageFromDisk>(db_disk, std::move(key_compatibility_prefix), std::move(key_generator), persistent_removal_log, metadata_removal_log_compaction_threshold);
    });
}

#if CLICKHOUSE_CLOUD
static void registerMetadataStorageFromKeeper(MetadataStorageFactory & factory)
{
    factory.registerMetadataStorageType("keeper", [](
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ClusterConfigurationPtr & cluster,
        const ObjectStorageRouterPtr & object_storages) -> MetadataStoragePtr
    {
        auto component_guard = Coordination::setCurrentComponent("registerMetadataStorageFromKeeper");
        LOG_INFO(getLogger("registerDiskS3"), "Using DiskS3 with metadata keeper");

        std::string zookeeper_name = config.getString(config_prefix + ".zookeeper_name", "default");
        const auto local_object_storage = object_storages->takePointingTo(cluster->getLocalLocation());
        const auto key_compatibility_prefix = getObjectKeyCompatiblePrefix(local_object_storage, config, config_prefix);
        const auto key_generator = local_object_storage->createKeyGenerator();
        /// Yes, we place objects in metadata storage from keeper by prefix from s3 object keys.
        /// No reason, it just happened. Now it has to be preserved.
        auto keeper_prefix = key_compatibility_prefix;

        return std::make_shared<MetadataStorageFromKeeper>(
            name, zookeeper_name, keeper_prefix, key_compatibility_prefix, key_generator, config, config_prefix, Context::getGlobalContextInstance());
    });
}
#endif

static void registerPlainMetadataStorage(MetadataStorageFactory & factory)
{
    factory.registerMetadataStorageType("plain", [](
        const std::string & /* name */,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ClusterConfigurationPtr & cluster,
        const ObjectStorageRouterPtr & object_storages) -> MetadataStoragePtr
    {
        checkSingleLocation(cluster);

        const auto local_object_storage = object_storages->takePointingTo(cluster->getLocalLocation());
        std::string key_compatibility_prefix = getObjectKeyCompatiblePrefix(local_object_storage, config, config_prefix);
        size_t object_metadata_cache_size = config.getUInt64(config_prefix + ".object_metadata_cache_size", 0);

        return std::make_shared<MetadataStorageFromPlainObjectStorage>(local_object_storage, key_compatibility_prefix, object_metadata_cache_size);
    });
}

static void registerPlainRewritableMetadataStorage(MetadataStorageFactory & factory)
{
    factory.registerMetadataStorageType("plain_rewritable", [](
        const std::string & /* name */,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ClusterConfigurationPtr & cluster,
        const ObjectStorageRouterPtr & object_storages) -> MetadataStoragePtr
    {
        checkSingleLocation(cluster);

        const auto local_object_storage = object_storages->takePointingTo(cluster->getLocalLocation());
        std::string key_compatibility_prefix = getObjectKeyCompatiblePrefix(local_object_storage, config, config_prefix);

        return std::make_shared<MetadataStorageFromPlainRewritableObjectStorage>(local_object_storage, key_compatibility_prefix);
    });
}

static void registerMetadataStorageFromStaticFilesWebServer(MetadataStorageFactory & factory)
{
    factory.registerMetadataStorageType("web", [](
        const std::string & /* name */,
        const Poco::Util::AbstractConfiguration & /* config */,
        const std::string & /* config_prefix */,
        const ClusterConfigurationPtr & cluster,
        const ObjectStorageRouterPtr & object_storages) -> MetadataStoragePtr
    {
        checkSingleLocation(cluster);

        const auto local_object_storage = object_storages->takePointingTo(cluster->getLocalLocation());

        return std::make_shared<MetadataStorageFromStaticFilesWebServer>(assert_cast<const WebObjectStorage &>(*local_object_storage));
    });
}

static void registerMetadataStorageInMemory(MetadataStorageFactory & factory)
{
    factory.registerMetadataStorageType("memory", [](
        const std::string & /* name */,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ClusterConfigurationPtr & cluster,
        const ObjectStorageRouterPtr & object_storages) -> MetadataStoragePtr
    {
        checkSingleLocation(cluster);

        const auto local_object_storage = object_storages->takePointingTo(cluster->getLocalLocation());
        auto key_compatibility_prefix = getObjectKeyCompatiblePrefix(local_object_storage, config, config_prefix);
        auto key_generator = local_object_storage->createKeyGenerator();

        return std::make_shared<MetadataStorageInMemory>(std::move(key_compatibility_prefix), std::move(key_generator));
    });
}

void registerMetadataStorages();

void registerMetadataStorages()
{
    auto & factory = MetadataStorageFactory::instance();
    registerMetadataStorageFromDisk(factory);
    registerPlainMetadataStorage(factory);
    registerPlainRewritableMetadataStorage(factory);
    registerMetadataStorageFromStaticFilesWebServer(factory);
    registerMetadataStorageInMemory(factory);
#if CLICKHOUSE_CLOUD
    registerMetadataStorageFromKeeper(factory);
#endif
}

void MetadataStorageFactory::clearRegistry()
{
    registry.clear();
}
}
