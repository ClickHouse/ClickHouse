#include <Common/assert_cast.h>
#include <Disks/ObjectStorages/MetadataStorageFactory.h>
#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <Disks/ObjectStorages/MetadataStorageFromPlainObjectStorage.h>
#include <Disks/ObjectStorages/MetadataStorageFromPlainRewritableObjectStorage.h>
#include <Disks/ObjectStorages/Web/MetadataStorageFromStaticFilesWebServer.h>
#include <Disks/DiskLocal.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int LOGICAL_ERROR;
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

std::string MetadataStorageFactory::getCompatibilityMetadataTypeHint(const ObjectStorageType & type)
{
    switch (type)
    {
        case ObjectStorageType::S3:
        case ObjectStorageType::HDFS:
        case ObjectStorageType::Local:
        case ObjectStorageType::Azure:
            return "local";
        case ObjectStorageType::Web:
            return "web";
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
    ObjectStoragePtr object_storage,
    const std::string & compatibility_type_hint) const
{
    const auto type = getMetadataType(config, config_prefix, compatibility_type_hint);
    const auto it = registry.find(type);

    if (it == registry.end())
    {
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG,
                        "MetadataStorageFactory: unknown metadata storage type: {}", type);
    }

    return it->second(name, config, config_prefix, object_storage);
}

static std::string getObjectKeyCompatiblePrefix(
    const IObjectStorage & object_storage,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix)
{
    return config.getString(config_prefix + ".key_compatibility_prefix", object_storage.getCommonKeyPrefix());
}

void registerMetadataStorageFromDisk(MetadataStorageFactory & factory)
{
    factory.registerMetadataStorageType("local", [](
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ObjectStoragePtr object_storage) -> MetadataStoragePtr
    {
        auto metadata_path = config.getString(config_prefix + ".metadata_path",
                                              fs::path(Context::getGlobalContextInstance()->getPath()) / "disks" / name / "");
        auto metadata_keep_free_space_bytes = config.getUInt64(config_prefix + ".metadata_keep_free_space_bytes", 0);

        fs::create_directories(metadata_path);
        auto metadata_disk = std::make_shared<DiskLocal>(name + "-metadata", metadata_path, metadata_keep_free_space_bytes, config, config_prefix);
        auto key_compatibility_prefix = getObjectKeyCompatiblePrefix(*object_storage, config, config_prefix);
        return std::make_shared<MetadataStorageFromDisk>(metadata_disk, key_compatibility_prefix);
    });
}

void registerPlainMetadataStorage(MetadataStorageFactory & factory)
{
    factory.registerMetadataStorageType("plain", [](
        const std::string & /* name */,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ObjectStoragePtr object_storage) -> MetadataStoragePtr
    {
        auto key_compatibility_prefix = getObjectKeyCompatiblePrefix(*object_storage, config, config_prefix);
        return std::make_shared<MetadataStorageFromPlainObjectStorage>(
            object_storage, key_compatibility_prefix, config.getUInt64(config_prefix + ".object_metadata_cache_size", 0));
    });
}

void registerPlainRewritableMetadataStorage(MetadataStorageFactory & factory)
{
    factory.registerMetadataStorageType(
        "plain_rewritable",
        [](const std::string & /* name */,
           const Poco::Util::AbstractConfiguration & config,
           const std::string & config_prefix,
           ObjectStoragePtr object_storage) -> MetadataStoragePtr
        {
            auto key_compatibility_prefix = getObjectKeyCompatiblePrefix(*object_storage, config, config_prefix);
            return std::make_shared<MetadataStorageFromPlainRewritableObjectStorage>(
                object_storage, key_compatibility_prefix, config.getUInt64(config_prefix + ".object_metadata_cache_size", 0));
        });
}

void registerMetadataStorageFromStaticFilesWebServer(MetadataStorageFactory & factory)
{
    factory.registerMetadataStorageType("web", [](
        const std::string & /* name */,
        const Poco::Util::AbstractConfiguration & /* config */,
        const std::string & /* config_prefix */,
        ObjectStoragePtr object_storage) -> MetadataStoragePtr
    {
        return std::make_shared<MetadataStorageFromStaticFilesWebServer>(assert_cast<const WebObjectStorage &>(*object_storage));
    });
}

void registerMetadataStorages()
{
    auto & factory = MetadataStorageFactory::instance();
    registerMetadataStorageFromDisk(factory);
    registerPlainMetadataStorage(factory);
    registerPlainRewritableMetadataStorage(factory);
    registerMetadataStorageFromStaticFilesWebServer(factory);
}

}
