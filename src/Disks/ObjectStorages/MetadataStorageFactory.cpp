#include <Disks/ObjectStorages/MetadataStorageFactory.h>
#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <Disks/ObjectStorages/MetadataStorageFromPlainObjectStorage.h>
#include <Disks/ObjectStorages/Web/MetadataStorageFromStaticFilesWebServer.h>
#include <Disks/DiskLocal.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
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

MetadataStoragePtr MetadataStorageFactory::create(
    const std::string & name,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    ObjectStoragePtr object_storage,
    const std::string & compatibility_type_hint) const
{
    if (compatibility_type_hint.empty() && !config.has(config_prefix + ".metadata_type"))
    {
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Expected `metadata_type` in config");
    }

    const auto type = config.getString(config_prefix + ".metadata_type");
    const auto it = registry.find(type);

    if (it == registry.end())
    {
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG,
                        "MetadataStorageFactory: unknown metadata storage type: {}", type);
    }

    return it->second(name, config, config_prefix, object_storage);
}

void registerMetadataStorageFromDisk(MetadataStorageFactory & factory)
{
    auto creator = [](
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ObjectStoragePtr object_storage) -> MetadataStoragePtr
    {
        auto metadata_path = config.getString(config_prefix + ".metadata_path",
                                              fs::path(Context::getGlobalContextInstance()->getPath()) / "disks" / name / "");
        fs::create_directories(metadata_path);
        auto metadata_disk = std::make_shared<DiskLocal>(name + "-metadata", metadata_path, 0, config, config_prefix);
        return std::make_shared<MetadataStorageFromDisk>(metadata_disk, object_storage->getBasePath());
    };
    factory.registerMetadataStorageType("local", creator);
}

void registerMetadataStorageFromDiskPlain(MetadataStorageFactory & factory)
{
    auto creator = [](
        const std::string & /* name */,
        const Poco::Util::AbstractConfiguration & /* config */,
        const std::string & /* config_prefix */,
        ObjectStoragePtr object_storage) -> MetadataStoragePtr
    {
        return std::make_shared<MetadataStorageFromPlainObjectStorage>(object_storage, object_storage->getBasePath());
    };
    factory.registerMetadataStorageType("plain", creator);
}

void registerMetadataStorageFromStaticFilesWebServer(MetadataStorageFactory & factory)
{
    auto creator = [](
        const std::string & /* name */,
        const Poco::Util::AbstractConfiguration & /* config */,
        const std::string & /* config_prefix */,
        ObjectStoragePtr object_storage) -> MetadataStoragePtr
    {
        return std::make_shared<MetadataStorageFromStaticFilesWebServer>(assert_cast<const WebObjectStorage &>(*object_storage));
    };
    factory.registerMetadataStorageType("web", creator);
}

}
