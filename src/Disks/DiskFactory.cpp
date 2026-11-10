#include <Disks/DiskFactory.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}

DiskFactory & DiskFactory::instance()
{
    static DiskFactory factory;
    return factory;
}

void DiskFactory::registerDiskType(const String & disk_type, Creator creator, Documentation documentation)
{
    if (!registry.emplace(disk_type, creator).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DiskFactory: the disk type '{}' is not unique", disk_type);
    documentations.emplace(disk_type, std::move(documentation));
}

std::vector<String> DiskFactory::getAllRegisteredNames() const // STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    std::vector<String> result; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    result.reserve(registry.size());
    for (const auto & pair : registry)
        result.push_back(pair.first);
    return result;
}

Documentation DiskFactory::getDocumentation(const String & disk_type) const
{
    if (auto it = documentations.find(disk_type); it != documentations.end())
        return it->second;
    return {};
}

DiskPtr DiskFactory::create(
    const String & name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context,
    const DisksMap & map,
    bool attach,
    bool custom_disk,
    const std::unordered_set<String> & skip_types) const
{
    const auto disk_type = config.getString(config_prefix + ".type", "local");

    const auto found = registry.find(disk_type);
    if (found == registry.end())
    {
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG,
                        "DiskFactory: the disk '{}' has unknown disk type: {}", name, disk_type);
    }

    if (skip_types.contains(found->first))
    {
        return nullptr;
    }

    const auto & disk_creator = found->second;
    return disk_creator(name, config, config_prefix, context, map, attach, custom_disk);
}

void DiskFactory::clearRegistry()
{
    registry.clear();
    documentations.clear();
}
}
