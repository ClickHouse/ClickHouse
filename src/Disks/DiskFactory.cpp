#include "DiskFactory.h"

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

void DiskFactory::registerDiskType(const String & disk_type, Creator creator)
{
    if (!registry.emplace(disk_type, creator).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DiskFactory: the disk type '{}' is not unique", disk_type);
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

}
