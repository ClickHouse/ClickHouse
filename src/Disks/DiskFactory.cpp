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

void DiskFactory::registerDiskType(const String & disk_type, DB::DiskFactory::Creator creator)
{
    if (!registry.emplace(disk_type, creator).second)
        throw Exception("DiskFactory: the disk type '" + disk_type + "' is not unique", ErrorCodes::LOGICAL_ERROR);
}

DiskPtr DiskFactory::create(
    const String & name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context) const
{
    const auto disk_type = config.getString(config_prefix + ".type", "local");

    const auto found = registry.find(disk_type);
    if (found == registry.end())
        throw Exception{"DiskFactory: the disk '" + name + "' has unknown disk type: " + disk_type, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG};

    const auto & disk_creator = found->second;
    return disk_creator(name, config, config_prefix, context);
}

}
