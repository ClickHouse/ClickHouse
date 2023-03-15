#include "IVolume.h"

#include <Common/StringUtils/StringUtils.h>
#include <Common/quoteString.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
}


VolumeLoadBalancing parseVolumeLoadBalancing(const String & config)
{
    if (config == "round_robin")
        return VolumeLoadBalancing::ROUND_ROBIN;
    if (config == "least_used")
        return VolumeLoadBalancing::LEAST_USED;
    throw Exception(ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG, "'{}' is not valid load_balancing value", config);
}


IVolume::IVolume(
    String name_,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    DiskSelectorPtr disk_selector)
    : name(std::move(name_))
    , load_balancing(parseVolumeLoadBalancing(config.getString(config_prefix + ".load_balancing", "round_robin")))
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    for (const auto & disk : keys)
    {
        if (startsWith(disk, "disk"))
        {
            auto disk_name = config.getString(config_prefix + "." + disk);
            disks.push_back(disk_selector->get(disk_name));
        }
    }

    if (disks.empty())
        throw Exception("Volume must contain at least one disk", ErrorCodes::NO_ELEMENTS_IN_CONFIG);
}

UInt64 IVolume::getMaxUnreservedFreeSpace() const
{
    UInt64 res = 0;
    for (const auto & disk : disks)
        res = std::max(res, disk->getUnreservedSpace());
    return res;
}

}
