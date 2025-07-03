#include <Disks/IVolume.h>

#include <Common/StringUtils.h>
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
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Volume {} must contain at least one disk", name);
}

std::optional<UInt64> IVolume::getMaxUnreservedFreeSpace() const
{
    std::optional<UInt64> res;
    for (const auto & disk : disks)
    {
        auto disk_unreserved_space = disk->getUnreservedSpace();
        if (!disk_unreserved_space)
            return std::nullopt; /// There is at least one unlimited disk.

        if (!res || *disk_unreserved_space > *res)
            res = disk_unreserved_space;
    }
    return res;
}

}
