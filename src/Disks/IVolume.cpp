#include "IVolume.h"

#include <Common/StringUtils/StringUtils.h>
#include <Common/quoteString.h>

#include <memory>

namespace DB
{
namespace ErrorCodes
{
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int UNKNOWN_VOLUME_TYPE;
}

void VolumeType::fromString(const String & str)
{
    if (str == "JBOD")
        value = JBOD;
    else if (str == "SINGLE_DISK")
        value = SINGLE_DISK;
    else
        throw DB::Exception("Unexpected string for volume type: " + str, ErrorCodes::UNKNOWN_VOLUME_TYPE);
}

String VolumeType::toString() const
{
    switch (value)
    {
        case JBOD:
            return "JBOD";
        case SINGLE_DISK:
            return "SINGLE_DISK";
        default:
            return "Unknown";
    }
}

IVolume::IVolume(
    String name_, const Poco::Util::AbstractConfiguration & config, const String & config_prefix, DiskSelectorPtr disk_selector)
    : name(std::move(name_))
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
        throw Exception("Volume must contain at least one disk.", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
}

UInt64 IVolume::getMaxUnreservedFreeSpace() const
{
    UInt64 res = 0;
    for (const auto & disk : disks)
        res = std::max(res, disk->getUnreservedSpace());
    return res;
}

}
