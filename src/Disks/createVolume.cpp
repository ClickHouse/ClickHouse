#include "createVolume.h"

#include <Disks/SingleDiskVolume.h>
#include <Disks/VolumeJBOD.h>
#include <Disks/VolumeRAID1.h>

#include <boost/algorithm/string.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_RAID_TYPE;
}

VolumePtr createVolumeFromReservation(const ReservationPtr & reservation, VolumePtr other_volume)
{
    if (other_volume->getType() == VolumeType::JBOD || other_volume->getType() == VolumeType::SINGLE_DISK)
    {
        /// Since reservation on JBOD chooses one of disks and makes reservation there, volume
        /// for such type of reservation will be with one disk.
        return std::make_shared<SingleDiskVolume>(other_volume->getName(), reservation->getDisk());
    }
    if (other_volume->getType() == VolumeType::RAID1)
    {
        auto volume = std::dynamic_pointer_cast<VolumeRAID1>(other_volume);
        return std::make_shared<VolumeRAID1>(volume->getName(), reservation->getDisks(), volume->max_data_part_size);
    }
    return nullptr;
}

VolumePtr createVolumeFromConfig(
    String name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    DiskSelectorPtr disk_selector
)
{
    auto has_raid_type = config.has(config_prefix + ".raid_type");
    if (!has_raid_type)
    {
        return std::make_shared<VolumeJBOD>(name, config, config_prefix, disk_selector);
    }
    String raid_type = config.getString(config_prefix + ".raid_type");
    if (raid_type == "JBOD")
    {
        return std::make_shared<VolumeJBOD>(name, config, config_prefix, disk_selector);
    }
    throw Exception("Unknown raid type '" + raid_type + "'", ErrorCodes::UNKNOWN_RAID_TYPE);
}

}
