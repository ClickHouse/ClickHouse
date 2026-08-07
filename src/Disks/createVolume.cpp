#include <Disks/createVolume.h>

#include <Disks/SingleDiskVolume.h>
#include <Disks/VolumeJBOD.h>

#include <boost/algorithm/string.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_RAID_TYPE;
    extern const int INVALID_RAID_TYPE;
}

VolumePtr createVolumeFromReservation(const ReservationPtr & reservation, VolumePtr other_volume)
{
    if (other_volume->getType() == VolumeType::JBOD || other_volume->getType() == VolumeType::SINGLE_DISK)
    {
        /// Since reservation on JBOD chooses one of disks and makes reservation there, volume
        /// for such type of reservation will be with one disk.
        return std::make_shared<SingleDiskVolume>(other_volume->getName(), reservation->getDisk(), other_volume->max_data_part_size);
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
    String raid_type = config.getString(config_prefix + ".raid_type", "JBOD");
    if (raid_type == "JBOD")
    {
        return std::make_shared<VolumeJBOD>(name, config, config_prefix, disk_selector);
    }
    throw Exception(ErrorCodes::UNKNOWN_RAID_TYPE, "Unknown RAID type '{}'", raid_type);
}

VolumePtr updateVolumeFromConfig(
    VolumePtr volume,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    DiskSelectorPtr & disk_selector
)
{
    String raid_type = config.getString(config_prefix + ".raid_type", "JBOD");
    if (raid_type == "JBOD")
    {
        VolumeJBODPtr volume_jbod = std::dynamic_pointer_cast<VolumeJBOD>(volume);
        if (!volume_jbod)
            throw Exception(ErrorCodes::INVALID_RAID_TYPE, "Invalid RAID type '{}', shall be JBOD", raid_type);

        return std::make_shared<VolumeJBOD>(*volume_jbod, config, config_prefix, disk_selector);
    }
    throw Exception(ErrorCodes::UNKNOWN_RAID_TYPE, "Unknown RAID type '{}'", raid_type);
}

}
