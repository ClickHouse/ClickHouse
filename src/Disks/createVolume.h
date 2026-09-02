#pragma once

#include <Disks/IVolume.h>

namespace DB
{

VolumePtr createVolumeFromReservation(const ReservationPtr & reservation, VolumePtr other_volume);

VolumePtr createVolumeFromConfig(
    String name_,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    DiskSelectorPtr disk_selector
);

VolumePtr updateVolumeFromConfig(
    VolumePtr volume,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    DiskSelectorPtr & disk_selector
);

}
