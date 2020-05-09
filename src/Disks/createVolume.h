#pragma once

#include <Disks/IVolume.h>
#include <Disks/VolumeJBOD.h>
#include <Disks/SingleDiskVolume.h>

namespace DB
{

VolumePtr createVolumeFromReservation(ReservationPtr reservation, VolumePtr other_volume);

}
