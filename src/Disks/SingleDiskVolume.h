#pragma once

#include <Disks/IVolume.h>

namespace DB
{

class SingleDiskVolume : public IVolume
{
public:
    SingleDiskVolume(const String & name_, DiskPtr disk): IVolume(name_, {disk})
    {
    }

    ReservationPtr reserve(UInt64 bytes) override
    {
        return disks[0]->reserve(bytes);
    }

    VolumeType getType() const override { return VolumeType::SINGLE_DISK; }

};

using VolumeSingleDiskPtr = std::shared_ptr<SingleDiskVolume>;
using VolumesSingleDiskPtr = std::vector<VolumeSingleDiskPtr>;

}
