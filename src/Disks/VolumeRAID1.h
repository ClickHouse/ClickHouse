#pragma once

#include <Disks/createVolume.h>
#include <Disks/VolumeJBOD.h>

namespace DB
{

/// Volume which reserserves space on each underlying disk.
///
/// NOTE: Just interface implementation, doesn't used in codebase,
/// also not available for user.
class VolumeRAID1 : public VolumeJBOD
{
public:
    VolumeRAID1(String name_, Disks disks_, UInt64 max_data_part_size_)
        : VolumeJBOD(name_, disks_, max_data_part_size_)
    {
    }

    VolumeRAID1(
        String name_,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        DiskSelectorPtr disk_selector)
        : VolumeJBOD(name_, config, config_prefix, disk_selector)
    {
    }

    VolumeType getType() const override { return VolumeType::RAID1; }

    ReservationPtr reserve(UInt64 bytes) override;
};

using VolumeRAID1Ptr = std::shared_ptr<VolumeRAID1>;

}
