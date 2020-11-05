#pragma once

#include <Disks/IVolume.h>

namespace DB
{

/**
 * Implements something similar to JBOD (https://en.wikipedia.org/wiki/Non-RAID_drive_architectures#JBOD).
 * When MergeTree engine wants to write part — it requests VolumeJBOD to reserve space on the next available
 * disk and then writes new part to that disk.
 */
class VolumeJBOD : public IVolume
{
public:
    VolumeJBOD(String name_, Disks disks_, UInt64 max_data_part_size_)
        : IVolume(name_, disks_, max_data_part_size_)
    {
    }

    VolumeJBOD(
        String name_,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        DiskSelectorPtr disk_selector
    );

    VolumeType getType() const override { return VolumeType::JBOD; }

    /// Always returns next disk (round-robin), ignores argument.
    ///
    /// - Used with policy for temporary data
    /// - Ignores all limitations
    /// - Shares last access with reserve()
    DiskPtr getDisk(size_t index) const override;

    /// Uses Round-robin to choose disk for reservation.
    /// Returns valid reservation or nullptr if there is no space left on any disk.
    ReservationPtr reserve(UInt64 bytes) override;

private:
    mutable std::atomic<size_t> last_used = 0;
};

using VolumeJBODPtr = std::shared_ptr<VolumeJBOD>;
using VolumesJBOD = std::vector<VolumeJBODPtr>;

}
