#pragma once

#include <Disks/IDisk.h>
#include <Disks/DiskSelector.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

/**
 * Disks group by some (user) criteria. For example,
 * - VolumeJBOD("slow_disks", [d1, d2], 100)
 * - VolumeJBOD("fast_disks", [d3, d4], 200)
 *
 * Here VolumeJBOD is one of implementations of IVolume.
 *
 * Different of implementations of this interface implement different reserve behaviour —
 * VolumeJBOD reserves space on the next disk after the last used, other future implementations
 * will reserve, for example, equal spaces on all disks.
 */
class IVolume : public Space
{
public:
    IVolume(String name_, Disks disks_): disks(std::move(disks_)), name(std::move(name_))
    {
    }

    IVolume(
        String name_,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        DiskSelectorPtr disk_selector
    );

    virtual ReservationPtr reserve(UInt64 bytes) override = 0;

    /// Volume name from config
    const String & getName() const override { return name; }

    /// Return biggest unreserved space across all disks
    UInt64 getMaxUnreservedFreeSpace() const;

    Disks disks;
protected:
    const String name;
};

using VolumePtr = std::shared_ptr<IVolume>;
using Volumes = std::vector<VolumePtr>;

}
