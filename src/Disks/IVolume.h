#pragma once

#include <Disks/IDisk.h>
#include <Disks/DiskSelector.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

class VolumeType
{
public:
    enum Value
    {
        JBOD,
        SINGLE_DISK,
        UNKNOWN
    };
    VolumeType() : value(UNKNOWN) {}
    VolumeType(Value value_) : value(value_) {}

    bool operator==(const VolumeType & other) const
    {
        return value == other.value;
    }

    bool operator!=(const VolumeType & other) const
    {
        return !(*this == other);
    }

    void fromString(const String & str);
    String toString() const;

private:
    Value value;
};

class IVolume;
using VolumePtr = std::shared_ptr<IVolume>;
using Volumes = std::vector<VolumePtr>;

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
    IVolume(const String & name_, Disks disks_): disks(std::move(disks_)), name(name_)
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
    virtual VolumeType getType() const = 0;

    /// Return biggest unreserved space across all disks
    UInt64 getMaxUnreservedFreeSpace() const;

    DiskPtr getDisk(size_t i = 0) const { return disks[i]; }
    const Disks & getDisks() const { return disks; }

protected:
    Disks disks;
    const String name;
};

}
