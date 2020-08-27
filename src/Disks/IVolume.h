#pragma once

#include <Disks/IDisk.h>
#include <Disks/DiskSelector.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

enum class VolumeType
{
    JBOD,
    RAID1,
    SINGLE_DISK,
    UNKNOWN
};

String volumeTypeToString(VolumeType t);

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
    IVolume(String name_, Disks disks_, size_t max_data_part_size_ = 0)
        : disks(std::move(disks_))
        , name(name_)
        , max_data_part_size(max_data_part_size_)
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

    DiskPtr getDisk() const { return getDisk(0); }
    virtual DiskPtr getDisk(size_t i) const { return disks[i]; }
    const Disks & getDisks() const { return disks; }

protected:
    Disks disks;
    const String name;

public:
    /// Max size of reservation, zero means unlimited size
    UInt64 max_data_part_size = 0;
};

/// Reservation for multiple disks at once. Can be used in RAID1 implementation.
class MultiDiskReservation : public IReservation
{
public:
    MultiDiskReservation(Reservations & reservations, UInt64 size);

    UInt64 getSize() const override { return size; }

    DiskPtr getDisk(size_t i) const override { return reservations[i]->getDisk(); }

    Disks getDisks() const override;

    void update(UInt64 new_size) override;
private:
    Reservations reservations;
    UInt64 size;
};

}
