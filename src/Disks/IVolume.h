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
    IVolume(String name_, Disks disks_, size_t max_data_part_size_ = 0, bool perform_ttl_move_on_insert_ = true)
        : disks(std::move(disks_))
        , name(name_)
        , max_data_part_size(max_data_part_size_)
        , perform_ttl_move_on_insert(perform_ttl_move_on_insert_)
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
    Disks & getDisks() { return disks; }
    const Disks & getDisks() const { return disks; }

    /// Returns effective value of whether merges are allowed on this volume (true) or not (false).
    virtual bool areMergesAvoided() const { return false; }

    /// User setting for enabling and disabling merges on volume.
    virtual void setAvoidMergesUserOverride(bool /*avoid*/) {}

protected:
    Disks disks;
    const String name;

public:
    /// Max size of reservation, zero means unlimited size
    UInt64 max_data_part_size = 0;
    /// Should a new data part be synchronously moved to a volume according to ttl on insert
    /// or move this part in background task asynchronously after insert.
    bool perform_ttl_move_on_insert = true;
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
