#pragma once

#include <memory>
#include <optional>
#include <queue>

#include <Disks/IVolume.h>
#include <base/defines.h>
#include <base/types.h>
#include <Common/Stopwatch.h>


namespace DB
{

class VolumeJBOD;

using VolumeJBODPtr = std::shared_ptr<VolumeJBOD>;
using VolumesJBOD = std::vector<VolumeJBODPtr>;

/**
 * Implements something similar to JBOD (https://en.wikipedia.org/wiki/Non-RAID_drive_architectures#JBOD).
 * When MergeTree engine wants to write part — it requests VolumeJBOD to reserve space on the next available
 * disk and then writes new part to that disk.
 */
class VolumeJBOD : public IVolume
{
public:
    VolumeJBOD(String name_, Disks disks_, UInt64 max_data_part_size_, bool are_merges_avoided_, bool perform_ttl_move_on_insert_, VolumeLoadBalancing load_balancing_, UInt64 least_used_ttl_ms_)
        : IVolume(name_, disks_, max_data_part_size_, perform_ttl_move_on_insert_, load_balancing_)
        , are_merges_avoided(are_merges_avoided_)
        , least_used_ttl_ms(least_used_ttl_ms_)
    {
    }

    VolumeJBOD(
        String name_,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        DiskSelectorPtr disk_selector
    );

    VolumeJBOD(
        const VolumeJBOD & volume_jbod,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        DiskSelectorPtr disk_selector
    );

    VolumeType getType() const override { return VolumeType::JBOD; }

    /// Returns disk based on the load balancing algorithm (round-robin, or least-used),
    /// ignores @index argument.
    ///
    /// - Used with policy for temporary data
    /// - Ignores all limitations
    /// - Shares last access with reserve()
    DiskPtr getDisk(size_t index) const override;

    /// Uses Round-robin to choose disk for reservation.
    /// Returns valid reservation or nullptr if there is no space left on any disk.
    ReservationPtr reserve(UInt64 bytes) override;

    bool areMergesAvoided() const override;

    void setAvoidMergesUserOverride(bool avoid) override;

    /// True if parts on this volume participate in merges according to configuration.
    bool are_merges_avoided = true;

private:
    struct DiskWithSize
    {
        DiskPtr disk;
        std::optional<UInt64> free_size = 0;

        explicit DiskWithSize(DiskPtr disk_)
            : disk(disk_)
            , free_size(disk->getUnreservedSpace())
        {}

        bool operator<(const DiskWithSize & rhs) const
        {
            return free_size < rhs.free_size;
        }

        ReservationPtr reserve(UInt64 bytes)
        {
            ReservationPtr reservation = disk->reserve(bytes);
            if (!reservation)
                return {};

            /// Not just subtract bytes, but update the value,
            /// since some reservations may be done directly via IDisk, or not by ClickHouse.
            free_size = reservation->getUnreservedSpace();
            return reservation;
        }
    };

    mutable std::mutex mutex;
    /// Index of last used disk, for load_balancing=round_robin
    mutable std::atomic<size_t> last_used = 0;
    /// Priority queue of disks sorted by size, for load_balancing=least_used
    using LeastUsedDisksQueue = std::priority_queue<DiskWithSize>;
    mutable LeastUsedDisksQueue disks_by_size TSA_GUARDED_BY(mutex);
    mutable Stopwatch least_used_update_watch TSA_GUARDED_BY(mutex);
    UInt64 least_used_ttl_ms = 0;

    /// True if parts on this volume participate in merges according to START/STOP MERGES ON VOLUME.
    std::atomic<std::optional<bool>> are_merges_avoided_user_override{std::nullopt};
};

}
