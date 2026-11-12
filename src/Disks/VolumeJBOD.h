#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <vector>

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
        , disks_with_size(disks.begin(), disks.end())
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

    /// Chooses the disk for reservation by the load balancing algorithm (round-robin, or least-used).
    /// Returns valid reservation or nullptr if there is no space left on any disk.
    ReservationPtr reserve(UInt64 bytes) override;

    /// Reserve with constraints. Checks free space requirements before making reservation.
    /// Returns valid reservation or nullptr if constraints are not met on any disk.
    ReservationPtr reserve(UInt64 bytes, const ReservationConstraints & constraints) override;

    bool areMergesAvoided() const override;

    void setAvoidMergesUserOverride(bool avoid) override;

    /// True if parts on this volume participate in merges according to configuration.
    bool are_merges_avoided = true;

private:
    ReservationPtr reserveImpl(UInt64 bytes, const std::optional<ReservationConstraints> & constraints);

    struct DiskWithSize
    {
        DiskPtr disk;
        std::optional<UInt64> unreserved_space;

        explicit DiskWithSize(DiskPtr disk_)
            : disk(disk_)
            , unreserved_space(disk->getUnreservedSpace())
        {}

        /// Read-only disks compare below the others, so they are chosen last.
        bool operator<(const DiskWithSize & rhs) const
        {
            return std::pair(!disk->isReadOnly(), unreserved_space) < std::pair(!rhs.disk->isReadOnly(), rhs.unreserved_space);
        }
    };

    mutable std::mutex mutex;
    /// The next turn of load_balancing=round_robin: turn `t` goes to the disk `disks[t % disks.size()]`
    mutable std::atomic<size_t> last_used = 0;
    /// Disks with their unreserved space, in no particular order, for load_balancing=least_used
    mutable std::vector<DiskWithSize> disks_with_size TSA_GUARDED_BY(mutex);
    mutable Stopwatch least_used_update_watch TSA_GUARDED_BY(mutex);
    UInt64 least_used_ttl_ms = 0;

    /// Reads the unreserved space of every disk anew when `least_used_ttl_ms` has passed since the last read.
    void updateUnreservedSpaceIfExpired() const TSA_REQUIRES(mutex);

    /// True if parts on this volume participate in merges according to START/STOP MERGES ON VOLUME.
    std::atomic<std::optional<bool>> are_merges_avoided_user_override{std::nullopt};
};

}
