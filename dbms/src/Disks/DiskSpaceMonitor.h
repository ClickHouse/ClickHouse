#pragma once

#include <Disks/IDisk.h>
#include <IO/WriteHelpers.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>
#include <common/logger_useful.h>

#include <memory>
#include <mutex>
#include <unistd.h>
#include <boost/noncopyable.hpp>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_ENOUGH_SPACE;
    extern const int NOT_IMPLEMENTED;
    extern const int SYSTEM_ERROR;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int UNKNOWN_POLICY;
    extern const int UNKNOWN_DISK;
}

/// Parse .xml configuration and store information about disks
/// Mostly used for introspection.
class DiskSelector
{
public:
    DiskSelector(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, const Context & context);

    /// Get disk by name
    const DiskPtr & operator[](const String & name) const;

    /// Get all disks with names
    const auto & getDisksMap() const { return disks; }

private:
    std::map<String, DiskPtr> disks;
};

/**
 * Disks group by some (user) criteria. For example,
 * - Volume("slow_disks", [d1, d2], 100)
 * - Volume("fast_disks", [d3, d4], 200)
 * Cannot store parts larger than max_data_part_size.
 */
class Volume : public Space
{
    friend class StoragePolicy;

public:
    Volume(String name_, std::vector<DiskPtr> disks_, UInt64 max_data_part_size_)
        : max_data_part_size(max_data_part_size_), disks(std::move(disks_)), name(std::move(name_))
    {
    }

    Volume(
        String name_,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        const DiskSelector & disk_selector);

    /// Uses Round-robin to choose disk for reservation.
    /// Returns valid reservation or nullptr if there is no space left on any disk.
    ReservationPtr reserve(UInt64 bytes) override;

    /// Return biggest unreserved space across all disks
    UInt64 getMaxUnreservedFreeSpace() const;

    /// Volume name from config
    const String & getName() const override { return name; }

    /// Max size of reservation
    UInt64 max_data_part_size = 0;

    /// Disks in volume
    Disks disks;

private:
    mutable std::atomic<size_t> last_used = 0;
    const String name;
};

using VolumePtr = std::shared_ptr<Volume>;
using Volumes = std::vector<VolumePtr>;


/**
 * Contains all information about volumes configuration for Storage.
 * Can determine appropriate Volume and Disk for each reservation.
 */
class StoragePolicy
{
public:
    StoragePolicy(String name_, const Poco::Util::AbstractConfiguration & config, const String & config_prefix, const DiskSelector & disks);

    StoragePolicy(String name_, Volumes volumes_, double move_factor_);

    /// Returns disks ordered by volumes priority
    Disks getDisks() const;

    /// Returns any disk
    /// Used when it's not important, for example for
    /// mutations files
    DiskPtr getAnyDisk() const;

    DiskPtr getDiskByName(const String & disk_name) const;

    /// Get free space from most free disk
    UInt64 getMaxUnreservedFreeSpace() const;

    const String & getName() const { return name; }

    /// Returns valid reservation or null
    ReservationPtr reserve(UInt64 bytes) const;

    /// Reserve space on any volume with index > min_volume_index
    ReservationPtr reserve(UInt64 bytes, size_t min_volume_index) const;

    /// Find volume index, which contains disk
    size_t getVolumeIndexByDisk(const DiskPtr & disk_ptr) const;

    /// Reserves 0 bytes on disk with max available space
    /// Do not use this function when it is possible to predict size.
    ReservationPtr makeEmptyReservationOnLargestDisk() const;

    const Volumes & getVolumes() const { return volumes; }

    /// Returns number [0., 1.] -- fraction of free space on disk
    /// which should be kept with help of background moves
    double getMoveFactor() const { return move_factor; }

    /// Get volume by index from storage_policy
    VolumePtr getVolume(size_t i) const { return (i < volumes_names.size() ? volumes[i] : VolumePtr()); }

    VolumePtr getVolumeByName(const String & volume_name) const
    {
        auto it = volumes_names.find(volume_name);
        if (it == volumes_names.end())
            return {};
        return getVolume(it->second);
    }

private:
    Volumes volumes;
    const String name;
    std::map<String, size_t> volumes_names;

    /// move_factor from interval [0., 1.]
    /// We move something if disk from this policy
    /// filled more than total_size * move_factor
    double move_factor = 0.1; /// by default move factor is 10%
};


using StoragePolicyPtr = std::shared_ptr<const StoragePolicy>;

/// Parse .xml configuration and store information about policies
/// Mostly used for introspection.
class StoragePolicySelector
{
public:
    StoragePolicySelector(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, const DiskSelector & disks);

    /// Policy by name
    const StoragePolicyPtr & operator[](const String & name) const;

    /// All policies
    const std::map<String, StoragePolicyPtr> & getPoliciesMap() const { return policies; }

private:
    std::map<String, StoragePolicyPtr> policies;
};

}
