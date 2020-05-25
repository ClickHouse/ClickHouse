#pragma once

#include <Disks/DiskSelector.h>
#include <Disks/IDisk.h>
#include <Disks/IVolume.h>
#include <Disks/VolumeJBOD.h>
#include <Disks/SingleDiskVolume.h>
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

class StoragePolicy;
using StoragePolicyPtr = std::shared_ptr<const StoragePolicy>;

/**
 * Contains all information about volumes configuration for Storage.
 * Can determine appropriate Volume and Disk for each reservation.
 */
class StoragePolicy
{
public:
    StoragePolicy(String name_, const Poco::Util::AbstractConfiguration & config, const String & config_prefix, DiskSelectorPtr disks);

    StoragePolicy(String name_, VolumesJBOD volumes_, double move_factor_);

    bool isDefaultPolicy() const;

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

    const VolumesJBOD & getVolumes() const { return volumes; }

    /// Returns number [0., 1.] -- fraction of free space on disk
    /// which should be kept with help of background moves
    double getMoveFactor() const { return move_factor; }

    /// Get volume by index from storage_policy
    VolumeJBODPtr getVolume(size_t i) const { return (i < volumes_names.size() ? volumes[i] : VolumeJBODPtr()); }

    VolumeJBODPtr getVolumeByName(const String & volume_name) const
    {
        auto it = volumes_names.find(volume_name);
        if (it == volumes_names.end())
            return {};
        return getVolume(it->second);
    }

    /// Checks if storage policy can be replaced by another one.
    void checkCompatibleWith(const StoragePolicyPtr & new_storage_policy) const;

private:
    VolumesJBOD volumes;
    const String name;
    std::map<String, size_t> volumes_names;

    /// move_factor from interval [0., 1.]
    /// We move something if disk from this policy
    /// filled more than total_size * move_factor
    double move_factor = 0.1; /// by default move factor is 10%
};


class StoragePolicySelector;
using StoragePolicySelectorPtr = std::shared_ptr<const StoragePolicySelector>;
using StoragePoliciesMap = std::map<String, StoragePolicyPtr>;

/// Parse .xml configuration and store information about policies
/// Mostly used for introspection.
class StoragePolicySelector
{
public:
    StoragePolicySelector(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, DiskSelectorPtr disks);

    StoragePolicySelectorPtr updateFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, DiskSelectorPtr disks) const;

    /// Policy by name
    StoragePolicyPtr get(const String & name) const;

    /// All policies
    const StoragePoliciesMap & getPoliciesMap() const { return policies; }

private:
    StoragePoliciesMap policies;
};

}
