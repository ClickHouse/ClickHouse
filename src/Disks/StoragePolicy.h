#pragma once

#include <Disks/IStoragePolicy.h>
#include <Disks/IDisk.h>

#include <memory>
#include <unordered_map>

namespace Poco::Util
{
    class AbstractConfiguration;
};


namespace DB
{
class DiskSelector;
using DiskSelectorPtr = std::shared_ptr<const DiskSelector>;

/**
 * Contains all information about volumes configuration for Storage.
 * Can determine appropriate Volume and Disk for each reservation.
 */
class StoragePolicy : public IStoragePolicy
{
public:
    StoragePolicy(String name_, const Poco::Util::AbstractConfiguration & config, const String & config_prefix, DiskSelectorPtr disks);

    StoragePolicy(String name_, Volumes volumes_, double move_factor_);

    StoragePolicy(
        StoragePolicyPtr storage_policy,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        DiskSelectorPtr disks
    );

    bool isDefaultPolicy() const override;

    /// Returns disks ordered by volumes priority
    Disks getDisks() const override;

    /// Returns any disk
    /// Used when it's not important, for example for
    /// mutations files
    DiskPtr getAnyDisk() const override;

    DiskPtr tryGetDiskByName(const String & disk_name) const override;

    /// Get free space from most free disk
    UInt64 getMaxUnreservedFreeSpace() const override;

    const String & getName() const override{ return name; }

    /// Returns valid reservation or nullptr
    ReservationPtr reserve(UInt64 bytes) const override;

    /// Reserves space on any volume or throws
    ReservationPtr reserveAndCheck(UInt64 bytes) const override;

    /// Reserves space on any volume with index > min_volume_index or returns nullptr
    ReservationPtr reserve(UInt64 bytes, size_t min_volume_index) const override;

    /// Find volume index, which contains disk
    std::optional<size_t> tryGetVolumeIndexByDiskName(const String & disk_name) const override;

    /// Reserves 0 bytes on disk with max available space
    /// Do not use this function when it is possible to predict size.
    ReservationPtr makeEmptyReservationOnLargestDisk() const override;

    const Volumes & getVolumes() const  override{ return volumes; }

    /// Returns number [0., 1.] -- fraction of free space on disk
    /// which should be kept with help of background moves
    double getMoveFactor() const  override{ return move_factor; }

    /// Get volume by index.
    VolumePtr getVolume(size_t index) const override;

    VolumePtr tryGetVolumeByName(const String & volume_name) const override;

    /// Checks if storage policy can be replaced by another one.
    void checkCompatibleWith(const StoragePolicyPtr & new_storage_policy) const override;

    /// Check if we have any volume with stopped merges
    bool hasAnyVolumeWithDisabledMerges() const override;

    bool containsVolume(const String & volume_name) const override;

private:
    Volumes volumes;
    const String name;
    std::unordered_map<String, size_t> volume_index_by_volume_name;
    std::unordered_map<String, size_t> volume_index_by_disk_name;

    /// move_factor from interval [0., 1.]
    /// We move something if disk from this policy
    /// filled more than total_size * move_factor
    double move_factor = 0.1; /// by default move factor is 10%

    void buildVolumeIndices();

    LoggerPtr log;
};


class StoragePolicySelector;
using StoragePolicySelectorPtr = std::shared_ptr<const StoragePolicySelector>;
using StoragePoliciesMap = std::map<String, StoragePolicyPtr>;

/// Parse .xml configuration and store information about policies
/// Mostly used for introspection.
class StoragePolicySelector
{
public:
    static constexpr auto TMP_STORAGE_POLICY_PREFIX = "__";

    StoragePolicySelector(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, DiskSelectorPtr disks);

    StoragePolicySelectorPtr updateFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, DiskSelectorPtr disks, Strings & new_disks) const;

    /// Policy by name
    StoragePolicyPtr get(const String & name) const;

    StoragePolicyPtr tryGet(const String & name) const;

    /// All policies
    const StoragePoliciesMap & getPoliciesMap() const { return policies; }

    /// Add storage policy to StoragePolicySelector.
    /// Used when storage policy needs to be created on the fly, not being present in config file.
    /// Done by getOrSetStoragePolicyForSingleDisk.
    void add(StoragePolicyPtr storage_policy);

private:
    StoragePoliciesMap policies;
};

}
