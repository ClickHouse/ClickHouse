#pragma once

#include <mutex>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#if defined(__linux__)
#include <cstdio>
#include <mntent.h>
#endif
#include <memory>
#include <filesystem>
#include <boost/noncopyable.hpp>
#include <Poco/Util/AbstractConfiguration.h>
#include <common/logger_useful.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>
#include <Common/formatReadable.h>
#include <Common/CurrentMetrics.h>


namespace CurrentMetrics
{
    extern const Metric DiskSpaceReservedForMerge;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_STATVFS;
    extern const int NOT_ENOUGH_SPACE;
    extern const int SYSTEM_ERROR;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int UNKNOWN_POLICY;
    extern const int UNKNOWN_DISK;
}

namespace DiskSpace
{

static constexpr UInt64 UNLIMITED_PARTITION_SIZE = std::numeric_limits<UInt64>::max();

class Reservation;
using ReservationPtr = std::unique_ptr<Reservation>;

/// Returns mount point of filesystem where absoulte_path (must exist) is located
std::filesystem::path getMountPoint(std::filesystem::path absolute_path);

/// Returns name of filesystem mounted to mount_point
#if !defined(__linux__)
[[noreturn]]
#endif
std::string getFilesystemName([[maybe_unused]] const std::string & mount_point);

inline struct statvfs getStatVFS(const std::string & path)
{
    struct statvfs fs;
    if (statvfs(path.c_str(), &fs) != 0)
        throwFromErrnoWithPath(
            "Could not calculate available disk space (statvfs)", path, ErrorCodes::CANNOT_STATVFS);
    return fs;
}

/**
 *  Provide interface for reservation
 */
class Space : public std::enable_shared_from_this<Space>
{
public:
    virtual ReservationPtr reserve(UInt64 bytes) const = 0;

    virtual const String & getName() const = 0;

    virtual ~Space() = default;
};

using SpacePtr = std::shared_ptr<const Space>;


/** Disk - Smallest space unit.
 *  path - Path to space. Ends with /
 *  name - Unique key using for disk space reservation.
 */
class Disk : public Space
{
public:
    friend class Reservation;

    class Stat
    {
        struct statvfs fs{};
        UInt64 keep_free_space_bytes;

    public:
        explicit Stat(const Disk & disk)
        {
            if (statvfs(disk.path.c_str(), &fs) != 0)
                throwFromErrno("Could not calculate available disk space (statvfs)", ErrorCodes::CANNOT_STATVFS);
            keep_free_space_bytes = disk.keep_free_space_bytes;
        }

        UInt64 getTotalSpace() const;

        UInt64 getAvailableSpace() const;
    };

    Disk(String name_, String path_, UInt64 keep_free_space_bytes_)
        : name(std::move(name_)),
          path(std::move(path_)),
          keep_free_space_bytes(keep_free_space_bytes_)
    {
        if (path.back() != '/')
            throw Exception("Disk path must ends with '/', but '" + path + "' doesn't.", ErrorCodes::LOGICAL_ERROR);
    }

    ReservationPtr reserve(UInt64 bytes) const override;

    bool tryReserve(UInt64 bytes) const;

    const String & getName() const override { return name; }

    const String & getPath() const { return path; }

    UInt64 getKeepingFreeSpace() const { return keep_free_space_bytes; }

    Stat getSpaceInformation() const { return Stat(*this); }

    UInt64 getTotalSpace() const { return getSpaceInformation().getTotalSpace(); }

    UInt64 getAvailableSpace() const { return getSpaceInformation().getAvailableSpace(); }

    UInt64 getUnreservedSpace() const;

private:
    String name;
    String path;
    UInt64 keep_free_space_bytes;

    /// Real reservation data
    static std::mutex mutex;
    mutable UInt64 reserved_bytes = 0;
    mutable UInt64 reservation_count = 0;
};

/// It is not possible to change disk runtime.
using DiskPtr = std::shared_ptr<const Disk>;
using Disks = std::vector<DiskPtr>;


/**
 * Information about reserved size on concrete disk.
 * Unreserve on destroy.
 */
class Reservation final : private boost::noncopyable
{
public:
    Reservation(UInt64 size_, DiskPtr disk_ptr_)
        : size(size_)
        , metric_increment(CurrentMetrics::DiskSpaceReservedForMerge, size)
        , disk_ptr(disk_ptr_)
    {
    }

    ~Reservation();

    /// Change amount of reserved space. When new_size is greater than before,
    /// availability of free space is not checked.
    void update(UInt64 new_size);

    UInt64 getSize() const
    {
        return size;
    }

    const DiskPtr & getDisk() const
    {
        return disk_ptr;
    }

private:
    UInt64 size;
    CurrentMetrics::Increment metric_increment;
    DiskPtr disk_ptr;
};

/// Parse .xml configuration and store information about disks
/// Mostly used for introspection
class DiskSelector
{
public:
    DiskSelector(const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix, String default_path);

    const DiskPtr & operator[](const String & name) const;

    bool hasDisk(const String & name) const;

    void add(const DiskPtr & disk);

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
        : max_data_part_size(max_data_part_size_)
        , disks(std::move(disks_))
        , name(std::move(name_))
    {
    }

    Volume(String name_, const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix, const DiskSelector & disk_selector);

    /// Uses Round-robin to choose disk for reservation.
    /// Returns valid reservation or null.
    ReservationPtr reserve(UInt64 bytes) const override;

    /// Return biggest unreserved space across all disks
    UInt64 getMaxUnreservedFreeSpace() const;

    const String & getName() const override { return name; }

    UInt64 max_data_part_size = UNLIMITED_PARTITION_SIZE;

    Disks disks;

private:
    mutable std::atomic<size_t> last_used = 0;
    String name;
};

using VolumePtr = std::shared_ptr<const Volume>;
using Volumes = std::vector<VolumePtr>;


/**
 * Contains all information about volumes configuration for Storage.
 * Can determine appropriate Volume and Disk for each reservation.
 */
class StoragePolicy : public Space
{
public:

    StoragePolicy(String name_, const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix, const DiskSelector & disks);

    StoragePolicy(String name_, Volumes volumes_, double move_factor_);

    /// Returns disks ordered by volumes priority
    Disks getDisks() const;

    DiskPtr getAnyDisk() const;

    DiskPtr getDiskByName(const String & disk_name) const;

    UInt64 getMaxUnreservedFreeSpace() const;

    const String & getName() const override { return name; }

    /// Returns valid reservation or null
    ReservationPtr reserve(UInt64 bytes) const override;

    /// Reserve space on any volume with priority > min_volume_index
    ReservationPtr reserve(UInt64 bytes, size_t min_volume_index) const;

    size_t getVolumePriorityByDisk(const DiskPtr & disk_ptr) const;

    /// Reserves 0 bytes on disk with max available space
    /// Do not use this function when it is possible to predict size!!!
    ReservationPtr reserveOnMaxDiskWithoutReservation() const;

    const Volumes & getVolumes() const { return volumes; }

    double getMoveFactor() const { return move_factor; }

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
    String name;
    std::map<String, size_t> volumes_names;

    /// move_factor from interval [0., 1.]
    /// We move something if disk from this policy
    /// filled more than total_size * move_factor
    double move_factor;
};




using StoragePolicyPtr = std::shared_ptr<const StoragePolicy>;

/// Parse .xml configuration and store information about policies
/// Mostly used for introspection
class StoragePolicySelector
{
public:
    StoragePolicySelector(const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix, const DiskSelector & disks);

    const StoragePolicyPtr & operator[](const String & name) const;

    const std::map<String, StoragePolicyPtr> & getPoliciesMap() const { return policies; }

private:
    std::map<String, StoragePolicyPtr> policies;
};

}

}
