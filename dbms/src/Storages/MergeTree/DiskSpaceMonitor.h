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


class Reservation;
using ReservationPtr = std::unique_ptr<Reservation>;


/** Space.
 *  Provide interface for reservation
 */
class Space : public std::enable_shared_from_this<Space>
{
public:
    Space() = default;

    virtual ~Space() = default;
    Space(const Space &) = default;
    Space(Space &&) = default;
    Space& operator=(const Space &) = default;
    Space& operator=(Space &&) = default;

    virtual ReservationPtr reserve(UInt64 bytes) const = 0;

    virtual const String & getName() const = 0;
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
        struct statvfs fs = {};
        UInt64 keep_free_space_bytes;

    public:
        explicit Stat(const Disk & disk)
        {
            if (statvfs(disk.path.c_str(), &fs) != 0)
                throwFromErrno("Could not calculate available disk space (statvfs)", ErrorCodes::CANNOT_STATVFS);
            keep_free_space_bytes = disk.keep_free_space_bytes;
        }

        UInt64 getTotalSpace()
        {
            UInt64 size = fs.f_blocks * fs.f_bsize;
            if (size < keep_free_space_bytes)
                return 0;
            return size - keep_free_space_bytes;
        }

        UInt64 getAvailableSpace()
        {
            UInt64 size = fs.f_bfree * fs.f_bsize;
            if (size < keep_free_space_bytes)
                return 0;
            return size - keep_free_space_bytes;
        }
    };

    Disk(String name_, String path_, UInt64 keep_free_space_bytes_)
        : name(std::move(name_)),
          path(std::move(path_)),
          keep_free_space_bytes(keep_free_space_bytes_)
    {
    }

    ~Disk() override = default;

    ReservationPtr reserve(UInt64 bytes) const override;

    bool try_reserve(UInt64 bytes) const
    {
        auto available_space = getAvailableSpace();
        std::lock_guard lock(mutex);
        if (bytes == 0)
        {
            LOG_DEBUG(&Logger::get("DiskSpaceMonitor"), "Reserve 0 bytes on disk " << name);
            ++reservation_count;
            return true;
        }
        available_space -= std::min(available_space, reserved_bytes);
        LOG_DEBUG(&Logger::get("DiskSpaceMonitor"), "Unreserved " << available_space << " , to reserve " << bytes << " on disk " << name);
        if (available_space >= bytes)
        {
            ++reservation_count;
            reserved_bytes += bytes;
            return true;
        }
        return false;
    }

    const String & getName() const override
    {
        return name;
    }

    const String & getPath() const
    {
        return path;
    }

    inline static struct statvfs getStatVFS(const std::string & path)
    {
        struct statvfs fs;
        if (statvfs(path.c_str(), &fs) != 0)
            throwFromErrnoWithPath("Could not calculate available disk space (statvfs)", path,
                                   ErrorCodes::CANNOT_STATVFS);
        return fs;
    }

    UInt64 getKeepingFreeSpace() const
    {
        return keep_free_space_bytes;
    }

    auto getSpaceInformation() const
    {
        return Stat(*this);
    }

    UInt64 getTotalSpace() const
    {
        return getSpaceInformation().getTotalSpace();
    }

    UInt64 getAvailableSpace() const
    {
        return getSpaceInformation().getAvailableSpace();
    }

    UInt64 getUnreservedSpace() const
    {
        auto available_space = getSpaceInformation().getAvailableSpace();
        std::lock_guard lock(mutex);
        available_space -= std::min(available_space, reserved_bytes);
        return available_space;
    }

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


/** Reservationcontain
 *  Contain information about disk and size of reservation
 *  Unreserve on destroy
 */
class Reservation : private boost::noncopyable
{
public:
    Reservation(UInt64 size_, DiskPtr disk_ptr_)
        : size(size_), metric_increment(CurrentMetrics::DiskSpaceReservedForMerge, size), disk_ptr(std::move(disk_ptr_)), valid(disk_ptr->try_reserve(size))
    {
    }

    ~Reservation()
    {
        try
        {
            std::lock_guard lock(Disk::mutex);
            if (disk_ptr->reserved_bytes < size)
            {
                disk_ptr->reserved_bytes = 0;
                LOG_ERROR(&Logger::get("DiskSpaceMonitor"), "Unbalanced reservations size; it's a bug");
            }
            else
            {
                disk_ptr->reserved_bytes -= size;
            }

            if (disk_ptr->reservation_count == 0)
            {
                LOG_ERROR(&Logger::get("DiskSpaceMonitor"), "Unbalanced reservation count; it's a bug");
            }
            else
            {
                --disk_ptr->reservation_count;
            }
        }
        catch (...)
        {
            tryLogCurrentException("~DiskSpaceMonitor");
        }
    }

    /// Change amount of reserved space. When new_size is greater than before, availability of free space is not checked.
    void update(UInt64 new_size)
    {
        std::lock_guard lock(Disk::mutex);
        disk_ptr->reserved_bytes -= size;
        size = new_size;
        disk_ptr->reserved_bytes += size;
    }

    UInt64 getSize() const
    {
        return size;
    }

    /// Returns mount point of filesystem where absoulte_path (must exist) is located
    static std::filesystem::path getMountPoint(std::filesystem::path absolute_path)
    {
        if (absolute_path.is_relative())
            throw Exception("Path is relative. It's a bug.", ErrorCodes::LOGICAL_ERROR);

        absolute_path = std::filesystem::canonical(absolute_path);

        const auto get_device_id = [](const std::filesystem::path & p)
        {
            struct stat st;
            if (stat(p.c_str(), &st))
                throwFromErrnoWithPath("Cannot stat " + p.string(), p.string(), ErrorCodes::SYSTEM_ERROR);
            return st.st_dev;
        };

        /// If /some/path/to/dir/ and /some/path/to/ have different device id,
        /// then device which contains /some/path/to/dir/filename is mounted to /some/path/to/dir/
        auto device_id = get_device_id(absolute_path);
        while (absolute_path.has_relative_path())
        {
            auto parent = absolute_path.parent_path();
            auto parent_device_id = get_device_id(parent);
            if (device_id != parent_device_id)
                return absolute_path;
            absolute_path = parent;
            device_id = parent_device_id;
        }

        return absolute_path;
    }

    /// Returns name of filesystem mounted to mount_point
#if !defined(__linux__)
[[noreturn]]
#endif
    static std::string getFilesystemName([[maybe_unused]] const std::string & mount_point)
    {
#if defined(__linux__)
        auto mounted_filesystems = setmntent("/etc/mtab", "r");
        if (!mounted_filesystems)
            throw DB::Exception("Cannot open /etc/mtab to get name of filesystem", ErrorCodes::SYSTEM_ERROR);
        mntent fs_info;
        constexpr size_t buf_size = 4096;     /// The same as buffer used for getmntent in glibc. It can happen that it's not enough
        char buf[buf_size];
        while (getmntent_r(mounted_filesystems, &fs_info, buf, buf_size) && fs_info.mnt_dir != mount_point)
            ;
        endmntent(mounted_filesystems);
        if (fs_info.mnt_dir != mount_point)
            throw DB::Exception("Cannot find name of filesystem by mount point " + mount_point, ErrorCodes::SYSTEM_ERROR);
        return fs_info.mnt_fsname;
#else
        throw DB::Exception("Supported on linux only", ErrorCodes::NOT_IMPLEMENTED);
#endif
    }

    const DiskPtr & getDisk() const
    {
        return disk_ptr;
    }

    bool isValid() const { return valid; }

private:
    UInt64 size;
    CurrentMetrics::Increment metric_increment;
    DiskPtr disk_ptr;
    bool valid = false;
};


class DiskSelector
{
public:
    DiskSelector(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, String default_path);

    const DiskPtr & operator[](const String & name) const;

    bool has(const String & name) const;

    void add(const DiskPtr & disk);

    const auto & getDisksMap() const { return disks; }

private:
    std::map<String, DiskPtr> disks;
};


/** Volume.
 *  Contain set of "equivalent" disks
 */
class Volume : public Space
{
    friend class StoragePolicy;

public:
    Volume(String name_, std::vector<DiskPtr> disks_, UInt64 max_data_part_size_)
            : max_data_part_size(max_data_part_size_), disks(std::move(disks_)), name(std::move(name_)) { }

    Volume(String name_, const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, const DiskSelector & disk_selector);

    Volume(const Volume & other) : Space(other), max_data_part_size(other.max_data_part_size), disks(other.disks), name(other.name) { }

    Volume & operator=(const Volume & other)
    {
        disks = other.disks;
        max_data_part_size = other.max_data_part_size;
        last_used.store(0, std::memory_order_relaxed);
        name = other.name;
        return *this;
    }

    Volume(Volume && other) noexcept
            : max_data_part_size(other.max_data_part_size), disks(std::move(other.disks)), name(std::move(other.name)) { }

    Volume & operator=(Volume && other) noexcept
    {
        disks = std::move(other.disks);
        max_data_part_size = other.max_data_part_size;
        last_used.store(0, std::memory_order_relaxed);
        name = std::move(other.name);
        return *this;
    }

    /// Returns valid reservation or null
    ReservationPtr reserve(UInt64 bytes) const override;

    UInt64 getMaxUnreservedFreeSpace() const;

    const String & getName() const override { return name; }

    UInt64 max_data_part_size = std::numeric_limits<decltype(max_data_part_size)>::max();

    Disks disks;

private:
    mutable std::atomic<size_t> last_used = 0;
    String name;
};

using VolumePtr = std::shared_ptr<const Volume>;
using Volumes = std::vector<VolumePtr>;


/** Policy.
 *  Contain ordered set of Volumes
 */
class StoragePolicy : public Space
{
public:

    StoragePolicy(String name_, const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
           const DiskSelector & disks);

    StoragePolicy(String name_, Volumes volumes_, double move_factor_) : volumes(std::move(volumes_)), name(std::move(name_)), move_factor(move_factor_)
    {
        if (volumes.empty())
            throw Exception("StoragePolicy must contain at least one Volume", ErrorCodes::UNKNOWN_POLICY);

        for (size_t i = 0; i != volumes.size(); ++i)
        {
            if (volumes_names.find(volumes[i]->getName()) != volumes_names.end())
                throw Exception("Volumes names must be unique (" + volumes[i]->getName() + " duplicated)", ErrorCodes::UNKNOWN_POLICY);
            volumes_names[volumes[i]->getName()] = i;
        }
    }

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

    auto getVolumePriorityByDisk(const DiskPtr & disk_ptr) const
    {
        for (size_t i = 0; i != volumes.size(); ++i)
        {
            const auto & volume = volumes[i];
            for (auto && disk : volume->disks)
            {
                if (disk->getName() == disk_ptr->getName())
                    return i;
            }
        }
        throw Exception("No disk " + disk_ptr->getName() + " in Policy " + name, ErrorCodes::UNKNOWN_DISK);
    }

    /// Reserves 0 bytes on disk with max available space
    /// Do not use this function when it is possible to predict size!!!
    ReservationPtr reserveOnMaxDiskWithoutReservation() const;

    const auto & getVolumes() const { return volumes; }

    auto getMoveFactor() const { return move_factor; }

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
    double move_factor;
};

using StoragePolicyPtr = std::shared_ptr<const StoragePolicy>;

class StoragePolicySelector
{
public:
    StoragePolicySelector(const Poco::Util::AbstractConfiguration & config, const String& config_prefix, const DiskSelector & disks);

    const StoragePolicyPtr & operator[](const String & name) const;

    const auto & getPoliciesMap() const { return policies; }

private:
    std::map<String, StoragePolicyPtr> policies;
};

}

}
