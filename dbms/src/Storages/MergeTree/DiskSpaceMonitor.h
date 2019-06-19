#pragma once

#include <mutex>
#include <sys/statvfs.h>
#include <memory>
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
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int UNKNOWN_POLICY;
    extern const int UNKNOWN_DISK;
}

/** path - Contain path to data on disk.
  *        Ends with /
  * name - Unique key using for disk space reservation.
  */
class Disk
{
public:
    class SpaceInformation {
        struct statvfs fs;
        UInt64 keep_free_space_bytes;

    public:
        SpaceInformation(const Disk & disk) {
            if (statvfs(disk.path.c_str(), &fs) != 0)
                throwFromErrno("Could not calculate available disk space (statvfs)", ErrorCodes::CANNOT_STATVFS);
            keep_free_space_bytes = disk.keep_free_space_bytes;
        }

        UInt64 getTotalSpace() {
            UInt64 size = fs.f_blocks * fs.f_bsize;
            if (size < keep_free_space_bytes) {
                return 0;
            }
            return size - keep_free_space_bytes;
        }

        UInt64 getAvailableSpace() {
            UInt64 size = fs.f_bfree * fs.f_bsize;
            if (size < keep_free_space_bytes) {
                return 0;
            }
            return size - keep_free_space_bytes;
        }
    };


    Disk(String name_, String path_, UInt64 keep_free_space_bytes_)
        : name(std::move(name_)),
          path(std::move(path_)),
          keep_free_space_bytes(keep_free_space_bytes_)
    {
    }

    const String & getName() const
    {
        return name;
    }

    const String & getPath() const
    {
        return path;
    }

    UInt64 getKeepingFreeSpace() const
    {
        return keep_free_space_bytes;
    }

    auto getSpaceInformation() const {
        return SpaceInformation(*this);
    }

    UInt64 getTotalSpace() const
    {
        return getSpaceInformation().getTotalSpace();
    }

    UInt64 getAvailableSpace() const
    {
        return getSpaceInformation().getAvailableSpace();
    }

private:
    String name;
    String path;
    UInt64 keep_free_space_bytes;
};

/// It is not possible to change disk runtime.
using DiskPtr = std::shared_ptr<const Disk>;


/** Determines amount of free space in filesystem.
  * Could "reserve" space, for different operations to plan disk space usage.
  * Reservations are not separated for different filesystems,
  *  instead it is assumed, that all reservations are done within same filesystem.
  *
  *  It is necessary to set all paths in map before MergeTreeData starts
  */
class DiskSpaceMonitor
{
public:
    struct DiskReserve
    {
        UInt64 reserved_bytes;
        UInt64 reservation_count;
    };

    class Reservation : private boost::noncopyable
    {
    public:
        ~Reservation()
        {
            try
            {
                std::lock_guard lock(DiskSpaceMonitor::mutex);
                if (reserves->reserved_bytes < size)
                {
                    reserves->reserved_bytes = 0;
                    LOG_ERROR(&Logger::get("DiskSpaceMonitor"), "Unbalanced reservations size; it's a bug");
                }
                else
                {
                    reserves->reserved_bytes -= size;
                }

                if (reserves->reservation_count == 0)
                {
                    LOG_ERROR(&Logger::get("DiskSpaceMonitor"), "Unbalanced reservation count; it's a bug");
                }
                else
                {
                    --reserves->reservation_count;
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
            std::lock_guard lock(DiskSpaceMonitor::mutex);
            reserves->reserved_bytes -= size;
            size = new_size;
            reserves->reserved_bytes += size;
        }

        UInt64 getSize() const
        {
            return size;
        }

        const DiskPtr & getDisk() const
        {
            return disk_ptr;
        }

        Reservation(UInt64 size_, DiskPtr disk_ptr_)
            : size(size_), metric_increment(CurrentMetrics::DiskSpaceReservedForMerge, size), disk_ptr(std::move(disk_ptr_)) ///@TODO_IGR ASK DiskSpaceReservedForMerge?
        {
            /// Just make reservation if size is 0
            if (size == 0)
            {
                std::lock_guard lock(DiskSpaceMonitor::mutex);
                reserves = &DiskSpaceMonitor::reserved[disk_ptr->getName()];
                ++reserves->reservation_count;
                valid = true;
                return;
            }
            auto unreserved = disk_ptr->getAvailableSpace();

            LOG_DEBUG(&Logger::get("DiskSpaceMonitor"), "Unreserved " << unreserved << " , to reserve " << size);

            std::lock_guard lock(DiskSpaceMonitor::mutex);

            if (size > unreserved)
            {
                /// Can not reserve, not enough space
                ///@TODO_IGR ASK metric_increment?
                size = 0;
                return;
            }

            reserves = &DiskSpaceMonitor::reserved[disk_ptr->getName()];
            reserves->reserved_bytes += size;
            ++reserves->reservation_count;
            valid = true;
        }

        bool isValid() const { return valid; }

    private:
        UInt64 size;
        CurrentMetrics::Increment metric_increment;
        DiskReserve * reserves;
        DiskPtr disk_ptr;
        bool valid = false;
    };

    using ReservationPtr = std::unique_ptr<Reservation>;

    static UInt64 getUnreservedFreeSpace(const DiskPtr & disk_ptr)
    {
        UInt64 res = disk_ptr->getAvailableSpace();

        std::lock_guard lock(mutex);

        auto & reserved_bytes = reserved[disk_ptr->getName()].reserved_bytes;

        if (reserved_bytes > res)
            res = 0;
        else
            res -= reserved_bytes;

        return res;
    }

    static std::vector<UInt64> getAllReservedSpace()
    {
        std::lock_guard lock(mutex);
        std::vector<UInt64> res;
        for (const auto & reserve : reserved)
            res.push_back(reserve.second.reserved_bytes);
        return res;
    }

    static std::vector<UInt64> getAllReservationCount()
    {
        std::lock_guard lock(mutex);
        std::vector<UInt64> res;
        for (const auto & reserve : reserved)
            res.push_back(reserve.second.reservation_count);
        return res;
    }

    /// If not enough (approximately) space, do not reserve.
    /// If not, returns valid pointer
    ///@TODO_IGR ASK bla bla bla Reservation->operator bool()
    static ReservationPtr tryToReserve(const DiskPtr & disk_ptr, UInt64 size)
    {
        return std::make_unique<Reservation>(size, disk_ptr);
    }

private:
    static std::map<String, DiskReserve> reserved;
    static std::mutex mutex;
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


class StoragePolicy
{
public:
    using Disks = std::vector<DiskPtr>;

    class Volume
    {
        friend class StoragePolicy;

    public:
        Volume(std::vector<DiskPtr> disks_, UInt64 max_data_part_size_)
            : max_data_part_size(max_data_part_size_), disks(std::move(disks_)) { }

        Volume(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, const DiskSelector & disk_selector);

        Volume(const Volume & other) : max_data_part_size(other.max_data_part_size), disks(other.disks) { }

        Volume & operator=(const Volume & other)
        {
            disks = other.disks;
            max_data_part_size = other.max_data_part_size;
            last_used.store(0, std::memory_order_relaxed);
            return *this;
        }

        Volume(Volume && other) noexcept
            : max_data_part_size(other.max_data_part_size), disks(std::move(other.disks)) { }

        Volume & operator=(Volume && other) noexcept
        {
            disks = std::move(other.disks);
            max_data_part_size = other.max_data_part_size;
            last_used.store(0, std::memory_order_relaxed);
            return *this;
        }

        /// Returns valid reservation or null
        DiskSpaceMonitor::ReservationPtr reserve(UInt64 expected_size) const;

        /// Returns valid reservation or null
        DiskSpaceMonitor::ReservationPtr reserveAtDisk(const DiskPtr & disk, UInt64 expected_size) const;

        UInt64 getMaxUnreservedFreeSpace() const;

        UInt64 max_data_part_size = std::numeric_limits<decltype(max_data_part_size)>::max();

        Disks disks;

    private:
        mutable std::atomic<size_t> last_used = 0;
    };

    using Volumes = std::vector<Volume>;

    StoragePolicy(const String & name_, const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
           const DiskSelector & disks);

    StoragePolicy(const String & name_, Volumes volumes_) : volumes(std::move(volumes_)), name(name_) { }

    Disks getDisks() const;

    DiskPtr getAnyDisk() const;

    DiskPtr getDiskByName(const String & disk_name) const;

    UInt64 getMaxUnreservedFreeSpace() const;

    const String & getName() const { return name; }

    /// Reserve space on any volume with priority > min_volume_index
    /// Returns valid reservation or null
    DiskSpaceMonitor::ReservationPtr reserve(UInt64 expected_size, size_t min_volume_index=0) const;

    /// Returns valid reservation or null
    DiskSpaceMonitor::ReservationPtr reserveAtDisk(const DiskPtr & disk, UInt64 expected_size) const;

    auto getVolumePriorityByDisk(const DiskPtr & disk_ptr) const
    {
        for (size_t i = 0; i != volumes.size(); ++i)
        {
            const auto & volume = volumes[i];
            for (auto && disk : volume.disks)
            {
                if (disk->getName() == disk_ptr->getName())
                    return i;
            }
        }
        throw Exception("No disk " + disk_ptr->getName() + " in Policy " + name, ErrorCodes::UNKNOWN_DISK);
    }

    /// Reserves 0 bytes on disk with max available space
    /// Do not use this function when it is possible to predict size!!!
    DiskSpaceMonitor::ReservationPtr reserveOnMaxDiskWithoutReservation() const;

    const auto & getVolumes() const { return volumes; }

private:
    Volumes volumes;
    String name;
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
