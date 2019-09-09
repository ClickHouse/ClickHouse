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
}


/** Determines amount of free space in filesystem.
  * Could "reserve" space, for different operations to plan disk space usage.
  * Reservations are not separated for different filesystems,
  *  instead it is assumed, that all reservations are done within same filesystem.
  */
class DiskSpaceMonitor
{
public:
    class Reservation : private boost::noncopyable
    {
    public:
        ~Reservation()
        {
            try
            {
                std::lock_guard lock(DiskSpaceMonitor::mutex);
                if (DiskSpaceMonitor::reserved_bytes < size)
                {
                    DiskSpaceMonitor::reserved_bytes = 0;
                    LOG_ERROR(&Logger::get("DiskSpaceMonitor"), "Unbalanced reservations size; it's a bug");
                }
                else
                {
                    DiskSpaceMonitor::reserved_bytes -= size;
                }

                if (DiskSpaceMonitor::reservation_count == 0)
                {
                    LOG_ERROR(&Logger::get("DiskSpaceMonitor"), "Unbalanced reservation count; it's a bug");
                }
                else
                {
                    --DiskSpaceMonitor::reservation_count;
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
            DiskSpaceMonitor::reserved_bytes -= size;
            size = new_size;
            DiskSpaceMonitor::reserved_bytes += size;
        }

        UInt64 getSize() const
        {
            return size;
        }

        Reservation(UInt64 size_)
            : size(size_), metric_increment(CurrentMetrics::DiskSpaceReservedForMerge, size)
        {
            std::lock_guard lock(DiskSpaceMonitor::mutex);
            DiskSpaceMonitor::reserved_bytes += size;
            ++DiskSpaceMonitor::reservation_count;
        }

    private:
        UInt64 size;
        CurrentMetrics::Increment metric_increment;
    };

    using ReservationPtr = std::unique_ptr<Reservation>;

    inline static struct statvfs getStatVFS(const std::string & path)
    {
        struct statvfs fs;
        if (statvfs(path.c_str(), &fs) != 0)
            throwFromErrnoWithPath("Could not calculate available disk space (statvfs)", path,
                                   ErrorCodes::CANNOT_STATVFS);
        return fs;
    }

    static UInt64 getUnreservedFreeSpace(const std::string & path)
    {
        struct statvfs fs = getStatVFS(path);

        UInt64 res = fs.f_bfree * fs.f_bsize;

        /// Heuristic by Michael Kolupaev: reserve 30 MB more, because statvfs shows few megabytes more space than df.
        res -= std::min(res, static_cast<UInt64>(30 * (1ul << 20)));

        std::lock_guard lock(mutex);

        if (reserved_bytes > res)
            res = 0;
        else
            res -= reserved_bytes;

        return res;
    }

    static UInt64 getReservedSpace()
    {
        std::lock_guard lock(mutex);
        return reserved_bytes;
    }

    static UInt64 getReservationCount()
    {
        std::lock_guard lock(mutex);
        return reservation_count;
    }

    /// If not enough (approximately) space, throw an exception.
    static ReservationPtr reserve(const std::string & path, UInt64 size)
    {
        UInt64 free_bytes = getUnreservedFreeSpace(path);
        if (free_bytes < size)
            throw Exception("Not enough free disk space to reserve: " + formatReadableSizeWithBinarySuffix(free_bytes) + " available, "
                + formatReadableSizeWithBinarySuffix(size) + " requested", ErrorCodes::NOT_ENOUGH_SPACE);
        return std::make_unique<Reservation>(size);
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

private:
    static UInt64 reserved_bytes;
    static UInt64 reservation_count;
    static std::mutex mutex;
};

}
