#pragma once

#include <mutex>
#include <sys/statvfs.h>
#include <memory>
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
}


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
    struct DiskReserve {
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

        const String & getPath() const {
            return path;
        }

        void addEnclosedDirToPath(const String & dir) {
            path += dir + '/';
        }

        Reservation(UInt64 size_, DiskReserve * reserves_, const String & path_)
            : size(size_), metric_increment(CurrentMetrics::DiskSpaceReservedForMerge, size), reserves(reserves_),
              path(path_)
        {
            std::lock_guard lock(DiskSpaceMonitor::mutex);
            reserves->reserved_bytes += size;
            ++reserves->reservation_count;
        }

    private:
        UInt64 size;
        CurrentMetrics::Increment metric_increment;
        DiskReserve * reserves;
        String path;
    };

    using ReservationPtr = std::unique_ptr<Reservation>;

    static UInt64 getUnreservedFreeSpace(const String & disk_path)
    {
        struct statvfs fs;

        if (statvfs(disk_path.c_str(), &fs) != 0)
            throwFromErrno("Could not calculate available disk space (statvfs)", ErrorCodes::CANNOT_STATVFS);

        UInt64 res = fs.f_bfree * fs.f_bsize;

        /// Heuristic by Michael Kolupaev: reserve 30 MB more, because statvfs shows few megabytes more space than df.
        res -= std::min(res, static_cast<UInt64>(30 * (1ul << 20)));

        std::lock_guard lock(mutex);

        auto & reserved_bytes = reserved[disk_path].reserved_bytes;

        if (reserved_bytes > res)
            res = 0;
        else
            res -= reserved_bytes;

        return res;
    }

    /** Returns max of unreserved free space on all disks
      * It is necessary to have guarantee that all paths are set
      */
    static UInt64 getMaxUnreservedFreeSpace()
    {
        UInt64 max_unreserved = 0;
        for (auto& [disk_path, reserve] : reserved) {
            struct statvfs fs;

            if (statvfs(disk_path.c_str(), &fs) != 0)
                throwFromErrno("Could not calculate available disk space (statvfs)", ErrorCodes::CANNOT_STATVFS);

            UInt64 res = fs.f_bfree * fs.f_bsize;

            /// Heuristic by Michael Kolupaev: reserve 30 MB more, because statvfs shows few megabytes more space than df.
            res -= std::min(res, static_cast<UInt64>(30 * (1ul << 20)));

            ///@TODO_IGR ASK Maybe mutex out of for
            std::lock_guard lock(mutex);

            auto &reserved_bytes = reserved[disk_path].reserved_bytes;

            if (reserved_bytes > res)
                res = 0;
            else
                res -= reserved_bytes;

            max_unreserved = std::max(max_unreserved, res);
        }
        return max_unreserved;
    }

    static UInt64 getReservedSpace(const String & disk_path)
    {
        std::lock_guard lock(mutex);
        return reserved[disk_path].reserved_bytes;
    }

    static UInt64 getReservationCount(const String & disk_path)
    {
        std::lock_guard lock(mutex);
        return reserved[disk_path].reservation_count;
    }

    /// If not enough (approximately) space, do not reserve.
    static ReservationPtr tryToReserve(const String & disk_path, UInt64 size)
    {
        UInt64 free_bytes = getUnreservedFreeSpace(disk_path);
        ///@TODO_IGR ASK twice reservation?
        if (free_bytes < size)
        {
            return {};
        }
        return std::make_unique<Reservation>(size, &reserved[disk_path], disk_path);
    }

private:
    static std::map<String, DiskReserve> reserved;
    static std::mutex mutex;
};

class Schema
{
    class Volume {
        friend class Schema;

    public:
        Volume(std::vector<String> paths_) : paths(std::move(paths_))
        {
        }

        DiskSpaceMonitor::ReservationPtr reserve(UInt64 expected_size) const {
            for (size_t i = 0; i != paths.size(); ++i) {
                last_used = (last_used + 1) % paths.size();
                auto reservation = DiskSpaceMonitor::tryToReserve(paths[last_used], expected_size);
                if (reservation) {
                    return reservation;
                }
            }
            return {};
        }

    private:
        const Strings paths;
        mutable size_t last_used = 0; ///@TODO_IGR ASK It is thread safe, but it is not consistent. :(
                                      /// P.S. I do not want to use mutex here
    };

public:
    Schema(const std::vector<Strings> & disks) {
        for (const Strings & volume : disks) {
            volumes.emplace_back(volume);
        }
    }

    ///@TODO_IGR ASK maybe iterator without copy?
    Strings getFullPaths() const {
        Strings res;
        for (const auto & volume : volumes) {
            std::copy(volume.paths.begin(), volume.paths.end(), std::back_inserter(res));
        }
        return res;
    }

    DiskSpaceMonitor::ReservationPtr reserve(UInt64 expected_size) const {
        for (auto & volume : volumes) {
            auto reservation = volume.reserve(expected_size);
            if (reservation) {
                return reservation;
            }
        }
        return {};
    }

private:
    std::vector<Volume> volumes;
};

}
