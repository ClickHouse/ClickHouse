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
                std::lock_guard<std::mutex> lock(DiskSpaceMonitor::mutex);
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
            std::lock_guard<std::mutex> lock(DiskSpaceMonitor::mutex);
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
            std::lock_guard<std::mutex> lock(DiskSpaceMonitor::mutex);
            DiskSpaceMonitor::reserved_bytes += size;
            ++DiskSpaceMonitor::reservation_count;
        }

    private:
        UInt64 size;
        CurrentMetrics::Increment metric_increment;
    };

    using ReservationPtr = std::unique_ptr<Reservation>;

    static UInt64 getUnreservedFreeSpace(const std::string & path)
    {
        struct statvfs fs;

        if (statvfs(path.c_str(), &fs) != 0)
            throwFromErrno("Could not calculate available disk space (statvfs)", ErrorCodes::CANNOT_STATVFS);

        UInt64 res = fs.f_bfree * fs.f_bsize;

        /// Heuristic by Michael Kolupaev: reserve 30 MB more, because statvfs shows few megabytes more space than df.
        res -= std::min(res, static_cast<UInt64>(30 * (1ul << 20)));

        std::lock_guard<std::mutex> lock(mutex);

        if (reserved_bytes > res)
            res = 0;
        else
            res -= reserved_bytes;

        return res;
    }

    static UInt64 getReservedSpace()
    {
        std::lock_guard<std::mutex> lock(mutex);
        return reserved_bytes;
    }

    static UInt64 getReservationCount()
    {
        std::lock_guard<std::mutex> lock(mutex);
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

private:
    static UInt64 reserved_bytes;
    static UInt64 reservation_count;
    static std::mutex mutex;
};

}
