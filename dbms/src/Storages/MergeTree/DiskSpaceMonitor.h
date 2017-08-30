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
        void update(size_t new_size)
        {
            std::lock_guard<std::mutex> lock(DiskSpaceMonitor::mutex);
            DiskSpaceMonitor::reserved_bytes -= size;
            size = new_size;
            DiskSpaceMonitor::reserved_bytes += size;
        }

        size_t getSize() const
        {
            return size;
        }

        Reservation(size_t size_)
            : size(size_), metric_increment(CurrentMetrics::DiskSpaceReservedForMerge, size)
        {
            std::lock_guard<std::mutex> lock(DiskSpaceMonitor::mutex);
            DiskSpaceMonitor::reserved_bytes += size;
            ++DiskSpaceMonitor::reservation_count;
        }

    private:
        size_t size;
        CurrentMetrics::Increment metric_increment;
    };

    using ReservationPtr = std::unique_ptr<Reservation>;

    static size_t getUnreservedFreeSpace(const std::string & path)
    {
        struct statvfs fs;

        if (statvfs(path.c_str(), &fs) != 0)
            throwFromErrno("Could not calculate available disk space (statvfs)", ErrorCodes::CANNOT_STATVFS);

        size_t res = fs.f_bfree * fs.f_bsize;

        /// Heuristic by Michael Kolupaev: reserve 30 MB more, because statvfs shows few megabytes more space than df.
        res -= std::min(res, static_cast<size_t>(30 * (1ul << 20)));

        std::lock_guard<std::mutex> lock(mutex);

        if (reserved_bytes > res)
            res = 0;
        else
            res -= reserved_bytes;

        return res;
    }

    static size_t getReservedSpace()
    {
        std::lock_guard<std::mutex> lock(mutex);
        return reserved_bytes;
    }

    static size_t getReservationCount()
    {
        std::lock_guard<std::mutex> lock(mutex);
        return reservation_count;
    }

    /// If not enough (approximately) space, throw an exception.
    static ReservationPtr reserve(const std::string & path, size_t size)
    {
        size_t free_bytes = getUnreservedFreeSpace(path);
        if (free_bytes < size)
            throw Exception("Not enough free disk space to reserve: " + formatReadableSizeWithBinarySuffix(free_bytes) + " available, "
                + formatReadableSizeWithBinarySuffix(size) + " requested", ErrorCodes::NOT_ENOUGH_SPACE);
        return std::make_unique<Reservation>(size);
    }

private:
    static size_t reserved_bytes;
    static size_t reservation_count;
    static std::mutex mutex;
};

}
