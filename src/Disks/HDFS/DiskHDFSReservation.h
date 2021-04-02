#pragma once

#include <Disks/IDisk.h>


namespace DB
{

class DiskHDFSReservation : public IReservation
{

public:
    DiskHDFSReservation(const DiskHDFSPtr & disk_, UInt64 size_)
        : disk(disk_)
        , size(size_)
        , metric_increment(CurrentMetrics::DiskSpaceReservedForMerge, size_)
    {
    }

    ~DiskHDFSReservation() override
    {
        try
        {
            std::lock_guard lock(disk->reservation_mutex);
            if (disk->reserved_bytes < size)
            {
                disk->reserved_bytes = 0;
                //LOG_ERROR(&Poco::Logger::get("DiskLocal"), "Unbalanced reservations size for disk '{}'", disk->getName());
            }
            else
            {
                disk->reserved_bytes -= size;
            }

            if (disk->reservation_count == 0)
            {
                //LOG_ERROR(&Poco::Logger::get("DiskLocal"), "Unbalanced reservation count for disk '{}'", disk->getName());
            }
            else
                --disk->reservation_count;
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    UInt64 getSize() const override { return size; }

    DiskPtr getDisk(size_t /* i */) const override { return disk; }

    void update(UInt64 new_size) override
    {
        std::lock_guard lock(disk->reservation_mutex);
        disk->reserved_bytes -= size;
        size = new_size;
        disk->reserved_bytes += size;
    }

    Disks getDisks() const override { return {}; }

private:
    DiskHDFSPtr disk;
    UInt64 size;
    CurrentMetrics::Increment metric_increment;
};

}
