#pragma once
#if defined (OS_LINUX)

#include <cstdint>

#include <Core/Types.h>


namespace DB
{

class ReadBuffer;


/** Opens file /proc/mounts, reads all mounted filesystems and
  * calculates disk usage.
  */
class DiskStatisticsOS
{
public:
    // In bytes
    struct Data
    {
        uint64_t total_bytes;
        uint64_t used_bytes;
        uint64_t total_inodes;
        uint64_t used_inodes;
    };

    Data get();
};

}

#endif
