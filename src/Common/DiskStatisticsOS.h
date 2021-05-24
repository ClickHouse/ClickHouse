#pragma once
#if defined (OS_LINUX)

#include <cstdint>

#include <Core/Types.h>

#include <IO/ReadBufferFromFile.h>

namespace DB
{

/** Opens file /proc/mounts, reads all mounted filesystems and
  * calculates disk usage.
  */
class DiskStatisticsOS
{
public:
    // In bytes
    struct Data
    {
        uint64_t total;
        uint64_t used;
    };

    DiskStatisticsOS();
    ~DiskStatisticsOS();

    Data get();

private:
    String readNextFilesystem(ReadBuffer& mounts_in);
};

}

#endif
