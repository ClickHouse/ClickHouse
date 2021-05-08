#if defined (OS_LINUX)

#include <cstdint>

#include <Core/Types.h>

#include <IO/ReadBufferFromFile.h>

namespace DB 
{

/** Opens file /proc/mounts. Keeps it open, reads all mounted filesytems and 
  * calculates disk usage.
  */ 
class DiskStatisticsOS 
{
public:
    // In bytes
    struct Data {
        uint64_t total;
        uint64_t used;
    };

    DiskStatisticsOS();
    ~DiskStatisticsOS();

    Data get();

private:
    String readNextFilesystem();

private:
    ReadBufferFromFile mounts_in;
};

}

#endif
