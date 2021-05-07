#pragma once
#if defined(OS_LINUX)

#include <cstdint>
#include <string>

#include <Core/Types.h>

#include <IO/ReadBufferFromFile.h>

namespace DB 
{

/** Opens file /proc/meminfo. Keeps it open and reads statistics about memory usage.
  * This is Linux specific.
  * See: man procfs
  */

class MemoryInfoOS 
{
public:
    // In kB
    struct Data {
        uint64_t total;
        uint64_t free;
        uint64_t buffers;
        uint64_t cached;
        uint64_t free_and_cached;

        uint64_t swap_total;
        uint64_t swap_free;
        uint64_t swap_cached;
    };

    MemoryInfoOS();
    ~MemoryInfoOS();

    Data get();

private:
    ReadBufferFromFile meminfo_in;
    
    bool readField(uint64_t & field_val, const String & field_name_target);

    void skipField();
};

}

#endif
