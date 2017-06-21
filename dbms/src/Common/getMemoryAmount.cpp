#include "getMemoryAmount.h"

#if __has_include(<sys/sysinfo.h>)
#include <sys/sysinfo.h>
#endif

// http://nadeausoftware.com/articles/2012/09/c_c_tip_how_get_physical_memory_size_system

namespace DB
{

size_t getMemoryAmount()
{
#if __has_include(<sys/sysinfo.h>)
    struct sysinfo system_information;
    if (sysinfo(&system_information))
        return 0;
    return system_information.totalram;
#else
    return 0;
#endif
}

}
