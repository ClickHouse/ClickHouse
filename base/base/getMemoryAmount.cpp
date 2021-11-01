#include <stdexcept>
#include <fstream>
#include <base/getMemoryAmount.h>
#include <base/getPageSize.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/param.h>
#if defined(BSD)
#include <sys/sysctl.h>
#endif


/** Returns the size of physical memory (RAM) in bytes.
  * Returns 0 on unsupported platform
  */
uint64_t getMemoryAmountOrZero()
{
#if defined(OS_LINUX)
    // Try to lookup at the Cgroup limit
    std::ifstream cgroup_limit("/sys/fs/cgroup/memory/memory.limit_in_bytes");
    if (cgroup_limit.is_open())
    {
        uint64_t amount = 0; // in case of read error
        cgroup_limit >> amount;
        return amount;
    }
#endif

    int64_t num_pages = sysconf(_SC_PHYS_PAGES);
    if (num_pages <= 0)
        return 0;

    int64_t page_size = getPageSize();
    if (page_size <= 0)
        return 0;

    return num_pages * page_size;
}


uint64_t getMemoryAmount()
{
    auto res = getMemoryAmountOrZero();
    if (!res)
        throw std::runtime_error("Cannot determine memory amount");
    return res;
}
