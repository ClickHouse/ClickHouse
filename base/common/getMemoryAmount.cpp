#include <stdexcept>
#include "common/getMemoryAmount.h"

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
    int64_t num_pages = sysconf(_SC_PHYS_PAGES);
    if (num_pages <= 0)
        return 0;

    int64_t page_size = sysconf(_SC_PAGESIZE);
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
