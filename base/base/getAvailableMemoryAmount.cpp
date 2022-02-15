#include <stdexcept>
#include <fstream>
#include <base/getAvailableMemoryAmount.h>
#include <base/getPageSize.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/param.h>
#if defined(BSD)
#include <sys/sysctl.h>
#include <sys/vmmeter.h>
#endif


uint64_t getAvailableMemoryAmountOrZero()
{
    int64_t page_size = getPageSize();
    if (page_size <= 0)
        return 0;

#if defined(__FreeBSD__)
    struct vmtotal vmt;
    size_t vmt_size = sizeof(vmt);
    if (sysctlbyname("vm.vmtotal", &vmt, &vmt_size, NULL, 0) < 0)
       return 0;
    uint64_t available_pages = vmt.t_avm;
#else
    uint64_t available_pages = sysconf(_SC_AVPHYS_PAGES);
#endif

    return page_size * available_pages;
}


uint64_t getAvailableMemoryAmount()
{
    auto res = getAvailableMemoryAmountOrZero();
    if (!res)
        throw std::runtime_error("Cannot determine available memory amount");
    return res;
}
