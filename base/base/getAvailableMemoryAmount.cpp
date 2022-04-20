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
#if defined(_SC_PHYS_PAGES) // linux
    return getPageSize() * sysconf(_SC_PHYS_PAGES);
#elif defined(__FreeBSD__)
    struct vmtotal vmt;
    size_t vmt_size = sizeof(vmt);
    if (sysctlbyname("vm.vmtotal", &vmt, &vmt_size, NULL, 0) == 0)
        return getPageSize() * vmt.t_avm;
    else
        return 0;
#else // darwin
    unsigned int usermem;
    size_t len = sizeof(usermem);
    static int mib[2] = { CTL_HW, HW_USERMEM };
    if (sysctl(mib, 2, &usermem, &len, nullptr, 0) == 0 && len == sizeof(usermem))
        return usermem;
    else
        return 0;
#endif
}


uint64_t getAvailableMemoryAmount()
{
    auto res = getAvailableMemoryAmountOrZero();
    if (!res)
        throw std::runtime_error("Cannot determine available memory amount");
    return res;
}
