#include <base/getMemoryAmount.h>

#include <base/getPageSize.h>

#include <fstream>
#include <stdexcept>

#include <unistd.h>
#include <sys/types.h>
#include <sys/param.h>
#if defined(BSD)
#include <sys/sysctl.h>
#endif


namespace
{

std::optional<uint64_t> getCgroupsMemoryLimit(const std::string & setting)
{
#if defined(OS_LINUX)
    std::filesystem::path default_cgroups_mount = "/sys/fs/cgroup";
    std::ifstream file(default_cgroups_mount / setting);
    if (!file.is_open())
        return {};
    uint64_t value;
    if (file >> value)
        return {value};
    else
        return {}; /// e.g. the cgroups default "max"
#else
    return {};
#endif
}

}

/** Returns the size of physical memory (RAM) in bytes.
  * Returns 0 on unsupported platform
  */
uint64_t getMemoryAmountOrZero()
{
    int64_t num_pages = sysconf(_SC_PHYS_PAGES);
    if (num_pages <= 0)
        return 0;

    int64_t page_size = getPageSize();
    if (page_size <= 0)
        return 0;

    uint64_t memory_amount = num_pages * page_size;

    /// Respect the memory limit set by cgroups

    /// cgroups v2
    auto limit_v2 = getCgroupsMemoryLimit("memory.max");
    if (limit_v2.has_value() && *limit_v2 < memory_amount)
         memory_amount = *limit_v2;
    else
    {
        auto limit_v1 = getCgroupsMemoryLimit("memory/memory.limit_in_bytes");
        if (limit_v1.has_value() && *limit_v1 < memory_amount)
             memory_amount = *limit_v1;
    }

    return memory_amount;
}


uint64_t getMemoryAmount()
{
    auto res = getMemoryAmountOrZero();
    if (!res)
        throw std::runtime_error("Cannot determine memory amount");
    return res;
}
