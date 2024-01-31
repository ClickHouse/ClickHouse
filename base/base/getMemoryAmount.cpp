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

std::optional<uint64_t> getCgroupsV2MemoryLimit(const std::string & setting)
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

    /// Respect the memory limit set by cgroups v2.
    /// Cgroups v1 is dead since many years and its limits are not considered for simplicity.

    auto limit = getCgroupsV2MemoryLimit("memory.max");
    if (limit.has_value() && *limit < memory_amount)
         memory_amount = *limit;

    return memory_amount;
}


uint64_t getMemoryAmount()
{
    auto res = getMemoryAmountOrZero();
    if (!res)
        throw std::runtime_error("Cannot determine memory amount");
    return res;
}
