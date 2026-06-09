#include <base/getL2CacheSize.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <unistd.h>

#if defined(__x86_64__)
#    include <cpuid.h>
#endif

namespace
{
    constexpr size_t default_l2_size = 256 * 1024;

#if defined(__x86_64__)
    /// Read L2 data/unified cache size via CPUID deterministic cache parameters.
    /// Intel uses leaf 4, AMD uses 0x8000001D, both with identical sub-leaf format
    /// (Intel since Nehalem, AMD since Zen). x86_64-v4 implies Skylake-X+ / Zen 4+,
    /// so at least one of the two leaves is always supported.
    size_t readL2FromCPUID()
    {
        const unsigned leaves[] = {0x4u, 0x8000001Du};
        for (unsigned leaf : leaves)
        {
            /// Probe maximum supported leaf for the corresponding range.
            unsigned max_eax = 0;
            unsigned ign_ebx = 0;
            unsigned ign_ecx = 0;
            unsigned ign_edx = 0;
            __cpuid(leaf & 0x80000000u, max_eax, ign_ebx, ign_ecx, ign_edx);
            if (leaf > max_eax)
                continue;

            for (unsigned sub = 0; sub < 32; ++sub)
            {
                unsigned eax = 0;
                unsigned ebx = 0;
                unsigned ecx = 0;
                unsigned edx = 0;
                __cpuid_count(leaf, sub, eax, ebx, ecx, edx);

                unsigned cache_type = eax & 0x1Fu;
                if (cache_type == 0)
                    break; /// no more cache descriptors

                unsigned cache_level = (eax >> 5) & 0x7u;
                if (cache_level != 2)
                    continue;
                /// 1 = data, 2 = instruction, 3 = unified. We want bytes accessible to loads/stores.
                if (cache_type != 1 && cache_type != 3)
                    continue;

                unsigned line_size  = (ebx & 0xFFFu) + 1;
                unsigned partitions = ((ebx >> 12) & 0x3FFu) + 1;
                unsigned ways       = ((ebx >> 22) & 0x3FFu) + 1;
                unsigned sets       = ecx + 1;
                return static_cast<size_t>(line_size) * partitions * ways * sets;
            }
        }
        return 0;
    }
#endif

#if defined(OS_LINUX)
    size_t readL2FromSysfs()
    {
        FILE * f = std::fopen("/sys/devices/system/cpu/cpu0/cache/index2/size", "r");
        if (!f)
            return 0;

        char buf[32] = {};
        size_t n = std::fread(buf, 1, sizeof(buf) - 1, f);
        (void)std::fclose(f);
        if (n == 0)
            return 0;

        char * end = nullptr;
        uint64_t value = std::strtoull(buf, &end, 10);
        if (end == buf)
            return 0;
        if (*end == 'K' || *end == 'k')
            value *= 1024ull;
        else if (*end == 'M' || *end == 'm')
            value *= 1024ull * 1024ull;
        return static_cast<size_t>(value);
    }
#endif

    size_t getL2CacheSizeImpl()
    {
#if defined(__x86_64__)
        if (size_t v = readL2FromCPUID())
            return v;
#endif
#if defined(OS_LINUX) && defined(_SC_LEVEL2_CACHE_SIZE)
        if (auto ret = ::sysconf(_SC_LEVEL2_CACHE_SIZE); ret > 0)
            return static_cast<size_t>(ret);
#endif
#if defined(OS_LINUX)
        if (size_t fs = readL2FromSysfs())
            return fs;
#endif
        return default_l2_size;
    }
}

/// Function-local static so the initialization survives LTO (mirrors
/// `getPageSize.cpp`'s handling of the musl/aarch64 LTO DCE issue).
size_t getL2CacheSize()
{
    static const size_t result = getL2CacheSizeImpl();
    return result;
}
