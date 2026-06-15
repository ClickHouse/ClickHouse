#include <base/getL2CacheSize.h>

#if defined(__x86_64__)
#    include <cpuid.h>
#endif

namespace
{
    constexpr size_t default_l2_size = 256 * 1024;

#if defined(__x86_64__)
    /// Read L2 data/unified cache size via CPUID deterministic cache parameters
    /// (Intel leaf 4, AMD leaf 0x8000001D; identical sub-leaf format).
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

    size_t getL2CacheSizeImpl()
    {
#if defined(__x86_64__)
        if (size_t v = readL2FromCPUID())
            return v;
#endif
        /// No `sysconf(_SC_LEVEL2_CACHE_SIZE)` fallback: it is x86-only in glibc
        /// and unimplemented in musl. 256 KiB is a safe default on non-x86.
        return default_l2_size;
    }
}

/// Function-local static: computed once, and LTO-safe (a namespace-scope
/// dynamic initializer can be DCE'd).
size_t getL2CacheSize()
{
    static const size_t result = getL2CacheSizeImpl();
    return result;
}
