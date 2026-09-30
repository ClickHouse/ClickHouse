#include <base/getL2CacheSize.h>

#if defined(__x86_64__)
#    include <cpuid.h>
#elif defined(OS_DARWIN)
#    include <sys/sysctl.h>
#elif defined(OS_LINUX)
#    include <filesystem>
#    include <fstream>
#    include <string>
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
#elif defined(OS_DARWIN)
    /// Apple Silicon does not expose CPUID; the kernel reports the size via sysctl.
    size_t readL2FromSysctl()
    {
        uint64_t value = 0;
        size_t size = sizeof(value);
        if (sysctlbyname("hw.l2cachesize", &value, &size, nullptr, 0) == 0)
            return static_cast<size_t>(value);
        return 0;
    }
#elif defined(OS_LINUX)
    /// Non-x86 Linux (e.g. aarch64): read the cache geometry the kernel exposes
    /// under sysfs. Iterate the per-CPU cache descriptors and pick the L2
    /// data/unified one, mirroring the level/type filtering done for CPUID.
    size_t readL2FromSysfs()
    {
        namespace fs = std::filesystem;
        const fs::path base = "/sys/devices/system/cpu/cpu0/cache";

        std::error_code ec;
        for (unsigned index = 0; ; ++index)
        {
            const fs::path dir = base / ("index" + std::to_string(index));
            if (!fs::exists(dir, ec))
                break;

            unsigned level = 0;
            {
                std::ifstream level_file(dir / "level");
                if (!(level_file >> level))
                    continue;
            }
            if (level != 2)
                continue;

            std::string type;
            {
                std::ifstream type_file(dir / "type");
                std::getline(type_file, type);
            }
            if (type != "Data" && type != "Unified")
                continue;

            /// "size" is a number with an optional unit suffix, e.g. "2048K" or "1M".
            size_t value = 0;
            char suffix = 0;
            {
                std::ifstream size_file(dir / "size");
                size_file >> value >> suffix;
            }
            if (suffix == 'K' || suffix == 'k')
                value *= 1024;
            else if (suffix == 'M' || suffix == 'm')
                value *= 1024 * 1024;
            return value;
        }
        return 0;
    }
#endif

    size_t getL2CacheSizeImpl()
    {
#if defined(__x86_64__)
        if (size_t v = readL2FromCPUID())
            return v;
#elif defined(OS_DARWIN)
        if (size_t v = readL2FromSysctl())
            return v;
#elif defined(OS_LINUX)
        if (size_t v = readL2FromSysfs())
            return v;
#endif
        /// No `sysconf(_SC_LEVEL2_CACHE_SIZE)` fallback: it is x86-only in glibc
        /// and unimplemented in musl. 256 KiB is a safe default if probing fails.
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
