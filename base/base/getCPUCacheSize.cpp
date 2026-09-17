#include <base/getCPUCacheSize.h>

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

#if defined(__x86_64__)
    /// CPUID deterministic cache parameters: Intel leaf 4 and AMD leaf 0x8000001D share the
    /// sub-leaf format, so both are walked.
    size_t readFromCPUID(unsigned level)
    {
        const unsigned leaves[] = {0x4u, 0x8000001Du};
        for (unsigned leaf : leaves)
        {
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
                    break;

                unsigned cache_level = (eax >> 5) & 0x7u;
                if (cache_level != level)
                    continue;
                /// 1 = data, 2 = instruction, 3 = unified.
                const bool wanted = cache_type == 1 || (cache_type == 3 && level > 1);
                if (!wanted)
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
    /// Apple Silicon does not expose CPUID.
    size_t readFromSysctl(unsigned level)
    {
        const char * name = level == 1 ? "hw.l1dcachesize" : (level == 2 ? "hw.l2cachesize" : "hw.l3cachesize");
        uint64_t value = 0;
        size_t size = sizeof(value);
        if (sysctlbyname(name, &value, &size, nullptr, 0) == 0)
            return static_cast<size_t>(value);
        return 0;
    }
#elif defined(OS_LINUX)
    size_t readFromSysfs(unsigned level)
    {
        namespace fs = std::filesystem;
        const fs::path base = "/sys/devices/system/cpu/cpu0/cache";

        std::error_code ec;
        for (unsigned index = 0; ; ++index)
        {
            const fs::path dir = base / ("index" + std::to_string(index));
            if (!fs::exists(dir, ec))
                break;

            unsigned cache_level = 0;
            {
                std::ifstream level_file(dir / "level");
                if (!(level_file >> cache_level))
                    continue;
            }
            if (cache_level != level)
                continue;

            std::string type;
            {
                std::ifstream type_file(dir / "type");
                std::getline(type_file, type);
            }
            if (type != "Data" && !(type == "Unified" && level > 1))
                continue;

            /// A number with an optional unit suffix, e.g. "64K" or "1M".
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

}

size_t getCPUDataCacheSize([[maybe_unused]] unsigned level)
{
#if defined(__x86_64__)
    return readFromCPUID(level);
#elif defined(OS_DARWIN)
    return readFromSysctl(level);
#elif defined(OS_LINUX)
    return readFromSysfs(level);
#else
    /// No `sysconf(_SC_LEVEL1_DCACHE_SIZE)` fallback: x86-only in glibc, unimplemented in musl.
    return 0;
#endif
}
