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
    size_t readFromCPUID(unsigned level)
    {
        /// Intel leaf 4 and AMD leaf 0x8000001D describe the caches in the same sub-leaf format.
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
    size_t readFromSysctl(unsigned level)
    {
        const char * name = nullptr;
        if (level == 1)
            name = "hw.l1dcachesize";
        else if (level == 2)
            name = "hw.l2cachesize";
        else if (level == 3)
            name = "hw.l3cachesize";
        else
            return 0;

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
    /// `sysconf(_SC_LEVEL1_DCACHE_SIZE)` and similar names do not help: glibc has them only on x86, and musl has none.
    return 0;
#endif
}
