#include <base/getDataTLBEntries.h>

#if defined(__x86_64__)
#    include <cpuid.h>
#elif defined(OS_LINUX) && defined(__aarch64__)
#    include <cstdint>
#    include <cstdlib>
#    include <fstream>
#    include <string>
#endif

namespace
{
    constexpr size_t default_data_tlb_entries = 1024;

#if defined(__x86_64__)
    /// Intel deterministic address translation parameters: one sub-leaf per translation cache.
    size_t readFromIntelLeaf()
    {
        unsigned max_leaf = 0;
        unsigned ebx = 0;
        unsigned ecx = 0;
        unsigned edx = 0;
        __cpuid(0, max_leaf, ebx, ecx, edx);
        if (max_leaf < 0x18)
            return 0;

        unsigned max_sub_leaf = 0;
        __cpuid_count(0x18, 0, max_sub_leaf, ebx, ecx, edx);

        size_t best_level = 0;
        size_t best_entries = 0;
        for (unsigned sub_leaf = 0; sub_leaf <= max_sub_leaf; ++sub_leaf)
        {
            unsigned eax = 0;
            __cpuid_count(0x18, sub_leaf, eax, ebx, ecx, edx);

            const unsigned type = edx & 0x1Fu;
            /// 1 = data, 3 = unified, 4 = load only. Instruction (2) and store only (5) TLBs do not serve loads.
            if (type != 1 && type != 3 && type != 4)
                continue;
            if ((ebx & 0x1u) == 0)
                continue; /// does not hold 4 KiB pages

            const size_t level = (edx >> 5) & 0x7u;
            const size_t ways = (ebx >> 16) & 0xFFFFu;
            const size_t entries = ways * ecx;
            if (level > best_level || (level == best_level && entries > best_entries))
            {
                best_level = level;
                best_entries = entries;
            }
        }
        return best_entries;
    }

    /// AMD: the L2 data TLB size for 4 KiB pages is EBX[27:16] of leaf 0x80000006.
    size_t readFromAMDLeaf()
    {
        unsigned max_leaf = 0;
        unsigned ebx = 0;
        unsigned ecx = 0;
        unsigned edx = 0;
        __cpuid(0x80000000u, max_leaf, ebx, ecx, edx);
        if (max_leaf < 0x80000006u)
            return 0;

        unsigned eax = 0;
        __cpuid(0x80000006u, eax, ebx, ecx, edx);
        return (ebx >> 16) & 0xFFFu;
    }
#elif defined(OS_LINUX) && defined(__aarch64__)
    /// The published L2 TLB sizes of the cores.
    size_t entriesForArmCore(unsigned part)
    {
        switch (part)
        {
            case 0xd0c: return 1280; /// Neoverse N1 (e.g. AWS Graviton2)
            case 0xd40: return 2048; /// Neoverse V1 (e.g. AWS Graviton3)
            case 0xd49: return 1280; /// Neoverse N2
            case 0xd4f: return 2048; /// Neoverse V2 (e.g. AWS Graviton4)
            default: return 0;
        }
    }

    size_t readFromMIDR()
    {
        std::ifstream file("/sys/devices/system/cpu/cpu0/regs/identification/midr_el1");
        std::string value;
        if (!(file >> value))
            return 0;

        const uint64_t midr = std::strtoull(value.c_str(), nullptr, 16);
        const unsigned implementer = (midr >> 24) & 0xFFu;
        const unsigned part = (midr >> 4) & 0xFFFu;
        if (implementer != 0x41) /// Arm Ltd.
            return 0;
        return entriesForArmCore(part);
    }
#endif

    size_t getDataTLBEntriesImpl()
    {
#if defined(__x86_64__)
        if (size_t v = readFromIntelLeaf())
            return v;
        if (size_t v = readFromAMDLeaf())
            return v;
#elif defined(OS_LINUX) && defined(__aarch64__)
        if (size_t v = readFromMIDR())
            return v;
#endif
        return default_data_tlb_entries;
    }
}

/// Function-local static: computed once, and LTO-safe (a namespace-scope
/// dynamic initializer can be DCE'd).
size_t getDataTLBEntries()
{
    static const size_t result = getDataTLBEntriesImpl();
    return result;
}
