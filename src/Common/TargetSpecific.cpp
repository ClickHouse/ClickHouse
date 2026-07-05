#include <Common/TargetSpecific.h>

#include <Common/CPUID.h>

namespace DB
{

UInt32 getSupportedArchs()
{
    UInt32 result = 0;

    // x86-64-v2: SSE3, SSSE3, SSE4.1, SSE4.2, POPCNT
    if (CPU::CPUFlagsCache::have_SSE3
        && CPU::CPUFlagsCache::have_SSSE3
        && CPU::CPUFlagsCache::have_SSE41
        && CPU::CPUFlagsCache::have_SSE42
        && CPU::CPUFlagsCache::have_POPCNT)
    {
        result |= static_cast<UInt32>(TargetArch::x86_64_v2);
    }

    // x86-64-v3: v2 + AVX, AVX2, BMI1, BMI2, F16C, FMA, LZCNT, MOVBE
    // x86-64 levels are cumulative, so v3 requires v2
    if ((result & static_cast<UInt32>(TargetArch::x86_64_v2)) == static_cast<UInt32>(TargetArch::x86_64_v2)
        && CPU::CPUFlagsCache::have_AVX
        && CPU::CPUFlagsCache::have_AVX2
        && CPU::CPUFlagsCache::have_BMI1
        && CPU::CPUFlagsCache::have_BMI2
        && CPU::CPUFlagsCache::have_F16C
        && CPU::CPUFlagsCache::have_FMA
        && CPU::CPUFlagsCache::have_LZCNT
        && CPU::CPUFlagsCache::have_MOVBE)
    {
        result |= static_cast<UInt32>(TargetArch::x86_64_v3);
    }

    // x86-64-v4: v3 + AVX512F, AVX512BW, AVX512CD, AVX512DQ, AVX512VL
    // x86-64 levels are cumulative, so v4 requires v3
    if ((result & static_cast<UInt32>(TargetArch::x86_64_v3)) == static_cast<UInt32>(TargetArch::x86_64_v3)
        && CPU::CPUFlagsCache::have_AVX512F
        && CPU::CPUFlagsCache::have_AVX512BW
        && CPU::CPUFlagsCache::have_AVX512CD
        && CPU::CPUFlagsCache::have_AVX512DQ
        && CPU::CPUFlagsCache::have_AVX512VL)
    {
        result |= static_cast<UInt32>(TargetArch::x86_64_v4);
    }

    // Ice Lake: v4 + AVX512VBMI, AVX512VBMI2, AVX512IFMA, AVX512VNNI, AVX512VPOPCNTDQ, AVX512BITALG, GFNI, VAES, VPCLMULQDQ
    if ((result & static_cast<UInt32>(TargetArch::x86_64_v4)) == static_cast<UInt32>(TargetArch::x86_64_v4)
        && CPU::CPUFlagsCache::have_AVX512VBMI
        && CPU::CPUFlagsCache::have_AVX512VBMI2
        && CPU::CPUFlagsCache::have_AVX512IFMA
        && CPU::CPUFlagsCache::have_AVX512VNNI
        && CPU::CPUFlagsCache::have_AVX512VPOPCNTDQ
        && CPU::CPUFlagsCache::have_AVX512BITALG
        && CPU::CPUFlagsCache::have_GFNI
        && CPU::CPUFlagsCache::have_VAES
        && CPU::CPUFlagsCache::have_VPCLMULQDQ)
    {
        result |= static_cast<UInt32>(TargetArch::x86_64_icelake);
    }

    // Sapphire Rapids: Ice Lake + AVX512BF16, AVX512FP16, AMX-BF16, AMX-INT8, AMX-TILE, AVXVNNI
    if ((result & static_cast<UInt32>(TargetArch::x86_64_icelake)) == static_cast<UInt32>(TargetArch::x86_64_icelake)
        && CPU::CPUFlagsCache::have_AVX512BF16
        && CPU::CPUFlagsCache::have_AVX512FP16
        && CPU::CPUFlagsCache::have_AVXVNNI
        && CPU::CPUFlagsCache::have_AMXBF16
        && CPU::CPUFlagsCache::have_AMXTILE
        && CPU::CPUFlagsCache::have_AMXINT8)
    {
        result |= static_cast<UInt32>(TargetArch::x86_64_sapphirerapids);
    }

    // CPU vendor detection
    if (CPU::CPUFlagsCache::have_GenuineIntel)
        result |= static_cast<UInt32>(TargetArch::GenuineIntel);

    return result;
}

String toString(TargetArch arch)
{
    switch (arch)
    {
        case TargetArch::Default:               return "default";
        case TargetArch::x86_64_v2:             return "x86-64-v2";
        case TargetArch::x86_64_v3:             return "x86-64-v3";
        case TargetArch::x86_64_v4:             return "x86-64-v4";
        case TargetArch::x86_64_icelake:        return "x86-64-icelake";
        case TargetArch::x86_64_sapphirerapids: return "x86-64-sapphirerapids";
        case TargetArch::GenuineIntel:          return "GenuineIntel";
    }

    // This should never be reached. If it is, someone added a new TargetArch
    // value but forgot to add a case above. The compiler should warn about this.
    throw std::logic_error("Unknown TargetArch value: " + std::to_string(static_cast<UInt32>(arch)));
}

}
