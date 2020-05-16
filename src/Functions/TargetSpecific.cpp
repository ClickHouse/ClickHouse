#include <Functions/TargetSpecific.h>

#if defined(__GNUC__)
#   include <cpuid.h>
#   include <x86intrin.h>
#else
#   error "Only CLANG and GCC compilers are supported for dynamic dispatch"
#endif

namespace DB
{

__attribute__ ((target("xsave")))
UInt64 xgetbv(UInt32 ecx)
{
    return _xgetbv(ecx);
}

UInt32 GetSupportedArches()
{
    UInt32 eax, ebx, ecx, edx;
    if (!__get_cpuid(1, &eax, &ebx, &ecx, &edx))
        return 0;

    UInt32 res = 0;
    if (ecx & bit_SSE4_2)
        res |= static_cast<UInt32>(TargetArch::SSE4);
    /// (xgetbv(0) & 0x6) == 0x6 checks that XMM state and YMM state are enabled.
    if ((ecx & bit_OSXSAVE) && (ecx & bit_AVX) && (xgetbv(0) & 0x6) == 0x6)
    {
        res |= static_cast<UInt32>(TargetArch::AVX);
        if (__get_cpuid(7, &eax, &ebx, &ecx, &edx) && (ebx & bit_AVX2))
        {
            res |= static_cast<UInt32>(TargetArch::AVX2);
            if (ebx & bit_AVX512F)
            {
                res |= static_cast<UInt32>(TargetArch::AVX512F);
            }
        }
    }
    return res;
}

bool IsArchSupported(TargetArch arch)
{
    static UInt32 arches = GetSupportedArches();
    return arch == TargetArch::Default || (arches & static_cast<UInt32>(arch));
}

String ToString(TargetArch arch)
{
    switch (arch)
    {
        case TargetArch::Default: return "default";
        case TargetArch::SSE4:    return "sse4";
        case TargetArch::AVX:     return "avx";
        case TargetArch::AVX2:    return "avx2";
        case TargetArch::AVX512F: return "avx512f";
    }

    __builtin_unreachable();
}

}
