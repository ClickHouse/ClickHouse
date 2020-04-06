#include <Functions/TargetSpecific.h>

#if defined(__GNUC__) || defined(__clang__)
#   include <cpuid.h>
#else
#   error "Only CLANG and GCC compilers are supported for dynamic dispatch"
#endif

namespace DB
{

int GetSupportedArches() {
    unsigned int eax, ebx, ecx, edx;
    if (!__get_cpuid(1, &eax, &ebx, &ecx, &edx)) {
        return 0;
    }
    int res = 0;
    if (ecx & bit_SSE4_2)
        res |= static_cast<int>(TargetArch::SSE4);
    if ((ecx & bit_OSXSAVE) && (ecx & bit_AVX)) {
        // TODO(dakovalkov): check XGETBV.
        res |= static_cast<int>(TargetArch::AVX);
        if (__get_cpuid(7, &eax, &ebx, &ecx, &edx) && (ebx & bit_AVX2)) {
            res |= static_cast<int>(TargetArch::AVX2);
        }
        // TODO(dakovalkov): check AVX512 support.
    }
    return res;
}

bool IsArchSupported(TargetArch arch)
{
    static int arches = GetSupportedArches();
    return arch == TargetArch::Default || (arches & static_cast<int>(arch));
}

} // namespace DB
