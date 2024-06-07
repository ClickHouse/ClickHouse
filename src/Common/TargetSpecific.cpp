#include <base/defines.h>
#include <Common/TargetSpecific.h>

#include <Common/CPUID.h>

namespace DB
{

UInt32 getSupportedArchs()
{
    UInt32 result = 0;
    if (CPU::CPUFlagsCache::have_SSE42)
        result |= static_cast<UInt32>(TargetArch::SSE42);
    if (CPU::CPUFlagsCache::have_AVX)
        result |= static_cast<UInt32>(TargetArch::AVX);
    if (CPU::CPUFlagsCache::have_AVX2)
        result |= static_cast<UInt32>(TargetArch::AVX2);
    if (CPU::CPUFlagsCache::have_AVX512F)
        result |= static_cast<UInt32>(TargetArch::AVX512F);
    if (CPU::CPUFlagsCache::have_AVX512BW)
        result |= static_cast<UInt32>(TargetArch::AVX512BW);
    if (CPU::CPUFlagsCache::have_AVX512VBMI)
        result |= static_cast<UInt32>(TargetArch::AVX512VBMI);
    if (CPU::CPUFlagsCache::have_AVX512VBMI2)
        result |= static_cast<UInt32>(TargetArch::AVX512VBMI2);
    if (CPU::CPUFlagsCache::have_AMXBF16)
        result |= static_cast<UInt32>(TargetArch::AMXBF16);
    if (CPU::CPUFlagsCache::have_AMXTILE)
        result |= static_cast<UInt32>(TargetArch::AMXTILE);
    if (CPU::CPUFlagsCache::have_AMXINT8)
        result |= static_cast<UInt32>(TargetArch::AMXINT8);
    return result;
}

bool isArchSupported(TargetArch arch)
{
    static UInt32 arches = getSupportedArchs();
    return arch == TargetArch::Default || (arches & static_cast<UInt32>(arch));
}

String toString(TargetArch arch)
{
    switch (arch)
    {
        case TargetArch::Default: return "default";
        case TargetArch::SSE42:   return "sse42";
        case TargetArch::AVX:     return "avx";
        case TargetArch::AVX2:    return "avx2";
        case TargetArch::AVX512F: return "avx512f";
        case TargetArch::AVX512BW:    return "avx512bw";
        case TargetArch::AVX512VBMI:  return "avx512vbmi";
        case TargetArch::AVX512VBMI2: return "avx512vbmi2";
        case TargetArch::AMXBF16: return "amxbf16";
        case TargetArch::AMXTILE: return "amxtile";
        case TargetArch::AMXINT8: return "amxint8";
    }
}

}
