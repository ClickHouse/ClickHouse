#include <Functions/TargetSpecific.h>

#include <Common/CpuId.h>

namespace DB
{

UInt32 getSupportedArchs()
{
    UInt32 result = 0;
    if (Cpu::CpuFlagsCache::have_SSE42)
        result |= static_cast<UInt32>(TargetArch::SSE42);
    if (Cpu::CpuFlagsCache::have_AVX)
        result |= static_cast<UInt32>(TargetArch::AVX);
    if (Cpu::CpuFlagsCache::have_AVX2)
        result |= static_cast<UInt32>(TargetArch::AVX2);
    if (Cpu::CpuFlagsCache::have_AVX512F)
        result |= static_cast<UInt32>(TargetArch::AVX512F);
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
    }

    __builtin_unreachable();
}

}
