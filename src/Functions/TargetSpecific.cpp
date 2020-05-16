#include <Functions/TargetSpecific.h>

#include <Common/CpuId.h>

namespace DB
{

UInt32 GetSupportedArches()
{
    UInt32 result = 0;
    if (Cpu::haveSSE42())
        result |= static_cast<UInt32>(TargetArch::SSE42);
    if (Cpu::haveAVX())
        result |= static_cast<UInt32>(TargetArch::AVX);
    if (Cpu::haveAVX2())
        result |= static_cast<UInt32>(TargetArch::AVX2);
    if (Cpu::haveAVX512F())
        result |= static_cast<UInt32>(TargetArch::AVX512F);
    return result;
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
        case TargetArch::SSE42:   return "sse42";
        case TargetArch::AVX:     return "avx";
        case TargetArch::AVX2:    return "avx2";
        case TargetArch::AVX512F: return "avx512f";
    }

    __builtin_unreachable();
}

}
