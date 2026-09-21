#include <Common/TargetSpecific.h>

#include <Common/CPUID.h>

namespace DB
{

constinit UInt32 supported_archs = 0;

namespace
{
    /// Priority 101 runs before every constructor of default priority, so `supported_archs` is already
    /// valid for any other static initializer. `getSupportedArchs` only executes CPUID and reads no globals.
    [[gnu::constructor(101)]] void initSupportedArchs()
    {
        supported_archs = getSupportedArchs();
    }
}

UInt32 getSupportedArchs()
{
    UInt32 result = 0;

    // x86-64-v2: SSE3, SSSE3, SSE4.1, SSE4.2, POPCNT
    if (CPU::haveSSE3()
        && CPU::haveSSSE3()
        && CPU::haveSSE41()
        && CPU::haveSSE42()
        && CPU::havePOPCNT())
    {
        result |= static_cast<UInt32>(TargetArch::x86_64_v2);
    }

    // x86-64-v3: v2 + AVX, AVX2, BMI1, BMI2, F16C, FMA, LZCNT, MOVBE
    // x86-64 levels are cumulative, so v3 requires v2
    if ((result & static_cast<UInt32>(TargetArch::x86_64_v2)) == static_cast<UInt32>(TargetArch::x86_64_v2)
        && CPU::haveAVX()
        && CPU::haveAVX2()
        && CPU::haveBMI1()
        && CPU::haveBMI2()
        && CPU::haveF16C()
        && CPU::haveFMA()
        && CPU::haveLZCNT()
        && CPU::haveMOVBE())
    {
        result |= static_cast<UInt32>(TargetArch::x86_64_v3);
    }

    // x86-64-v4: v3 + AVX512F, AVX512BW, AVX512CD, AVX512DQ, AVX512VL
    // x86-64 levels are cumulative, so v4 requires v3
    if ((result & static_cast<UInt32>(TargetArch::x86_64_v3)) == static_cast<UInt32>(TargetArch::x86_64_v3)
        && CPU::haveAVX512F()
        && CPU::haveAVX512BW()
        && CPU::haveAVX512CD()
        && CPU::haveAVX512DQ()
        && CPU::haveAVX512VL())
    {
        result |= static_cast<UInt32>(TargetArch::x86_64_v4);
    }

    // Ice Lake: v4 + AVX512VBMI, AVX512VBMI2, AVX512IFMA, AVX512VNNI, AVX512VPOPCNTDQ, AVX512BITALG, GFNI, VAES, VPCLMULQDQ
    if ((result & static_cast<UInt32>(TargetArch::x86_64_v4)) == static_cast<UInt32>(TargetArch::x86_64_v4)
        && CPU::haveAVX512VBMI()
        && CPU::haveAVX512VBMI2()
        && CPU::haveAVX512IFMA()
        && CPU::haveAVX512VNNI()
        && CPU::haveAVX512VPOPCNTDQ()
        && CPU::haveAVX512BITALG()
        && CPU::haveGFNI()
        && CPU::haveVAES()
        && CPU::haveVPCLMULQDQ())
    {
        result |= static_cast<UInt32>(TargetArch::x86_64_icelake);
    }

    // Sapphire Rapids: Ice Lake + AVX512BF16, AVX512FP16, AMX-BF16, AMX-INT8, AMX-TILE, AVXVNNI
    if ((result & static_cast<UInt32>(TargetArch::x86_64_icelake)) == static_cast<UInt32>(TargetArch::x86_64_icelake)
        && CPU::haveAVX512BF16()
        && CPU::haveAVX512FP16()
        && CPU::haveAVXVNNI()
        && CPU::haveAMXBF16()
        && CPU::haveAMXTILE()
        && CPU::haveAMXINT8())
    {
        result |= static_cast<UInt32>(TargetArch::x86_64_sapphirerapids);
    }

    // VAES: v3 + VAES. Kept separate from the levels above because the CPUs that have it do not line
    // up with any of them: Zen 3 has VAES and no AVX-512, Intel has it only from Ice Lake onwards.
    if ((result & static_cast<UInt32>(TargetArch::x86_64_v3)) == static_cast<UInt32>(TargetArch::x86_64_v3)
        && CPU::haveVAES())
    {
        result |= static_cast<UInt32>(TargetArch::x86_64_vaes);
    }

    // CPU vendor detection
    if (CPU::haveGenuineIntel())
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
        case TargetArch::x86_64_vaes:           return "x86-64-vaes";
    }

    // This should never be reached. If it is, someone added a new TargetArch
    // value but forgot to add a case above. The compiler should warn about this.
    throw std::logic_error("Unknown TargetArch value: " + std::to_string(static_cast<UInt32>(arch)));
}

}
