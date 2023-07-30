#pragma once

#include <base/types.h>

#if defined(__x86_64__) || defined(__i386__)
#include <cpuid.h>
#endif

#include <cstring>


namespace DB
{
namespace Cpu
{

#if (defined(__x86_64__) || defined(__i386__))
/// Our version is independent of -mxsave option, because we do dynamic dispatch.
inline UInt64 our_xgetbv(UInt32 xcr) noexcept
{
    UInt32 eax;
    UInt32 edx;
    __asm__ volatile(
        "xgetbv"
        : "=a"(eax), "=d"(edx)
        : "c"(xcr));
    return (static_cast<UInt64>(edx) << 32) | eax;
}
#endif

inline bool cpuid(UInt32 op, UInt32 sub_op, UInt32 * res) noexcept /// NOLINT
{
#if defined(__x86_64__) || defined(__i386__)
    __cpuid_count(op, sub_op, res[0], res[1], res[2], res[3]);
    return true;
#else
    (void)op;
    (void)sub_op;

    memset(res, 0, 4 * sizeof(*res));

    return false;
#endif
}

inline bool cpuid(UInt32 op, UInt32 * res) noexcept /// NOLINT
{
#if defined(__x86_64__) || defined(__i386__)
    __cpuid(op, res[0], res[1], res[2], res[3]);
    return true;
#else
    (void)op;

    memset(res, 0, 4 * sizeof(*res));

    return false;
#endif
}

#define CPU_ID_ENUMERATE(OP) \
    OP(SSE)                  \
    OP(SSE2)                 \
    OP(SSE3)                 \
    OP(SSSE3)                \
    OP(SSE41)                \
    OP(SSE42)                \
    OP(F16C)                 \
    OP(POPCNT)               \
    OP(BMI1)                 \
    OP(BMI2)                 \
    OP(PCLMUL)               \
    OP(AES)                  \
    OP(AVX)                  \
    OP(FMA)                  \
    OP(AVX2)                 \
    OP(AVX512F)              \
    OP(AVX512DQ)             \
    OP(AVX512IFMA)           \
    OP(AVX512PF)             \
    OP(AVX512ER)             \
    OP(AVX512CD)             \
    OP(AVX512BW)             \
    OP(AVX512VL)             \
    OP(AVX512VBMI)           \
    OP(AVX512VBMI2)          \
    OP(PREFETCHWT1)          \
    OP(SHA)                  \
    OP(ADX)                  \
    OP(RDRAND)               \
    OP(RDSEED)               \
    OP(PCOMMIT)              \
    OP(RDTSCP)               \
    OP(CLFLUSHOPT)           \
    OP(CLWB)                 \
    OP(XSAVE)                \
    OP(OSXSAVE)

union CpuInfo
{
    UInt32 info[4];

    struct Registers
    {
        UInt32 eax;
        UInt32 ebx;
        UInt32 ecx;
        UInt32 edx;
    } registers;

    inline explicit CpuInfo(UInt32 op) noexcept { cpuid(op, info); }

    inline CpuInfo(UInt32 op, UInt32 sub_op) noexcept { cpuid(op, sub_op, info); }
};

#define DEF_NAME(X) inline bool have##X() noexcept;
    CPU_ID_ENUMERATE(DEF_NAME)
#undef DEF_NAME

bool haveRDTSCP() noexcept
{
    return (CpuInfo(0x80000001).registers.edx >> 27) & 1u;
}

bool haveSSE() noexcept
{
    return (CpuInfo(0x1).registers.edx >> 25) & 1u;
}

bool haveSSE2() noexcept
{
    return (CpuInfo(0x1).registers.edx >> 26) & 1u;
}

bool haveSSE3() noexcept
{
    return CpuInfo(0x1).registers.ecx & 1u;
}

bool havePCLMUL() noexcept
{
    return (CpuInfo(0x1).registers.ecx >> 1) & 1u;
}

bool haveSSSE3() noexcept
{
    return (CpuInfo(0x1).registers.ecx >> 9) & 1u;
}

bool haveSSE41() noexcept
{
    return (CpuInfo(0x1).registers.ecx >> 19) & 1u;
}

bool haveSSE42() noexcept
{
    return (CpuInfo(0x1).registers.ecx >> 20) & 1u;
}

bool haveF16C() noexcept
{
    return (CpuInfo(0x1).registers.ecx >> 29) & 1u;
}

bool havePOPCNT() noexcept
{
    return (CpuInfo(0x1).registers.ecx >> 23) & 1u;
}

bool haveAES() noexcept
{
    return (CpuInfo(0x1).registers.ecx >> 25) & 1u;
}

bool haveXSAVE() noexcept
{
    return (CpuInfo(0x1).registers.ecx >> 26) & 1u;
}

bool haveOSXSAVE() noexcept
{
    return (CpuInfo(0x1).registers.ecx >> 27) & 1u;
}

bool haveAVX() noexcept
{
#if defined(__x86_64__) || defined(__i386__)
    // http://www.intel.com/content/dam/www/public/us/en/documents/manuals/64-ia-32-architectures-optimization-manual.pdf
    // https://bugs.chromium.org/p/chromium/issues/detail?id=375968
    return haveOSXSAVE()                           // implies haveXSAVE()
           && (our_xgetbv(0) & 6u) == 6u              // XMM state and YMM state are enabled by OS
           && ((CpuInfo(0x1).registers.ecx >> 28) & 1u); // AVX bit
#else
    return false;
#endif
}

bool haveFMA() noexcept
{
    return haveAVX() && ((CpuInfo(0x1).registers.ecx >> 12) & 1u);
}

bool haveAVX2() noexcept
{
    return haveAVX() && ((CpuInfo(0x7, 0).registers.ebx >> 5) & 1u);
}

bool haveBMI1() noexcept
{
    return (CpuInfo(0x7, 0).registers.ebx >> 3) & 1u;
}

bool haveBMI2() noexcept
{
    return (CpuInfo(0x7, 0).registers.ebx >> 8) & 1u;
}

bool haveAVX512F() noexcept
{
#if defined(__x86_64__) || defined(__i386__)
    // https://software.intel.com/en-us/articles/how-to-detect-knl-instruction-support
    return haveOSXSAVE()                           // implies haveXSAVE()
           && (our_xgetbv(0) & 6u) == 6u              // XMM state and YMM state are enabled by OS
           && ((our_xgetbv(0) >> 5) & 7u) == 7u       // ZMM state is enabled by OS
           && CpuInfo(0x0).registers.eax >= 0x7          // leaf 7 is present
           && ((CpuInfo(0x7, 0).registers.ebx >> 16) & 1u); // AVX512F bit
#else
    return false;
#endif
}

bool haveAVX512DQ() noexcept
{
    return haveAVX512F() && ((CpuInfo(0x7, 0).registers.ebx >> 17) & 1u);
}

bool haveRDSEED() noexcept
{
    return CpuInfo(0x0).registers.eax >= 0x7 && ((CpuInfo(0x7, 0).registers.ebx >> 18) & 1u);
}

bool haveADX() noexcept
{
    return CpuInfo(0x0).registers.eax >= 0x7 && ((CpuInfo(0x7, 0).registers.ebx >> 19) & 1u);
}

bool haveAVX512IFMA() noexcept
{
    return haveAVX512F() && ((CpuInfo(0x7, 0).registers.ebx >> 21) & 1u);
}

bool havePCOMMIT() noexcept
{
    return CpuInfo(0x0).registers.eax >= 0x7 && ((CpuInfo(0x7, 0).registers.ebx >> 22) & 1u);
}

bool haveCLFLUSHOPT() noexcept
{
    return CpuInfo(0x0).registers.eax >= 0x7 && ((CpuInfo(0x7, 0).registers.ebx >> 23) & 1u);
}

bool haveCLWB() noexcept
{
    return CpuInfo(0x0).registers.eax >= 0x7 && ((CpuInfo(0x7, 0).registers.ebx >> 24) & 1u);
}

bool haveAVX512PF() noexcept
{
    return haveAVX512F() && ((CpuInfo(0x7, 0).registers.ebx >> 26) & 1u);
}

bool haveAVX512ER() noexcept
{
    return haveAVX512F() && ((CpuInfo(0x7, 0).registers.ebx >> 27) & 1u);
}

bool haveAVX512CD() noexcept
{
    return haveAVX512F() && ((CpuInfo(0x7, 0).registers.ebx >> 28) & 1u);
}

bool haveSHA() noexcept
{
    return CpuInfo(0x0).registers.eax >= 0x7 && ((CpuInfo(0x7, 0).registers.ebx >> 29) & 1u);
}

bool haveAVX512BW() noexcept
{
    return haveAVX512F() && ((CpuInfo(0x7, 0).registers.ebx >> 30) & 1u);
}

bool haveAVX512VL() noexcept
{
    return haveAVX512F() && ((CpuInfo(0x7, 0).registers.ebx >> 31) & 1u);
}

bool havePREFETCHWT1() noexcept
{
    return CpuInfo(0x0).registers.eax >= 0x7 && ((CpuInfo(0x7, 0).registers.ecx >> 0) & 1u);
}

bool haveAVX512VBMI() noexcept
{
    return haveAVX512F() && ((CpuInfo(0x7, 0).registers.ecx >> 1) & 1u);
}

bool haveAVX512VBMI2() noexcept
{
    return haveAVX512F() && ((CpuInfo(0x7, 0).registers.ecx >> 6) & 1u);
}

bool haveRDRAND() noexcept
{
    return CpuInfo(0x0).registers.eax >= 0x7 && ((CpuInfo(0x1).registers.ecx >> 30) & 1u);
}

struct CpuFlagsCache
{
#define DEF_NAME(X) static inline bool have_##X = have##X();
    CPU_ID_ENUMERATE(DEF_NAME)
#undef DEF_NAME
};

}
}

