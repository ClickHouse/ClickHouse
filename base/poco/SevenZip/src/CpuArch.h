/* CpuArch.h -- CPU specific code
2010-12-01: Igor Pavlov : Public domain */

#ifndef __CPU_ARCH_H
#define __CPU_ARCH_H

#include "Types.h"

EXTERN_C_BEGIN

/*
MY_CPU_LE means that CPU is LITTLE ENDIAN.
If MY_CPU_LE is not defined, we don't know about that property of platform (it can be LITTLE ENDIAN).

MY_CPU_LE_UNALIGN means that CPU is LITTLE ENDIAN and CPU supports unaligned memory accesses.
If MY_CPU_LE_UNALIGN is not defined, we don't know about these properties of platform.
*/

#if defined(_M_X64) || defined(_M_AMD64) || defined(__x86_64__)
#define MY_CPU_AMD64
#endif

#if defined(MY_CPU_AMD64) || defined(_M_IA64)
#define MY_CPU_64BIT
#endif

#if defined(_M_IX86) || defined(__i386__)
#define MY_CPU_X86
#endif

#if defined(MY_CPU_X86) || defined(MY_CPU_AMD64)
#define MY_CPU_X86_OR_AMD64
#endif

#if defined(MY_CPU_X86) || defined(_M_ARM)
#define MY_CPU_32BIT
#endif

#if defined(_WIN32) && defined(_M_ARM)
#define MY_CPU_ARM_LE
#endif

#if defined(_WIN32) && defined(_M_IA64)
#define MY_CPU_IA64_LE
#endif

#if defined(MY_CPU_X86_OR_AMD64)
#define MY_CPU_LE_UNALIGN
#endif

#if defined(MY_CPU_X86_OR_AMD64) || defined(MY_CPU_ARM_LE)  || defined(MY_CPU_IA64_LE) || defined(__ARMEL__) || defined(__MIPSEL__) || defined(__LITTLE_ENDIAN__)
#define MY_CPU_LE
#endif

#if defined(__BIG_ENDIAN__) || defined(__m68k__) ||  defined(__ARMEB__) || defined(__MIPSEB__)
#define MY_CPU_BE
#endif

#if defined(MY_CPU_LE) && defined(MY_CPU_BE)
Stop_Compiling_Bad_Endian
#endif

#ifdef MY_CPU_LE_UNALIGN

#define GetUi16(p) (*(const UInt16 *)(p))
#define GetUi32(p) (*(const UInt32 *)(p))
#define GetUi64(p) (*(const UInt64 *)(p))
#define SetUi16(p, d) *(UInt16 *)(p) = (d);
#define SetUi32(p, d) *(UInt32 *)(p) = (d);
#define SetUi64(p, d) *(UInt64 *)(p) = (d);

#else

#define GetUi16(p) (((const Byte *)(p))[0] | ((UInt16)((const Byte *)(p))[1] << 8))

#define GetUi32(p) ( \
             ((const Byte *)(p))[0]        | \
    ((UInt32)((const Byte *)(p))[1] <<  8) | \
    ((UInt32)((const Byte *)(p))[2] << 16) | \
    ((UInt32)((const Byte *)(p))[3] << 24))

#define GetUi64(p) (GetUi32(p) | ((UInt64)GetUi32(((const Byte *)(p)) + 4) << 32))

#define SetUi16(p, d) { UInt32 _x_ = (d); \
    ((Byte *)(p))[0] = (Byte)_x_; \
    ((Byte *)(p))[1] = (Byte)(_x_ >> 8); }

#define SetUi32(p, d) { UInt32 _x_ = (d); \
    ((Byte *)(p))[0] = (Byte)_x_; \
    ((Byte *)(p))[1] = (Byte)(_x_ >> 8); \
    ((Byte *)(p))[2] = (Byte)(_x_ >> 16); \
    ((Byte *)(p))[3] = (Byte)(_x_ >> 24); }

#define SetUi64(p, d) { UInt64 _x64_ = (d); \
    SetUi32(p, (UInt32)_x64_); \
    SetUi32(((Byte *)(p)) + 4, (UInt32)(_x64_ >> 32)); }

#endif

#if defined(MY_CPU_LE_UNALIGN) && defined(_WIN64) && (_MSC_VER >= 1300)

#pragma intrinsic(_byteswap_ulong)
#pragma intrinsic(_byteswap_uint64)
#define GetBe32(p) _byteswap_ulong(*(const UInt32 *)(const Byte *)(p))
#define GetBe64(p) _byteswap_uint64(*(const UInt64 *)(const Byte *)(p))

#else

#define GetBe32(p) ( \
    ((UInt32)((const Byte *)(p))[0] << 24) | \
    ((UInt32)((const Byte *)(p))[1] << 16) | \
    ((UInt32)((const Byte *)(p))[2] <<  8) | \
             ((const Byte *)(p))[3] )

#define GetBe64(p) (((UInt64)GetBe32(p) << 32) | GetBe32(((const Byte *)(p)) + 4))

#endif

#define GetBe16(p) (((UInt16)((const Byte *)(p))[0] << 8) | ((const Byte *)(p))[1])


#ifdef MY_CPU_X86_OR_AMD64

typedef struct
{
  UInt32 maxFunc;
  UInt32 vendor[3];
  UInt32 ver;
  UInt32 b;
  UInt32 c;
  UInt32 d;
} Cx86cpuid;

enum
{
  CPU_FIRM_INTEL,
  CPU_FIRM_AMD,
  CPU_FIRM_VIA
};

Bool x86cpuid_CheckAndRead(Cx86cpuid *p);
int x86cpuid_GetFirm(const Cx86cpuid *p);

#define x86cpuid_GetFamily(p) (((p)->ver >> 8) & 0xFF00F)
#define x86cpuid_GetModel(p) (((p)->ver >> 4) & 0xF00F)
#define x86cpuid_GetStepping(p) ((p)->ver & 0xF)

Bool CPU_Is_InOrder();
Bool CPU_Is_Aes_Supported();

#endif

EXTERN_C_END

#endif
