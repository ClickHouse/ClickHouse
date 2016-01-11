/**************************  instrset_detect.cpp   ****************************
| Author:        Agner Fog
| Date created:  2012-05-30
| Last modified: 2014-07-23
| Version:       1.14
| Project:       vector classes
| Description:
| Functions for checking which instruction sets are supported.
|
| (c) Copyright 2012 - 2014 GNU General Public License http://www.gnu.org/licenses
\*****************************************************************************/

#include "instrset.h"

// Define interface to cpuid instruction.
// input:  eax = functionnumber, ecx = 0
// output: eax = output[0], ebx = output[1], ecx = output[2], edx = output[3]
static inline void cpuid (int output[4], int functionnumber) {	
#if defined (_MSC_VER) || defined (__INTEL_COMPILER)       // Microsoft or Intel compiler, intrin.h included

    __cpuidex(output, functionnumber, 0);                  // intrinsic function for CPUID

#elif defined(__GNUC__) || defined(__clang__)              // use inline assembly, Gnu/AT&T syntax

   int a, b, c, d;
   __asm("cpuid" : "=a"(a),"=b"(b),"=c"(c),"=d"(d) : "a"(functionnumber),"c"(0) : );
   output[0] = a;
   output[1] = b;
   output[2] = c;
   output[3] = d;

#else                                                      // unknown platform. try inline assembly with masm/intel syntax

    __asm {
        mov eax, functionnumber
        xor ecx, ecx
        cpuid;
        mov esi, output
        mov [esi],    eax
        mov [esi+4],  ebx
        mov [esi+8],  ecx
        mov [esi+12], edx
    }

#endif
}

// Define interface to xgetbv instruction
static inline int64_t xgetbv (int ctr) {	
#if (defined (_MSC_FULL_VER) && _MSC_FULL_VER >= 160040000) || (defined (__INTEL_COMPILER) && __INTEL_COMPILER >= 1200) // Microsoft or Intel compiler supporting _xgetbv intrinsic

    return _xgetbv(ctr);                                   // intrinsic function for XGETBV

#elif defined(__GNUC__)                                    // use inline assembly, Gnu/AT&T syntax

   uint32_t a, d;
   __asm("xgetbv" : "=a"(a),"=d"(d) : "c"(ctr) : );
   return a | (uint64_t(d) << 32);

#else  // #elif defined (_WIN32)                           // other compiler. try inline assembly with masm/intel/MS syntax

   uint32_t a, d;
    __asm {
        mov ecx, ctr
        _emit 0x0f
        _emit 0x01
        _emit 0xd0 ; // xgetbv
        mov a, eax
        mov d, edx
    }
   return a | (uint64_t(d) << 32);

#endif
}


/* find supported instruction set
    return value:
    0           = 80386 instruction set
    1  or above = SSE (XMM) supported by CPU (not testing for O.S. support)
    2  or above = SSE2
    3  or above = SSE3
    4  or above = Supplementary SSE3 (SSSE3)
    5  or above = SSE4.1
    6  or above = SSE4.2
    7  or above = AVX supported by CPU and operating system
    8  or above = AVX2
    9  or above = AVX512F
*/
int instrset_detect(void) {

    static int iset = -1;                                  // remember value for next call
    if (iset >= 0) {
        return iset;                                       // called before
    }
    iset = 0;                                              // default value
    int abcd[4] = {0,0,0,0};                               // cpuid results
    cpuid(abcd, 0);                                        // call cpuid function 0
    if (abcd[0] == 0) return iset;                         // no further cpuid function supported
    cpuid(abcd, 1);                                        // call cpuid function 1 for feature flags
    if ((abcd[3] & (1 <<  0)) == 0) return iset;           // no floating point
    if ((abcd[3] & (1 << 23)) == 0) return iset;           // no MMX
    if ((abcd[3] & (1 << 15)) == 0) return iset;           // no conditional move
    if ((abcd[3] & (1 << 24)) == 0) return iset;           // no FXSAVE
    if ((abcd[3] & (1 << 25)) == 0) return iset;           // no SSE
    iset = 1;                                              // 1: SSE supported
    if ((abcd[3] & (1 << 26)) == 0) return iset;           // no SSE2
    iset = 2;                                              // 2: SSE2 supported
    if ((abcd[2] & (1 <<  0)) == 0) return iset;           // no SSE3
    iset = 3;                                              // 3: SSE3 supported
    if ((abcd[2] & (1 <<  9)) == 0) return iset;           // no SSSE3
    iset = 4;                                              // 4: SSSE3 supported
    if ((abcd[2] & (1 << 19)) == 0) return iset;           // no SSE4.1
    iset = 5;                                              // 5: SSE4.1 supported
    if ((abcd[2] & (1 << 23)) == 0) return iset;           // no POPCNT
    if ((abcd[2] & (1 << 20)) == 0) return iset;           // no SSE4.2
    iset = 6;                                              // 6: SSE4.2 supported
    if ((abcd[2] & (1 << 27)) == 0) return iset;           // no OSXSAVE
    if ((xgetbv(0) & 6) != 6)       return iset;           // AVX not enabled in O.S.
    if ((abcd[2] & (1 << 28)) == 0) return iset;           // no AVX
    iset = 7;                                              // 7: AVX supported
    cpuid(abcd, 7);                                        // call cpuid leaf 7 for feature flags
    if ((abcd[1] & (1 <<  5)) == 0) return iset;           // no AVX2
    iset = 8;                                              // 8: AVX2 supported
    cpuid(abcd, 0xD);                                      // call cpuid leaf 0xD for feature flags
    if ((abcd[0] & 0x60) != 0x60)   return iset;           // no AVX512
    iset = 9;                                              // 8: AVX512F supported
    return iset;
}

// detect if CPU supports the FMA3 instruction set
bool hasFMA3(void) {
    if (instrset_detect() < 7) return false;               // must have AVX
    int abcd[4];                                           // cpuid results
    cpuid(abcd, 1);                                        // call cpuid function 1
    return ((abcd[2] & (1 << 12)) != 0);                   // ecx bit 12 indicates FMA3
}

// detect if CPU supports the FMA4 instruction set
bool hasFMA4(void) {
    if (instrset_detect() < 7) return false;               // must have AVX
    int abcd[4];                                           // cpuid results
    cpuid(abcd, 0x80000001);                               // call cpuid function 0x80000001
    return ((abcd[2] & (1 << 16)) != 0);                   // ecx bit 16 indicates FMA4
}

// detect if CPU supports the XOP instruction set
bool hasXOP(void) {
    if (instrset_detect() < 7) return false;               // must have AVX
    int abcd[4];                                           // cpuid results
    cpuid(abcd, 0x80000001);                               // call cpuid function 0x80000001
    return ((abcd[2] & (1 << 11)) != 0);                   // ecx bit 11 indicates XOP
}
