// Runtime dispatch for llvm-libc math functions (x86_64 only).
// Selects between x86-64-v2 (SSE4.2) and x86-64-v3 (AVX/FMA) variants based on CPU capabilities.

#if defined(__x86_64__)

#include <Common/CPUID.h>

namespace {
    __attribute__((always_inline))
    inline bool cpu_has_fma()
    {
        static const bool has_fma = DB::CPU::haveFMA();
        return has_fma;
    }
}


#define DISPATCH_1(ret, name, T1) \
    namespace __llvm_libc__x86_64_v3 { ret name(T1); } \
    namespace __llvm_libc__x86_64_v2 { ret name(T1); } \
    extern "C" ret name(T1 a1) { \
        return cpu_has_fma() ? __llvm_libc__x86_64_v3::name(a1) \
                             : __llvm_libc__x86_64_v2::name(a1); \
    }

#define DISPATCH_2(ret, name, T1, T2) \
    namespace __llvm_libc__x86_64_v3 { ret name(T1, T2); } \
    namespace __llvm_libc__x86_64_v2 { ret name(T1, T2); } \
    extern "C" ret name(T1 a1, T2 a2) { \
        return cpu_has_fma() ? __llvm_libc__x86_64_v3::name(a1, a2) \
                             : __llvm_libc__x86_64_v2::name(a1, a2); \
    }

#define DISPATCH_3(ret, name, T1, T2, T3) \
    namespace __llvm_libc__x86_64_v3 { ret name(T1, T2, T3); } \
    namespace __llvm_libc__x86_64_v2 { ret name(T1, T2, T3); } \
    extern "C" ret name(T1 a1, T2 a2, T3 a3) { \
        return cpu_has_fma() ? __llvm_libc__x86_64_v3::name(a1, a2, a3) \
                             : __llvm_libc__x86_64_v2::name(a1, a2, a3); \
    }

#define DISPATCH_2_PTR(ret, name, T1, T2) \
    namespace __llvm_libc__x86_64_v3 { ret name(T1, T2*); } \
    namespace __llvm_libc__x86_64_v2 { ret name(T1, T2*); } \
    extern "C" ret name(T1 a1, T2 *a2) { \
        return cpu_has_fma() ? __llvm_libc__x86_64_v3::name(a1, a2) \
                             : __llvm_libc__x86_64_v2::name(a1, a2); \
    }


DISPATCH_1(double, acos, double)
DISPATCH_1(double, asin, double)
DISPATCH_1(double, atan, double)
DISPATCH_1(double, cbrt, double)
DISPATCH_1(double, cos, double)
DISPATCH_1(double, log10, double)
DISPATCH_1(double, log1p, double)
DISPATCH_1(double, sin, double)
DISPATCH_1(double, sqrt, double)
DISPATCH_1(double, tan, double)

DISPATCH_1(float, asinf, float)
DISPATCH_1(float, sqrtf, float)

DISPATCH_1(long double, ceill, long double)
DISPATCH_1(long double, floorl, long double)
DISPATCH_1(long double, sqrtl, long double)
DISPATCH_1(long double, truncl, long double)

DISPATCH_1(long, lround, double)
DISPATCH_1(long long, llround, double)

DISPATCH_2(double, atan2, double, double)
DISPATCH_2(double, fmod, double, double)
DISPATCH_2(double, hypot, double, double)
DISPATCH_2(double, nextafter, double, double)

DISPATCH_2(double, ldexp, double, int)
DISPATCH_2(float, ldexpf, float, int)
DISPATCH_2(long double, ldexpl, long double, int)

DISPATCH_3(double, fma, double, double, double)

DISPATCH_2_PTR(double, frexp, double, int)
DISPATCH_2_PTR(long double, frexpl, long double, int)
DISPATCH_2_PTR(double, modf, double, double)

#endif // __x86_64__
