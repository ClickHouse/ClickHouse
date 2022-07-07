#pragma once
#include <cstddef>
#include <cstdint>

#include <microarchitecture/internal/aarch64.config.h>
#if defined(__linux__)
#    define MICROARCHITECTURE_FEATURE_DETECTABLE
#    include <fcntl.h>
#    include <unistd.h>
#    include <sys/auxv.h>
#    include <sys/syscall.h>

#    ifndef HWCAP2_SVE2
#        define HWCAP2_SVE2 (1 << 1)
#    endif

#    ifndef HWCAP_SVE
#        define HWCAP_SVE (1 << 22)
#    endif

#    ifndef HWCAP_CPUID
#        define HWCAP_CPUID (1 << 11)
#    endif

#    ifndef AT_HWCAP2
#        define AT_HWCAP2 26
#    endif

#    ifndef AT_HWCAP
#        define AT_HWCAP 16
#    endif
#endif

#ifndef MICROARCHITECTURE_AARCH64_IFUNC_SUPPORT
#    define MICROARCHITECTURE_AARCH64_IFUNC_SUPPORT 0
#endif

namespace MicroArchitecture
{
// the following runtimeHasXXX series function is defined
// to be standalone, they may be referenced from IFUNC's
// resolvers, where we even do not have C-runtime, not
// mentioning about C++-runtime.

// `getauxval` maybe overwritten to a non primitive version.
// Therefore, we manually iterate through `/proc/self/auxv`
// to get the values.
#ifdef MICROARCHITECTURE_FEATURE_DETECTABLE

static inline long syscall1(long n, long a)
{
    register long x8 __asm__("x8") = n;
    register long x0 __asm__("x0") = a;
    asm volatile("svc 0" : "=r"(x0) : "r"(x8), "0"(x0) : "memory", "cc");
    return x0;
}

static inline long syscall3(long n, long a, long b, long c)
{
    register long x8 __asm__("x8") = n;
    register long x0 __asm__("x0") = a;
    register long x1 __asm__("x1") = b;
    register long x2 __asm__("x2") = c;
    asm volatile("svc 0" : "=r"(x0) : "r"(x8), "0"(x0), "r"(x1), "r"(x2) : "memory", "cc");
    return x0;
}

static inline long syscall4(long n, long a, long b, long c, long d)
{
    register long x8 __asm__("x8") = n;
    register long x0 __asm__("x0") = a;
    register long x1 __asm__("x1") = b;
    register long x2 __asm__("x2") = c;
    register long x3 __asm__("x3") = d;
    asm volatile("svc 0" : "=r"(x0) : "r"(x8), "0"(x0), "r"(x1), "r"(x2), "r"(x3) : "memory", "cc");
    return x0;
}

static inline unsigned long iterateAuxv(unsigned long target)
{
    unsigned long entry[2] = {0, 0};
    unsigned long result = 0;
    char filename[] = "/proc/self/auxv";
    auto fd = syscall4(SYS_openat, 0, reinterpret_cast<long>(&filename[0]), O_RDONLY | O_CLOEXEC, 0);
    if (fd > 0)
    {
        while (true)
        {
            auto size = syscall3(SYS_read, fd, reinterpret_cast<long>(&entry[0]), static_cast<long>(sizeof(entry)));
            if (size != sizeof(entry) || entry[0] == 0)
            {
                break;
            }
            if (entry[0] == target)
            {
                result = entry[1];
                break;
            }
        }
        syscall1(SYS_close, fd);
    }
    return result;
}
#endif

static inline bool runtimeHasSVE2()
{
#ifdef MICROARCHITECTURE_FEATURE_DETECTABLE
    auto hwcaps = iterateAuxv(AT_HWCAP2);
    return (hwcaps & HWCAP2_SVE2) != 0;
#else
    return false;
#endif
}

static inline bool runtimeHasSVE()
{
#ifdef MICROARCHITECTURE_FEATURE_DETECTABLE
    auto hwcaps = iterateAuxv(AT_HWCAP);
    return (hwcaps & HWCAP_SVE) != 0;
#else
    return false;
#endif
}

static inline bool runtimeHasCPUID()
{
#ifdef MICROARCHITECTURE_FEATURE_DETECTABLE
    auto hwcaps = iterateAuxv(AT_HWCAP);
    return (hwcaps & HWCAP_CPUID) != 0;
#else
    return false;
#endif
}

// as described in Aarch64 manual, MOPS
// status is indicated by [16,19] bit range in ID_AA64ISAR2_EL1.
// The following usage is suggested in
// https://www.kernel.org/doc/html/latest/arm64/cpu-feature-registers.html
static inline bool runtimeHasMOPS()
{
#ifdef MICROARCHITECTURE_FEATURE_DETECTABLE
    if (!runtimeHasCPUID())
    {
        return false;
    }
    uint64_t id_reg;
    asm("mrs %0, ID_AA64ISAR2_EL1" : "=r"(id_reg));
    return (id_reg >> 16u & 0b1111u) == 0b0001u;
#else
    return false;
#endif
}

#ifdef MICROARCHITECTURE_AARCH64_MOPS_ATTRIBUTE
#    define MICROARCHITECTURE_AARCH64_MOPS_CODE(...) __VA_ARGS__
#else
#    define MICROARCHITECTURE_AARCH64_MOPS_CODE(...)
#endif

#ifdef MICROARCHITECTURE_AARCH64_SVE_ATTRIBUTE
#    define MICROARCHITECTURE_AARCH64_SVE_CODE(...) __VA_ARGS__
#else
#    define MICROARCHITECTURE_AARCH64_SVE_CODE(...)
#endif

#ifdef MICROARCHITECTURE_AARCH64_SVE2_ATTRIBUTE
#    define MICROARCHITECTURE_AARCH64_SVE2_CODE(...) __VA_ARGS__
#else
#    define MICROARCHITECTURE_AARCH64_SVE2_CODE(...)
#endif

/**
 * @brief Move memory using CPYF instruction set. LLVM 14+ is utilizing this method to generate
 * large sized __builtin_memcpy_inline. Notice that CPYF starts for memory copy forward: please
 * distinguish this behavior from REP MOVSB on x86-64.
 *
 * CPYFP performs some preconditioning of the arguments suitable for using the CPYFM instruction,
 * and performs an implementation defined amount of the memory copy. CPYFM performs an
 * implementation defined amount of the memory copy. CPYFE performs the last part of the memory
 * copy.
 *
 * @param dst destination of copy
 * @param src source of copy
 * @param size number of bytes
 *
 */
// clang-format off
MICROARCHITECTURE_AARCH64_MOPS_CODE(
    MICROARCHITECTURE_AARCH64_MOPS_ATTRIBUTE static inline void cpyf(void * dst, const void * src, size_t size)
    {
        asm volatile("cpyfp [%0]!, [%1]!, %2!" : "+r"(dst), "+r"(src), "+r"(size) : : "memory");
        asm volatile("cpyfm [%0]!, [%1]!, %2!" : "+r"(dst), "+r"(src), "+r"(size) : : "memory");
        asm volatile("cpyfe [%0]!, [%1]!, %2!" : "+r"(dst), "+r"(src), "+r"(size) : : "memory");
    })
// clang-format on

#if MICROARCHITECTURE_AARCH64_IFUNC_SUPPORT
#    define MICROARCHITECTURE_IFUNC_RESOLVER_IMPL(X) __attribute__((ifunc(#    X)))
#    define MICROARCHITECTURE_IFUNC_RESOLVER(X) MICROARCHITECTURE_IFUNC_RESOLVER_IMPL(X)
/**
 * @brief Define a function that can be dispatched using IFUNC strategy.
 * (IFunc is a gnu loader extension: https://sourceware.org/glibc/wiki/GNU_IFUNC)
 * On aarch64, function multiversioning is not implemented by compilers. Hence, we need to write
 * resolvers and ifunc declarations on ourselves.
 */
#    define MICROARCHITECTURE_DISPATCHED(RETURN_TYPE, NAME, ARG_LIST, BODY) \
        namespace MicroArchitecture::IFunc::NAME \
        { \
            __attribute__((visibility("default"))) RETURN_TYPE dispatchDefault ARG_LIST BODY; \
            MICROARCHITECTURE_AARCH64_SVE_CODE(__attribute__((visibility("default"))) \
                                               MICROARCHITECTURE_AARCH64_SVE_ATTRIBUTE RETURN_TYPE dispatchSVE ARG_LIST BODY); \
            MICROARCHITECTURE_AARCH64_SVE2_CODE(__attribute__((visibility("default"))) \
                                                MICROARCHITECTURE_AARCH64_SVE2_ATTRIBUTE RETURN_TYPE dispatchSVE2 ARG_LIST BODY); \
            __attribute__((used, visibility("default"))) extern "C" decltype(dispatchDefault) * NAME##MicroarchitectureIFuncResolver() \
            { \
                MICROARCHITECTURE_AARCH64_SVE2_CODE(if (::MicroArchitecture::runtimeHasSVE2()) return &dispatchSVE2;) \
                MICROARCHITECTURE_AARCH64_SVE_CODE(if (::MicroArchitecture::runtimeHasSVE()) return &dispatchSVE;) \
                return &dispatchDefault; \
            } \
            RETURN_TYPE dispatch ARG_LIST MICROARCHITECTURE_IFUNC_RESOLVER(NAME##MicroarchitectureIFuncResolver); \
        } \
        template <typename... Args> \
        static inline RETURN_TYPE NAME(Args &&... args) \
        { \
            return MicroArchitecture::IFunc::NAME::dispatch(::std::forward<Args>(args)...); \
        }
#else
enum class Aarch64VecVariant
{
    Default, /* ASIMD is within the default instruction set */
    SVE,
    SVE2,
};
static const inline Aarch64VecVariant DISTPACHED_VARIANT
    = runtimeHasSVE2() ? Aarch64VecVariant::SVE2 : (runtimeHasSVE() ? Aarch64VecVariant::SVE : Aarch64VecVariant::Default);
/**
 * @brief Define a function that can be dispatched in runtime.
 * A few of extra instructions to locate the function will be introduced but it should be acceptable if the operation body is relatively large.
 */
#    define MICROARCHITECTURE_DISPATCHED(RETURN_TYPE, NAME, ARG_LIST, BODY) \
        namespace MicroArchitecture::RuntimeDispatch::NAME \
        { \
            static inline RETURN_TYPE dispatchDefault ARG_LIST BODY; \
            MICROARCHITECTURE_AARCH64_SVE_CODE( \
                MICROARCHITECTURE_AARCH64_SVE_ATTRIBUTE static inline RETURN_TYPE dispatchSVE ARG_LIST BODY); \
            MICROARCHITECTURE_AARCH64_SVE2_CODE( \
                MICROARCHITECTURE_AARCH64_SVE2_ATTRIBUTE static inline RETURN_TYPE dispatchSVE2 ARG_LIST BODY); \
            static inline decltype(dispatchDefault) & getDispatchedFunction() \
            { \
                switch (::MicroArchitecture::DISTPACHED_VARIANT) \
                { \
                    MICROARCHITECTURE_AARCH64_SVE2_CODE(case ::MicroArchitecture::Aarch64VecVariant::SVE2 : return dispatchSVE2;) \
                    MICROARCHITECTURE_AARCH64_SVE_CODE(case ::MicroArchitecture::Aarch64VecVariant::SVE : return dispatchSVE;) \
                    default: \
                        return dispatchDefault; \
                } \
            } \
        } \
        static inline decltype(MicroArchitecture::RuntimeDispatch::NAME::dispatchDefault) & NAME \
            = MicroArchitecture::RuntimeDispatch::NAME::getDispatchedFunction();
#endif
}
