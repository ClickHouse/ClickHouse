#pragma once
#include <cstddef>
#include <utility>
#include <microarchitecture/internal/x86_64.config.h>
#if !defined(__APPLE__) && defined(__GNUC__) && __has_include(<cpuid.h>)
#    include <cpuid.h>
#    define MICROARCHITECTURE_FEATURE_DETECTABLE
#endif

#ifdef __has_builtin
#    define MICROARCHITECTURE_HAS_BUILTIN(X) (__has_builtin(X))
#else
#    define MICROARCHITECTURE_HAS_BUILTIN(X) (false)
#endif

#ifndef MICROARCHITECTURE_X86_64_IFUNC_SUPPORT
#    define MICROARCHITECTURE_X86_64_IFUNC_SUPPORT 0
#endif

namespace MicroArchitecture
{
/**
  * @brief Detects ERMS extension using CPUID. This is not available from LIBCPUID.
  *
  * @return true if GNU CPUID utility is ready and erms is support.
  * @return false if either GNU CPUID utility is not found or erms is not supported.
  */
static inline bool runtimeHasERMS()
{
#ifdef MICROARCHITECTURE_FEATURE_DETECTABLE
    constexpr unsigned int erms_bit_mask = 1u << 9u;
    unsigned int eax, ebx, ecx, edx;
    if (__get_cpuid_max(0, nullptr) >= 7)
    {
        __cpuid_count(7, 0, eax, ebx, ecx, edx);
        return (ebx & erms_bit_mask) != 0;
    }
#endif
    return false;
}

/**
  * @brief Detects x86-64-v3 using GNU extension if it is available.
  */
static inline bool runtimeHasV3Support()
{
#if defined(MICROARCHITECTURE_FEATURE_DETECTABLE) && MICROARCHITECTURE_HAS_BUILTIN(__builtin_cpu_supports)
    return __builtin_cpu_supports("avx") && __builtin_cpu_supports("avx2") && __builtin_cpu_supports("fma") && __builtin_cpu_supports("bmi")
        && __builtin_cpu_supports("bmi2");
#endif
    return false;
}

/**
  * @brief Detects x86-64-v4 using GNU extension if it is available.
  */
static inline bool runtimeHasV4Support()
{
#if defined(MICROARCHITECTURE_FEATURE_DETECTABLE) && MICROARCHITECTURE_HAS_BUILTIN(__builtin_cpu_supports)
    return runtimeHasV3Support() && __builtin_cpu_supports("avx512f") && __builtin_cpu_supports("avx512vl")
        && __builtin_cpu_supports("avx512bw") && __builtin_cpu_supports("avx512cd") && __builtin_cpu_supports("avx512dq");
#endif
    return false;
}

/**
 * @brief Move memory using REP MOVSB instruction. This can yield better performance than
 * memcpy when copying thousands of bytes if ERMS is available. REP MOVSB supports
 * both overlapped and nonoverlapped memory ranges.
 *
 * @param dst destination of copy
 * @param src source of copy
 * @param size number of bytes
 *
 */
static inline void repMovsb(void * dst, const void * src, size_t size)
{
    asm volatile("rep movsb" : "+D"(dst), "+S"(src), "+c"(size) : : "memory");
}

#ifdef MICROARCHITECTURE_X86_64_V3_ATTRIBUTE
#    define MICROARCHITECTURE_X86_64_V3_CODE(...) __VA_ARGS__
#else
#    define MICROARCHITECTURE_X86_64_V3_CODE(...)
#endif

#ifdef MICROARCHITECTURE_X86_64_V4_ATTRIBUTE
#    define MICROARCHITECTURE_X86_64_V4_CODE(...) __VA_ARGS__
#else
#    define MICROARCHITECTURE_X86_64_V4_CODE(...)
#endif

#if MICROARCHITECTURE_X86_64_IFUNC_SUPPORT
#    define MICROARCHITECTURE_IFUNC_RESOLVER_IMPL(X) __attribute__((ifunc(#    X)))
#    define MICROARCHITECTURE_IFUNC_RESOLVER(X) MICROARCHITECTURE_IFUNC_RESOLVER_IMPL(X)
/**
 * @brief Define a function that can be dispatched using IFUNC strategy.
 * (IFunc is a gnu loader extension: https://sourceware.org/glibc/wiki/GNU_IFUNC)
 * On x86_64, we actually do not need to specify IFUNC attribute, modern gcc and
 * clang supports function multiversioning:
 * https://gcc.gnu.org/onlinedocs/gcc/Function-Multiversioning.html.
 *
 * However, it seems that LLVM's LTO is buggy at handling ifunc. We need to manually
 * expand ifunc implementation and hint the compiler that the symbols are all alive.
 */
#    define MICROARCHITECTURE_DISPATCHED(RETURN_TYPE, NAME, ARG_LIST, BODY) \
        namespace MicroArchitecture::IFunc::NAME \
        { \
            __attribute__((visibility("default"))) RETURN_TYPE dispatchDefault ARG_LIST BODY; \
            MICROARCHITECTURE_X86_64_V3_CODE(__attribute__((visibility("default"))) \
                                             MICROARCHITECTURE_X86_64_V3_ATTRIBUTE RETURN_TYPE dispatchV3 ARG_LIST BODY); \
            MICROARCHITECTURE_X86_64_V4_CODE(__attribute__((visibility("default"))) \
                                             MICROARCHITECTURE_X86_64_V4_ATTRIBUTE RETURN_TYPE dispatchV4 ARG_LIST BODY); \
            __attribute__((used, visibility("default"))) extern "C" decltype(dispatchDefault) * NAME##MicroarchitectureIFuncResolver() \
            { \
                MICROARCHITECTURE_X86_64_V4_CODE(if (::MicroArchitecture::runtimeHasV4Support()) return &dispatchV4;) \
                MICROARCHITECTURE_X86_64_V3_CODE(if (::MicroArchitecture::runtimeHasV3Support()) return &dispatchV3;) \
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
enum class X86_64Level
{
    Default,
    V3,
    V4,
};
static const inline X86_64Level DISTPACHED_LEVEL
    = runtimeHasV4Support() ? X86_64Level::V4 : (runtimeHasV3Support() ? X86_64Level::V3 : X86_64Level::Default);
/**
 * @brief Define a function that can be dispatched in runtime.
 * A few of extra branches will be introduced but it should be acceptable if the operation body is relative large.
 */
#    define MICROARCHITECTURE_DISPATCHED(RETURN_TYPE, NAME, ARG_LIST, BODY) \
        namespace MicroArchitecture::RuntimeDispatch::NAME \
        { \
            static inline RETURN_TYPE dispatchDefault ARG_LIST BODY; \
            MICROARCHITECTURE_X86_64_V3_CODE(MICROARCHITECTURE_X86_64_V3_ATTRIBUTE static inline RETURN_TYPE dispatchV3 ARG_LIST BODY); \
            MICROARCHITECTURE_X86_64_V4_CODE(MICROARCHITECTURE_X86_64_V4_ATTRIBUTE static inline RETURN_TYPE dispatchV4 ARG_LIST BODY); \
        } \
        template <typename... Args> \
        static inline RETURN_TYPE NAME(Args &&... args) \
        { \
            switch (::MicroArchitecture::DISTPACHED_LEVEL) \
            { \
                MICROARCHITECTURE_X86_64_V4_CODE( \
                    case ::MicroArchitecture::X86_64Level::V4 \
                    : return MicroArchitecture::RuntimeDispatch::NAME::dispatchV4(::std::forward<Args>(args)...);) \
                MICROARCHITECTURE_X86_64_V3_CODE( \
                    case ::MicroArchitecture::X86_64Level::V3 \
                    : return MicroArchitecture::RuntimeDispatch::NAME::dispatchV3(::std::forward<Args>(args)...);) \
                default: \
                    return MicroArchitecture::RuntimeDispatch::NAME::dispatchDefault(::std::forward<Args>(args)...); \
            } \
        }
#endif
}
