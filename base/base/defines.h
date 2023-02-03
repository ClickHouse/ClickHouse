#pragma once

/// __has_feature supported only by clang.
///
/// But libcxx/libcxxabi overrides it to 0,
/// thus the checks for __has_feature will be wrong.
///
/// NOTE:
/// - __has_feature cannot be simply undefined,
///   since this will be broken if some C++ header will be included after
///   including <base/defines.h>
/// - it should not have fallback to 0,
///   since this may create false-positive detection (common problem)
#if defined(__clang__) && defined(__has_feature)
#    define ch_has_feature __has_feature
#endif

#if !defined(likely)
#    define likely(x)   (__builtin_expect(!!(x), 1))
#endif
#if !defined(unlikely)
#    define unlikely(x) (__builtin_expect(!!(x), 0))
#endif

// more aliases: https://mailman.videolan.org/pipermail/x264-devel/2014-May/010660.html

#define ALWAYS_INLINE __attribute__((__always_inline__))
#define NO_INLINE __attribute__((__noinline__))
#define MAY_ALIAS __attribute__((__may_alias__))

#if !defined(__x86_64__) && !defined(__aarch64__) && !defined(__PPC__) && !(defined(__riscv) && (__riscv_xlen == 64))
#    error "The only supported platforms are x86_64 and AArch64, PowerPC (work in progress) and RISC-V 64 (experimental)"
#endif

/// Check for presence of address sanitizer
#if !defined(ADDRESS_SANITIZER)
#    if defined(ch_has_feature)
#        if ch_has_feature(address_sanitizer)
#            define ADDRESS_SANITIZER 1
#        endif
#    elif defined(__SANITIZE_ADDRESS__)
#        define ADDRESS_SANITIZER 1
#    endif
#endif

#if !defined(THREAD_SANITIZER)
#    if defined(ch_has_feature)
#        if ch_has_feature(thread_sanitizer)
#            define THREAD_SANITIZER 1
#        endif
#    elif defined(__SANITIZE_THREAD__)
#        define THREAD_SANITIZER 1
#    endif
#endif

#if !defined(MEMORY_SANITIZER)
#    if defined(ch_has_feature)
#        if ch_has_feature(memory_sanitizer)
#            define MEMORY_SANITIZER 1
#        endif
#    elif defined(__MEMORY_SANITIZER__)
#        define MEMORY_SANITIZER 1
#    endif
#endif

#if !defined(UNDEFINED_BEHAVIOR_SANITIZER)
#    if defined(__has_feature)
#        if __has_feature(undefined_behavior_sanitizer)
#            define UNDEFINED_BEHAVIOR_SANITIZER 1
#        endif
#    elif defined(__UNDEFINED_BEHAVIOR_SANITIZER__)
#        define UNDEFINED_BEHAVIOR_SANITIZER 1
#    endif
#endif

#if defined(ADDRESS_SANITIZER)
#    define BOOST_USE_ASAN 1
#    define BOOST_USE_UCONTEXT 1
#endif

#if defined(THREAD_SANITIZER)
#    define BOOST_USE_TSAN 1
#    define BOOST_USE_UCONTEXT 1
#endif

/// TODO: Strange enough, there is no way to detect UB sanitizer.

/// Explicitly allow undefined behaviour for certain functions. Use it as a function attribute.
/// It is useful in case when compiler cannot see (and exploit) it, but UBSan can.
/// Example: multiplication of signed integers with possibility of overflow when both sides are from user input.
#if defined(__clang__)
#    define NO_SANITIZE_UNDEFINED __attribute__((__no_sanitize__("undefined")))
#    define NO_SANITIZE_ADDRESS __attribute__((__no_sanitize__("address")))
#    define NO_SANITIZE_THREAD __attribute__((__no_sanitize__("thread")))
#    define ALWAYS_INLINE_NO_SANITIZE_UNDEFINED __attribute__((__always_inline__, __no_sanitize__("undefined")))
#else  /// It does not work in GCC. GCC 7 cannot recognize this attribute and GCC 8 simply ignores it.
#    define NO_SANITIZE_UNDEFINED
#    define NO_SANITIZE_ADDRESS
#    define NO_SANITIZE_THREAD
#    define ALWAYS_INLINE_NO_SANITIZE_UNDEFINED ALWAYS_INLINE
#endif

#if defined(__clang__) && defined(__clang_major__) && __clang_major__ >= 14
#    define DISABLE_SANITIZER_INSTRUMENTATION __attribute__((disable_sanitizer_instrumentation))
#else
#    define DISABLE_SANITIZER_INSTRUMENTATION
#endif


#if !__has_include(<sanitizer/asan_interface.h>) || !defined(ADDRESS_SANITIZER)
#   define ASAN_UNPOISON_MEMORY_REGION(a, b)
#   define ASAN_POISON_MEMORY_REGION(a, b)
#endif

#if !defined(ABORT_ON_LOGICAL_ERROR)
    #if !defined(NDEBUG) || defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER) || defined(MEMORY_SANITIZER) || defined(UNDEFINED_BEHAVIOR_SANITIZER)
        #define ABORT_ON_LOGICAL_ERROR
    #endif
#endif

/// chassert(x) is similar to assert(x), but:
///     - works in builds with sanitizers, not only in debug builds
///     - tries to print failed assertion into server log
/// It can be used for all assertions except heavy ones.
/// Heavy assertions (that run loops or call complex functions) are allowed in debug builds only.
#if !defined(chassert)
    #if defined(ABORT_ON_LOGICAL_ERROR)
        #define chassert(x) static_cast<bool>(x) ? void(0) : abortOnFailedAssertion(#x)
    #else
        #define chassert(x) ((void)0)
    #endif
#endif

/// Macros for Clang Thread Safety Analysis (TSA). They can be safely ignored by other compilers.
/// Feel free to extend, but please stay close to https://clang.llvm.org/docs/ThreadSafetyAnalysis.html#mutexheader
#if defined(__clang__)
#    define TSA_GUARDED_BY(...) __attribute__((guarded_by(__VA_ARGS__)))                       /// data is protected by given capability
#    define TSA_PT_GUARDED_BY(...) __attribute__((pt_guarded_by(__VA_ARGS__)))                 /// pointed-to data is protected by the given capability
#    define TSA_REQUIRES(...) __attribute__((requires_capability(__VA_ARGS__)))                /// thread needs exclusive possession of given capability
#    define TSA_REQUIRES_SHARED(...) __attribute__((requires_shared_capability(__VA_ARGS__)))  /// thread needs shared possession of given capability
#    define TSA_ACQUIRED_AFTER(...) __attribute__((acquired_after(__VA_ARGS__)))               /// annotated lock must be locked after given lock
#    define TSA_NO_THREAD_SAFETY_ANALYSIS __attribute__((no_thread_safety_analysis))           /// disable TSA for a function

/// Macros for suppressing TSA warnings for specific reads/writes (instead of suppressing it for the whole function)
/// Consider adding a comment before using these macros.
#   define TSA_SUPPRESS_WARNING_FOR_READ(x) [&]() TSA_NO_THREAD_SAFETY_ANALYSIS -> const auto & { return (x); }()
#   define TSA_SUPPRESS_WARNING_FOR_WRITE(x) [&]() TSA_NO_THREAD_SAFETY_ANALYSIS -> auto & { return (x); }()

/// This macro is useful when only one thread writes to a member
/// and you want to read this member from the same thread without locking a mutex.
/// It's safe (because no concurrent writes are possible), but TSA generates a warning.
/// (Seems like there's no way to verify it, but it makes sense to distinguish it from TSA_SUPPRESS_WARNING_FOR_READ for readability)
#   define TSA_READ_ONE_THREAD(x) TSA_SUPPRESS_WARNING_FOR_READ(x)

#else
#    define TSA_GUARDED_BY(...)
#    define TSA_PT_GUARDED_BY(...)
#    define TSA_REQUIRES(...)
#    define TSA_REQUIRES_SHARED(...)
#    define TSA_NO_THREAD_SAFETY_ANALYSIS

#    define TSA_SUPPRESS_WARNING_FOR_READ(x)
#    define TSA_SUPPRESS_WARNING_FOR_WRITE(x)
#    define TSA_READ_ONE_THREAD(x)
#endif

/// A template function for suppressing warnings about unused variables or function results.
template <typename... Args>
constexpr void UNUSED(Args &&... args [[maybe_unused]])
{
}
