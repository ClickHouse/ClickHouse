#pragma once

#if defined(_MSC_VER)
#   if !defined(likely)
#      define likely(x)   (x)
#   endif
#   if !defined(unlikely)
#      define unlikely(x) (x)
#   endif
#else
#   if !defined(likely)
#       define likely(x)   (__builtin_expect(!!(x), 1))
#   endif
#   if !defined(unlikely)
#       define unlikely(x) (__builtin_expect(!!(x), 0))
#   endif
#endif

#if defined(_MSC_VER)
#    define ALWAYS_INLINE __forceinline
#    define NO_INLINE static __declspec(noinline)
#    define MAY_ALIAS
#else
#    define ALWAYS_INLINE __attribute__((__always_inline__))
#    define NO_INLINE __attribute__((__noinline__))
#    define MAY_ALIAS __attribute__((__may_alias__))
#endif

#if !defined(__x86_64__) && !defined(__aarch64__) && !defined(__PPC__)
#    error "The only supported platforms are x86_64 and AArch64, PowerPC (work in progress)"
#endif

/// Check for presence of address sanitizer
#if !defined(ADDRESS_SANITIZER)
#    if defined(__has_feature)
#        if __has_feature(address_sanitizer)
#            define ADDRESS_SANITIZER 1
#        endif
#    elif defined(__SANITIZE_ADDRESS__)
#        define ADDRESS_SANITIZER 1
#    endif
#endif

#if !defined(THREAD_SANITIZER)
#    if defined(__has_feature)
#        if __has_feature(thread_sanitizer)
#            define THREAD_SANITIZER 1
#        endif
#    elif defined(__SANITIZE_THREAD__)
#        define THREAD_SANITIZER 1
#    endif
#endif

#if !defined(MEMORY_SANITIZER)
#    if defined(__has_feature)
#        if __has_feature(memory_sanitizer)
#            define MEMORY_SANITIZER 1
#        endif
#    elif defined(__MEMORY_SANITIZER__)
#        define MEMORY_SANITIZER 1
#    endif
#endif

/// TODO: Strange enough, there is no way to detect UB sanitizer.

/// Explicitly allow undefined behaviour for certain functions. Use it as a function attribute.
/// It is useful in case when compiler cannot see (and exploit) it, but UBSan can.
/// Example: multiplication of signed integers with possibility of overflow when both sides are from user input.
#if defined(__clang__)
#    define NO_SANITIZE_UNDEFINED __attribute__((__no_sanitize__("undefined")))
#    define NO_SANITIZE_ADDRESS __attribute__((__no_sanitize__("address")))
#    define NO_SANITIZE_THREAD __attribute__((__no_sanitize__("thread")))
#    define NO_SANITIZE_MEMORY __attribute__((__no_sanitize__("memory")))
#else  /// It does not work in GCC. GCC 7 cannot recognize this attribute and GCC 8 simply ignores it.
#    define NO_SANITIZE_UNDEFINED
#    define NO_SANITIZE_ADDRESS
#    define NO_SANITIZE_THREAD
#    define NO_SANITIZE_MEMORY
#endif

#if defined __GNUC__ && !defined __clang__
#    define OPTIMIZE(x) __attribute__((__optimize__(x)))
#else
#    define OPTIMIZE(x)
#endif

/// A macro for suppressing warnings about unused variables or function results.
/// Useful for structured bindings which have no standard way to declare this.
#define UNUSED(...) (void)(__VA_ARGS__)
