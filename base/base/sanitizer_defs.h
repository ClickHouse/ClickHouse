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
#if defined(__has_feature)
#    define ch_has_feature __has_feature
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
#    if defined(ch_has_feature)
#        if ch_has_feature(undefined_behavior_sanitizer)
#            define UNDEFINED_BEHAVIOR_SANITIZER 1
#        endif
#    elif defined(__UNDEFINED_BEHAVIOR_SANITIZER__)
#        define UNDEFINED_BEHAVIOR_SANITIZER 1
#    endif
#endif

/// We used to have only ABORT_ON_LOGICAL_ERROR macro, but most of its uses were actually in places where we didn't care about logical errors
/// but wanted to check exactly if the current build type is debug or with sanitizer. This new macro is introduced to fix those places.
#if !defined(DEBUG_OR_SANITIZER_BUILD)
#    if !defined(NDEBUG) || defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER) || defined(MEMORY_SANITIZER) \
        || defined(UNDEFINED_BEHAVIOR_SANITIZER)
#        define DEBUG_OR_SANITIZER_BUILD
#    endif
#endif

/// Explicitly allow undefined behaviour for certain functions. Use it as a function attribute.
/// It is useful in case when compiler cannot see (and exploit) it, but UBSan can.
/// Example: multiplication of signed integers with possibility of overflow when both sides are from user input.
#define NO_SANITIZE_UNDEFINED __attribute__((__no_sanitize__("undefined")))
#define NO_SANITIZE_ADDRESS __attribute__((__no_sanitize__("address")))
#define NO_SANITIZE_THREAD __attribute__((__no_sanitize__("thread")))
#define ALWAYS_INLINE_NO_SANITIZE_UNDEFINED __attribute__((__always_inline__, __no_sanitize__("undefined")))
#define DISABLE_SANITIZER_INSTRUMENTATION __attribute__((disable_sanitizer_instrumentation))

#if !__has_include(<sanitizer/asan_interface.h>) || !defined(ADDRESS_SANITIZER)
#   define ASAN_UNPOISON_MEMORY_REGION(a, b)
#   define ASAN_POISON_MEMORY_REGION(a, b)
#endif
