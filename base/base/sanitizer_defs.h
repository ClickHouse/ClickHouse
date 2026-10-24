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
///
/// It also switches off `unsigned-integer-overflow`, which is not a part of the `undefined` group:
/// a function that is allowed to overflow signed integers is, in practice, always a function that is
/// allowed to wrap around unsigned integers as well (arithmetic on user-provided values, rounding,
/// interval and date/time calculations), and requiring a second annotation in every such place would
/// only make it easier to forget one of them.
#define NO_SANITIZE_UNDEFINED __attribute__((__no_sanitize__("undefined", "unsigned-integer-overflow")))

/// Unsigned integer overflow is well-defined behaviour in C++: it wraps around modulo 2^N.
/// We rely on that in hash functions, checksums, pseudo-random number generators, and in the
/// semantics of ClickHouse arithmetic over unsigned types. Nevertheless, UBSan can be asked to
/// report it (`-fsanitize=unsigned-integer-overflow`), because an *unintended* wraparound is a bug,
/// and typically a nasty one: `size - 1` of an empty container, an offset that walks behind a
/// buffer, a capacity calculation that silently truncates. We enable the check in CI, so every
/// place that wraps around on purpose has to say so - use this attribute for such functions.
///
/// Prefer this over NO_SANITIZE_UNDEFINED when the code is well-defined and only wraps around
/// unsigned values: it keeps the actual undefined behaviour in the function under the sanitizer.
#define NO_SANITIZE_UNSIGNED_OVERFLOW __attribute__((__no_sanitize__("unsigned-integer-overflow")))

#define NO_SANITIZE_ADDRESS __attribute__((__no_sanitize__("address")))
#define NO_SANITIZE_THREAD __attribute__((__no_sanitize__("thread")))
#define ALWAYS_INLINE_NO_SANITIZE_UNDEFINED __attribute__((__always_inline__, __no_sanitize__("undefined", "unsigned-integer-overflow")))
#define ALWAYS_INLINE_NO_SANITIZE_UNSIGNED_OVERFLOW __attribute__((__always_inline__, __no_sanitize__("unsigned-integer-overflow")))
#define DISABLE_SANITIZER_INSTRUMENTATION __attribute__((disable_sanitizer_instrumentation))

#if !__has_include(<sanitizer/asan_interface.h>) || !defined(ADDRESS_SANITIZER)
#   define ASAN_UNPOISON_MEMORY_REGION(a, b)
#   define ASAN_POISON_MEMORY_REGION(a, b)
#endif
