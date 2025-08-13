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

#if !defined(__x86_64__) && !defined(__aarch64__) && !defined(__PPC__) && !defined(__s390x__) && !(defined(__loongarch64)) && !(defined(__riscv) && (__riscv_xlen == 64))
#    error "The only supported platforms are x86_64 and AArch64, PowerPC (work in progress), s390x (work in progress), loongarch64 (experimental) and RISC-V 64 (experimental)"
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

/// We used to have only ABORT_ON_LOGICAL_ERROR macro, but most of its uses were actually in places where we didn't care about logical errors
/// but wanted to check exactly if the current build type is debug or with sanitizer. This new macro is introduced to fix those places.
#if !defined(DEBUG_OR_SANITIZER_BUILD)
#    if !defined(NDEBUG) || defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER) || defined(MEMORY_SANITIZER) \
        || defined(UNDEFINED_BEHAVIOR_SANITIZER)
#        define DEBUG_OR_SANITIZER_BUILD
#    endif
#endif

/// chassert(x) is similar to assert(x), but:
///     - works in builds with sanitizers, not only in debug builds
///     - tries to print failed assertion into server log
/// It can be used for all assertions except heavy ones.
/// Heavy assertions (that run loops or call complex functions) are allowed in debug builds only.
/// Also it makes sense to call abort() instead of __builtin_unreachable() in debug builds,
/// because SIGABRT is easier to debug than SIGTRAP (the second one makes gdb crazy)
#if !defined(chassert)
#    if defined(DEBUG_OR_SANITIZER_BUILD)
        // clang-format off
        #include <base/types.h>
        namespace DB
        {
            [[noreturn]] void abortOnFailedAssertion(const String & description);
        }
        #define chassert_1(x, ...) do { static_cast<bool>(x) ? void(0) : ::DB::abortOnFailedAssertion(#x); } while (0)
        #define chassert_2(x, comment, ...) do { static_cast<bool>(x) ? void(0) : ::DB::abortOnFailedAssertion(comment); } while (0)
        #define UNREACHABLE() abort()
        // clang-format off
    #else
        /// Here sizeof() trick is used to suppress unused warning for result,
        /// since simple "(void)x" will evaluate the expression, while
        /// "sizeof(!(x))" will not.
        #define chassert_1(x, ...) (void)sizeof(!(x))
        #define chassert_2(x, comment, ...) (void)sizeof(!(x))
        #define UNREACHABLE() __builtin_unreachable()
    #endif
        #define CHASSERT_DISPATCH(_1,_2, N,...) N(_1, _2)
        #define CHASSERT_INVOKE(tuple) CHASSERT_DISPATCH tuple
        #define chassert(...) CHASSERT_INVOKE((__VA_ARGS__, chassert_2, chassert_1))

#endif

/// Macros for Clang Thread Safety Analysis (TSA). They can be safely ignored by other compilers.
/// Feel free to extend, but please stay close to https://clang.llvm.org/docs/ThreadSafetyAnalysis.html#mutexheader
#define TSA_GUARDED_BY(...) __attribute__((guarded_by(__VA_ARGS__)))                       /// data is protected by given capability
#define TSA_PT_GUARDED_BY(...) __attribute__((pt_guarded_by(__VA_ARGS__)))                 /// pointed-to data is protected by the given capability
#define TSA_REQUIRES(...) __attribute__((requires_capability(__VA_ARGS__)))                /// thread needs exclusive possession of given capability
#define TSA_REQUIRES_SHARED(...) __attribute__((requires_shared_capability(__VA_ARGS__)))  /// thread needs shared possession of given capability
#define TSA_ACQUIRED_AFTER(...) __attribute__((acquired_after(__VA_ARGS__)))               /// annotated lock must be locked after given lock
#define TSA_NO_THREAD_SAFETY_ANALYSIS __attribute__((no_thread_safety_analysis))           /// disable TSA for a function
#define TSA_CAPABILITY(...) __attribute__((capability(__VA_ARGS__)))                       /// object of a class can be used as capability
#define TSA_ACQUIRE(...) __attribute__((acquire_capability(__VA_ARGS__)))                        /// function acquires a capability, but does not release it
#define TSA_TRY_ACQUIRE(...) __attribute__((try_acquire_capability(__VA_ARGS__)))                /// function tries to acquire a capability and returns a boolean value indicating success or failure
#define TSA_RELEASE(...) __attribute__((release_capability(__VA_ARGS__)))                        /// function releases the given capability
#define TSA_ACQUIRE_SHARED(...) __attribute__((acquire_shared_capability(__VA_ARGS__)))          /// function acquires a shared capability, but does not release it
#define TSA_TRY_ACQUIRE_SHARED(...) __attribute__((try_acquire_shared_capability(__VA_ARGS__)))  /// function tries to acquire a shared capability and returns a boolean value indicating success or failure
#define TSA_RELEASE_SHARED(...) __attribute__((release_shared_capability(__VA_ARGS__)))          /// function releases the given shared capability
#define TSA_SCOPED_LOCKABLE __attribute__((scoped_lockable)) /// object of a class has scoped lockable capability

/// Macros for suppressing TSA warnings for specific reads/writes (instead of suppressing it for the whole function)
/// They use a lambda function to apply function attribute to a single statement. This enable us to suppress warnings locally instead of
/// suppressing them in the whole function
/// Consider adding a comment when using these macros.
#define TSA_SUPPRESS_WARNING_FOR_READ(x) ([&]() TSA_NO_THREAD_SAFETY_ANALYSIS -> const auto & { return (x); }())
#define TSA_SUPPRESS_WARNING_FOR_WRITE(x) ([&]() TSA_NO_THREAD_SAFETY_ANALYSIS -> auto & { return (x); }())

/// This macro is useful when only one thread writes to a member
/// and you want to read this member from the same thread without locking a mutex.
/// It's safe (because no concurrent writes are possible), but TSA generates a warning.
/// (Seems like there's no way to verify it, but it makes sense to distinguish it from TSA_SUPPRESS_WARNING_FOR_READ for readability)
#define TSA_READ_ONE_THREAD(x) TSA_SUPPRESS_WARNING_FOR_READ(x)

/// A template function for suppressing warnings about unused variables or function results.
template <typename... Args>
constexpr void UNUSED(Args &&... args [[maybe_unused]]) // NOLINT(cppcoreguidelines-missing-std-forward)
{
}
