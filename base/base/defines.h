#pragma once

#if !defined(__x86_64__) && !defined(__aarch64__) && !defined(__PPC__) && !defined(__s390x__) && !(defined(__loongarch64)) && !(defined(__riscv) && (__riscv_xlen == 64)) && !defined(__e2k__) && !defined(__wasm__)
#    error "The only supported platforms are x86_64 and AArch64, PowerPC (work in progress), s390x (work in progress), loongarch64 (experimental), RISC-V 64 (experimental), E2K (experimental, work in progress) and WebAssembly (only a subset of the code, such as the SQL parser, is expected to build)"
#endif

/// Whether plain `long` is a type of its own, distinct from every fixed-width integer type.
/// On Darwin `Int64` is `long long`, and on 32-bit platforms (WebAssembly) `Int32` is `int`
/// while `long` is a separate 32-bit type. In both cases functions overloaded on the
/// fixed-width types need an overload for `long` as well, or calls with a `long` argument
/// become ambiguous.
#if defined(OS_DARWIN) || !defined(__LP64__)
#    define LONG_IS_A_DISTINCT_TYPE 1
#endif

/// Whether `size_t` is a type of its own, distinct from every fixed-width integer type.
/// This is a strictly narrower condition than `LONG_IS_A_DISTINCT_TYPE`: `size_t` is
/// `unsigned long` on Darwin and WebAssembly, but on the other 32-bit platforms it is
/// `unsigned int`, which is exactly `UInt32`, so it must not get an overload of its own there.
#if defined(OS_DARWIN) || defined(__wasm__)
#    define SIZE_T_IS_A_DISTINCT_TYPE 1
#endif

/// Whether every `std::exception` carries the stack trace of the throw that created it.
/// ClickHouse's patched libc++ records it (`contrib/libcxx-cmake` defines this to 1), and every
/// supported platform links that libc++. A port that has to use a foreign C++ standard library -
/// the standalone parser build in `utils/wasm-parser`, for one - gets no trace, and there is no
/// `std::exception::get_stack_trace_frames` to call at all. `Common/StackTrace.h` is where the
/// difference is handled; nothing else should test this macro.
/// Only the absence is defaulted here: a supported platform that somehow lost the definition must
/// not silently start throwing exceptions without stack traces, so `Common/Exception.cpp` asserts
/// that it is 1 there.
#if !defined(STD_EXCEPTION_HAS_STACK_TRACE)
#    define STD_EXCEPTION_HAS_STACK_TRACE 0
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

#include <base/sanitizer_defs.h>

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
        #include <stdlib.h>
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
#define TSA_GUARDED_BY(...) __attribute__((guarded_by(__VA_ARGS__)))                             /// data is protected by given capability
#define TSA_PT_GUARDED_BY(...) __attribute__((pt_guarded_by(__VA_ARGS__)))                       /// pointed-to data is protected by the given capability
#define TSA_REQUIRES(...) __attribute__((requires_capability(__VA_ARGS__)))                      /// thread needs exclusive possession of given capability
#define TSA_REQUIRES_SHARED(...) __attribute__((requires_shared_capability(__VA_ARGS__)))        /// thread needs shared possession of given capability
#define TSA_ACQUIRED_AFTER(...) __attribute__((acquired_after(__VA_ARGS__)))                     /// annotated lock must be locked after given lock
#define TSA_NO_THREAD_SAFETY_ANALYSIS __attribute__((no_thread_safety_analysis))                 /// disable TSA for a function
#define TSA_CAPABILITY(...) __attribute__((capability(__VA_ARGS__)))                             /// object of a class can be used as capability
#define TSA_ACQUIRE(...) __attribute__((acquire_capability(__VA_ARGS__)))                        /// function acquires a capability, but does not release it
#define TSA_TRY_ACQUIRE(...) __attribute__((try_acquire_capability(__VA_ARGS__)))                /// function tries to acquire a capability and returns a boolean value indicating success or failure
#define TSA_RELEASE(...) __attribute__((release_capability(__VA_ARGS__)))                        /// function releases the given capability
#define TSA_ACQUIRE_SHARED(...) __attribute__((acquire_shared_capability(__VA_ARGS__)))          /// function acquires a shared capability, but does not release it
#define TSA_TRY_ACQUIRE_SHARED(...) __attribute__((try_acquire_shared_capability(__VA_ARGS__)))  /// function tries to acquire a shared capability and returns a boolean value indicating success or failure
#define TSA_RELEASE_SHARED(...) __attribute__((release_shared_capability(__VA_ARGS__)))          /// function releases the given shared capability
#define TSA_SCOPED_LOCKABLE __attribute__((scoped_lockable))                                     /// object of a class has scoped lockable capability
#define TSA_RETURN_CAPABILITY(...) __attribute__((lock_returned(__VA_ARGS__)))                   /// to return capabilities in functions
#define TSA_ASSERT_CAPABILITY(...) __attribute__((assert_exclusive_lock(__VA_ARGS__)))           /// assert that exclusive capability is acquired
#define TSA_ASSERT_SHARED_CAPABILITY(...) __attribute__((assert_shared_lock(__VA_ARGS__)))       /// assert that shared capability is acquired

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
