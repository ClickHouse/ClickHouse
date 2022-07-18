#pragma once

#include <base/types.h>

/* This file contains macros and helpers for writing platform-dependent code.
 *
 * Macros DECLARE_<Arch>_SPECIFIC_CODE will wrap code inside it into the
 * namespace TargetSpecific::<Arch> and enable Arch-specific compile options.
 * Thus, it's allowed to call functions inside these namespaces only after
 * checking platform in runtime (see isArchSupported() below).
 *
 * If compiler is not gcc/clang or target isn't x86_64 or ENABLE_MULTITARGET_CODE
 * was set to OFF in cmake, all code inside these macros will be removed and
 * USE_MULTITARGET_CODE will be set to 0. Use #if USE_MULTITARGET_CODE whenever you
 * use anything from this namespaces.
 *
 * For similarities there is a macros DECLARE_DEFAULT_CODE, which wraps code
 * into the namespace TargetSpecific::Default but doesn't specify any additional
 * copile options. Functions and classes inside this macros are available regardless
 * of USE_MUTLITARGE_CODE.
 *
 * Example of usage:
 *
 * DECLARE_DEFAULT_CODE (
 * int funcImpl() {
 *     return 1;
 * }
 * ) // DECLARE_DEFAULT_CODE
 *
 * DECLARE_AVX2_SPECIFIC_CODE (
 * int funcImpl() {
 *     return 2;
 * }
 * ) // DECLARE_DEFAULT_CODE
 *
 * int func() {
 * #if USE_MULTITARGET_CODE
 *     if (isArchSupported(TargetArch::AVX2))
 *         return TargetSpecific::AVX2::funcImpl();
 * #endif
 *     return TargetSpecific::Default::funcImpl();
 * }
 *
 * Sometimes code may benefit from compiling with different options.
 * For these purposes use DECLARE_MULTITARGET_CODE macros. It will create a copy
 * of the code for every supported target and compile it with different options.
 * These copies are available via TargetSpecific namespaces described above.
 *
 * Inside every TargetSpecific namespace there is a constexpr variable BuildArch,
 * which indicates the target platform for current code.
 *
 * Example:
 *
 * DECLARE_MULTITARGET_CODE(
 * int funcImpl(int size, ...) {
 *     int iteration_size = 1;
 *     if constexpr (BuildArch == TargetArch::SSE42)
 *         iteration_size = 2
 *     else if constexpr (BuildArch == TargetArch::AVX || BuildArch == TargetArch::AVX2)
 *         iteration_size = 4;
 *     for (int i = 0; i < size; i += iteration_size)
 *     ...
 * }
 * ) // DECLARE_MULTITARGET_CODE
 *
 * // All target-specific and default implementations are available here via
 * TargetSpecific::<Arch>::funcImpl. Use runtime detection to choose one.
 *
 * If you want to write IFunction or IExecutableFuncionImpl with several implementations
 * see PerformanceAdaptors.h.
 */

namespace DB
{

enum class TargetArch : UInt32
{
    Default  = 0,         /// Without any additional compiler options.
    SSE42    = (1 << 0),  /// SSE4.2
    AVX      = (1 << 1),
    AVX2     = (1 << 2),
    AVX512F  = (1 << 3),
    AVX512BW    = (1 << 4),
    AVX512VBMI  = (1 << 5),
    AVX512VBMI2 = (1 << 6),
};

/// Runtime detection.
bool isArchSupported(TargetArch arch);

String toString(TargetArch arch);

#ifndef ENABLE_MULTITARGET_CODE
#   define ENABLE_MULTITARGET_CODE 0
#endif

#if ENABLE_MULTITARGET_CODE && defined(__GNUC__) && defined(__x86_64__)

/// NOLINTNEXTLINE
#define USE_MULTITARGET_CODE 1

#if defined(__clang__)

#define AVX512VBMI2_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f,avx512bw,avx512vl,avx512vbmi,avx512vbmi2")))
#define AVX512VBMI_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f,avx512bw,avx512vl,avx512vbmi")))
#define AVX512BW_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f,avx512bw")))
#define AVX512_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f")))
#define AVX2_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2")))
#define AVX_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx"))
#define SSE42_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt")))
#define DEFAULT_FUNCTION_SPECIFIC_ATTRIBUTE

#   define BEGIN_AVX512VBMI2_SPECIFIC_CODE \
        _Pragma("clang attribute push(__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f,avx512bw,avx512vl,avx512vbmi,avx512vbmi2\"))),apply_to=function)")
#   define BEGIN_AVX512VBMI_SPECIFIC_CODE \
        _Pragma("clang attribute push(__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f,avx512bw,avx512vl,avx512vbmi\"))),apply_to=function)")
#   define BEGIN_AVX512BW_SPECIFIC_CODE \
        _Pragma("clang attribute push(__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f,avx512bw\"))),apply_to=function)")
#   define BEGIN_AVX512F_SPECIFIC_CODE \
        _Pragma("clang attribute push(__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f\"))),apply_to=function)")
#   define BEGIN_AVX2_SPECIFIC_CODE \
        _Pragma("clang attribute push(__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2\"))),apply_to=function)")
#   define BEGIN_AVX_SPECIFIC_CODE \
        _Pragma("clang attribute push(__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx\"))),apply_to=function)")
#   define BEGIN_SSE42_SPECIFIC_CODE \
        _Pragma("clang attribute push(__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt\"))),apply_to=function)")
#   define END_TARGET_SPECIFIC_CODE \
        _Pragma("clang attribute pop")

/* Clang shows warning when there aren't any objects to apply pragma.
 * To prevent this warning we define this function inside every macros with pragmas.
 */
#   define DUMMY_FUNCTION_DEFINITION [[maybe_unused]] void _dummy_function_definition();
#else

#define AVX512VBMI2_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f,avx512bw,avx512vl,avx512vbmi,avx512vbmi2,tune=native")))
#define AVX512VBMI_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f,avx512bw,avx512vl,avx512vbmi,tune=native")))
#define AVX512BW_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f,avx512bw,tune=native")))
#define AVX512_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f,tune=native")))
#define AVX2_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,tune=native")))
#define AVX_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx,tune=native")))
#define SSE42_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt",tune=native)))
#define DEFAULT_FUNCTION_SPECIFIC_ATTRIBUTE

#   define BEGIN_AVX512VBMI2_SPECIFIC_CODE \
        _Pragma("GCC push_options") \
        _Pragma("GCC target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f,avx512bw,avx512vl,avx512vbmi,avx512vbmi2,tune=native\")")
#   define BEGIN_AVX512VBMI_SPECIFIC_CODE \
        _Pragma("GCC push_options") \
        _Pragma("GCC target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f,avx512bw,avx512vl,avx512vbmi,tune=native\")")
#   define BEGIN_AVX512BW_SPECIFIC_CODE \
        _Pragma("GCC push_options") \
        _Pragma("GCC target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f,avx512bw,tune=native\")")
#   define BEGIN_AVX512F_SPECIFIC_CODE \
        _Pragma("GCC push_options") \
        _Pragma("GCC target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f,tune=native\")")
#   define BEGIN_AVX2_SPECIFIC_CODE \
        _Pragma("GCC push_options") \
        _Pragma("GCC target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,tune=native\")")
#   define BEGIN_AVX_SPECIFIC_CODE \
        _Pragma("GCC push_options") \
        _Pragma("GCC target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx,tune=native\")")
#   define BEGIN_SSE42_SPECIFIC_CODE \
        _Pragma("GCC push_options") \
        _Pragma("GCC target(\"sse,sse2,sse3,ssse3,sse4,popcnt,tune=native\")")
#   define END_TARGET_SPECIFIC_CODE \
        _Pragma("GCC pop_options")

/* GCC doesn't show such warning, we don't need to define anything.
 */
#   define DUMMY_FUNCTION_DEFINITION
#endif

#define DECLARE_SSE42_SPECIFIC_CODE(...) \
BEGIN_SSE42_SPECIFIC_CODE \
namespace TargetSpecific::SSE42 { \
    DUMMY_FUNCTION_DEFINITION \
    using namespace DB::TargetSpecific::SSE42; \
    __VA_ARGS__ \
} \
END_TARGET_SPECIFIC_CODE

#define DECLARE_AVX_SPECIFIC_CODE(...) \
BEGIN_AVX_SPECIFIC_CODE \
namespace TargetSpecific::AVX { \
    DUMMY_FUNCTION_DEFINITION \
    using namespace DB::TargetSpecific::AVX; \
    __VA_ARGS__ \
} \
END_TARGET_SPECIFIC_CODE

#define DECLARE_AVX2_SPECIFIC_CODE(...) \
BEGIN_AVX2_SPECIFIC_CODE \
namespace TargetSpecific::AVX2 { \
    DUMMY_FUNCTION_DEFINITION \
    using namespace DB::TargetSpecific::AVX2; \
    __VA_ARGS__ \
} \
END_TARGET_SPECIFIC_CODE

#define DECLARE_AVX512F_SPECIFIC_CODE(...) \
BEGIN_AVX512F_SPECIFIC_CODE \
namespace TargetSpecific::AVX512F { \
    DUMMY_FUNCTION_DEFINITION \
    using namespace DB::TargetSpecific::AVX512F; \
    __VA_ARGS__ \
} \
END_TARGET_SPECIFIC_CODE

#define DECLARE_AVX512BW_SPECIFIC_CODE(...) \
BEGIN_AVX512BW_SPECIFIC_CODE \
namespace TargetSpecific::AVX512BW { \
    DUMMY_FUNCTION_DEFINITION \
    using namespace DB::TargetSpecific::AVX512BW; \
    __VA_ARGS__ \
} \
END_TARGET_SPECIFIC_CODE

#define DECLARE_AVX512VBMI_SPECIFIC_CODE(...) \
BEGIN_AVX512VBMI_SPECIFIC_CODE \
namespace TargetSpecific::AVX512VBMI { \
    DUMMY_FUNCTION_DEFINITION \
    using namespace DB::TargetSpecific::AVX512VBMI; \
    __VA_ARGS__ \
} \
END_TARGET_SPECIFIC_CODE

#define DECLARE_AVX512VBMI2_SPECIFIC_CODE(...) \
BEGIN_AVX512VBMI2_SPECIFIC_CODE \
namespace TargetSpecific::AVX512VBMI2 { \
    DUMMY_FUNCTION_DEFINITION \
    using namespace DB::TargetSpecific::AVX512VBMI2; \
    __VA_ARGS__ \
} \
END_TARGET_SPECIFIC_CODE


#else

#define USE_MULTITARGET_CODE 0

/* Multitarget code is disabled, just delete target-specific code.
 */
#define DECLARE_SSE42_SPECIFIC_CODE(...)
#define DECLARE_AVX_SPECIFIC_CODE(...)
#define DECLARE_AVX2_SPECIFIC_CODE(...)
#define DECLARE_AVX512F_SPECIFIC_CODE(...)
#define DECLARE_AVX512BW_SPECIFIC_CODE(...)
#define DECLARE_AVX512VBMI_SPECIFIC_CODE(...)
#define DECLARE_AVX512VBMI2_SPECIFIC_CODE(...)

#endif

#define DECLARE_DEFAULT_CODE(...) \
namespace TargetSpecific::Default { \
    using namespace DB::TargetSpecific::Default; \
    __VA_ARGS__ \
}

/// NOLINTNEXTLINE
#define DECLARE_MULTITARGET_CODE(...) \
DECLARE_DEFAULT_CODE         (__VA_ARGS__) \
DECLARE_SSE42_SPECIFIC_CODE  (__VA_ARGS__) \
DECLARE_AVX_SPECIFIC_CODE    (__VA_ARGS__) \
DECLARE_AVX2_SPECIFIC_CODE   (__VA_ARGS__) \
DECLARE_AVX512F_SPECIFIC_CODE(__VA_ARGS__) \
DECLARE_AVX512BW_SPECIFIC_CODE    (__VA_ARGS__) \
DECLARE_AVX512VBMI_SPECIFIC_CODE  (__VA_ARGS__) \
DECLARE_AVX512VBMI2_SPECIFIC_CODE (__VA_ARGS__)

DECLARE_DEFAULT_CODE(
    constexpr auto BuildArch = TargetArch::Default; /// NOLINT
) // DECLARE_DEFAULT_CODE

DECLARE_SSE42_SPECIFIC_CODE(
    constexpr auto BuildArch = TargetArch::SSE42; /// NOLINT
) // DECLARE_SSE42_SPECIFIC_CODE

DECLARE_AVX_SPECIFIC_CODE(
    constexpr auto BuildArch = TargetArch::AVX; /// NOLINT
) // DECLARE_AVX_SPECIFIC_CODE

DECLARE_AVX2_SPECIFIC_CODE(
    constexpr auto BuildArch = TargetArch::AVX2; /// NOLINT
) // DECLARE_AVX2_SPECIFIC_CODE

DECLARE_AVX512F_SPECIFIC_CODE(
    constexpr auto BuildArch = TargetArch::AVX512F; /// NOLINT
) // DECLARE_AVX512F_SPECIFIC_CODE

DECLARE_AVX512BW_SPECIFIC_CODE(
    constexpr auto BuildArch = TargetArch::AVX512BW; /// NOLINT
) // DECLARE_AVX512BW_SPECIFIC_CODE

DECLARE_AVX512VBMI_SPECIFIC_CODE(
    constexpr auto BuildArch = TargetArch::AVX512VBMI; /// NOLINT
) // DECLARE_AVX512VBMI_SPECIFIC_CODE

DECLARE_AVX512VBMI2_SPECIFIC_CODE(
    constexpr auto BuildArch = TargetArch::AVX512VBMI2; /// NOLINT
) // DECLARE_AVX512VBMI2_SPECIFIC_CODE

/** Runtime Dispatch helpers for class members.
  *
  * Example of usage:
  *
  * class TestClass
  * {
  * public:
  *     MULTITARGET_FUNCTION_AVX2_SSE42(
  *     MULTITARGET_FUNCTION_HEADER(int), testFunctionImpl, MULTITARGET_FUNCTION_BODY((int value)
  *     {
  *          return value;
  *     })
  *     )
  *
  *     void testFunction(int value) {
  *         if (isArchSupported(TargetArch::AVX2))
  *         {
  *             testFunctionImplAVX2(value);
  *         }
  *         else if (isArchSupported(TargetArch::SSE42))
  *         {
  *             testFunctionImplSSE42(value);
  *         }
  *         else
  *         {
  *             testFunction(value);
  *         }
  *     }
  *};
  *
  */

/// Function header
#define MULTITARGET_FUNCTION_HEADER(...) __VA_ARGS__

/// Function body
#define MULTITARGET_FUNCTION_BODY(...) __VA_ARGS__

#if ENABLE_MULTITARGET_CODE && defined(__GNUC__) && defined(__x86_64__)

/// NOLINTNEXTLINE
#define MULTITARGET_FUNCTION_AVX2_SSE42(FUNCTION_HEADER, name, FUNCTION_BODY) \
    FUNCTION_HEADER \
    \
    AVX2_FUNCTION_SPECIFIC_ATTRIBUTE \
    name##AVX2 \
    FUNCTION_BODY \
    \
    FUNCTION_HEADER \
    \
    SSE42_FUNCTION_SPECIFIC_ATTRIBUTE \
    name##SSE42 \
    FUNCTION_BODY \
    \
    FUNCTION_HEADER \
    \
    name \
    FUNCTION_BODY \

#else

/// NOLINTNEXTLINE
#define MULTITARGET_FUNCTION_AVX2_SSE42(FUNCTION_HEADER, name, FUNCTION_BODY) \
    FUNCTION_HEADER \
    \
    name \
    FUNCTION_BODY \

#endif

}
