#pragma once

#if defined(__clang__)
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wunused-macros"
#endif

#include <base/defines.h>
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
 * ) // DECLARE_AVX2_SPECIFIC_CODE
 *
 * int func() {
 * #if USE_MULTITARGET_CODE
 *     if (isArchSupported(TargetArch::x86_64_v3))
 *         return TargetSpecific::x86_64_v3::funcImpl();
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
 *     if constexpr (BuildArch == TargetArch::x86_64_v2)
 *         iteration_size = 2
 *     else if constexpr (BuildArch == TargetArch::AVX || BuildArch == TargetArch::x86_64_v3)
 *         iteration_size = 4;
 *     for (int i = 0; i < size; i += iteration_size)
 *     ...
 * }
 * ) // DECLARE_MULTITARGET_CODE
 *
 * // All target-specific and default implementations are available here via
 * TargetSpecific::<Arch>::funcImpl. Use runtime detection to choose one.
 *
 * If you want to write IFunction or IExecutableFunctionImpl with several implementations
 * see PerformanceAdaptors.h.
 */

namespace DB
{

/// See https://en.wikipedia.org/wiki/X86-64#Microarchitecture_levels for more details on the instruction sets supported by each level.
/// We use these levels as a convenient way to group related instruction sets and avoid long lists of features and many different instruction
/// sets with small differences.
enum class TargetArch : UInt32
{
    Default = 0,
    x86_64_v2 = (1 << 0),
    x86_64_v3 = (1 << 1),
    x86_64_v4 = (1 << 2),
    x86_64_icelake = (1 << 3),
    x86_64_sapphirerapids = (1 << 4),
    GenuineIntel = (1 << 5),          /// Not an instruction set, but a CPU vendor. Used for optimizations that are only applicable for Intel CPUs, like prefetching
};

/// Runtime detection.
UInt32 getSupportedArchs();
inline ALWAYS_INLINE bool isArchSupported(TargetArch arch)
{
    static UInt32 arches = getSupportedArchs();
    return arch == TargetArch::Default || (arches & static_cast<UInt32>(arch));
}

String toString(TargetArch arch);

#ifndef ENABLE_MULTITARGET_CODE
#   define ENABLE_MULTITARGET_CODE 0
#endif

#if ENABLE_MULTITARGET_CODE && defined(__GNUC__) && defined(__x86_64__)


#define USE_MULTITARGET_CODE 1

/// Function-specific attributes using arch= for cleaner specification
/// This matches -march= compiler flags and avoids long feature lists
///
/// IMPORTANT: Clang's default tuning for `x86-64-v4` includes `TuningPrefer256Bit` (see X86_64V4Tuning
/// in contrib/llvm-project/llvm/lib/Target/X86/X86.td). This was added conservatively to avoid AVX-512
/// frequency throttling on early implementations like Skylake-X. However, for ClickHouse's data-intensive
/// workloads, 512-bit operations often provide better performance despite potential frequency drops because:
/// - Memory-bound operations are less sensitive to frequency reduction
/// - Processing 64 bytes vs 32 bytes per instruction overcomes the penalty for large datasets
/// - Newer CPUs (Ice Lake, Sapphire Rapids, AMD Zen 4/5) have minimal throttling
///
/// We explicitly override with `no-prefer-256-bit` to enable 512-bit vectorization for AVX-512 targets.
#define X86_64_V2_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("arch=x86-64-v2")))
#define X86_64_V3_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("arch=x86-64-v3")))
#define X86_64_V4_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("arch=x86-64-v4,no-prefer-256-bit")))
#define X86_64_ICELAKE_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("arch=icelake-server,no-prefer-256-bit")))
#define X86_64_SAPPHIRE_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("arch=sapphirerapids,no-prefer-256-bit")))

#define DEFAULT_FUNCTION_SPECIFIC_ATTRIBUTE

/// Begin target-specific code blocks using arch= for cleaner specification
#define BEGIN_X86_64_V2_SPECIFIC_CODE \
    _Pragma("clang attribute push(__attribute__((target(\"arch=x86-64-v2\"))),apply_to=function)")
#define BEGIN_X86_64_V3_SPECIFIC_CODE \
    _Pragma("clang attribute push(__attribute__((target(\"arch=x86-64-v3\"))),apply_to=function)")
#define BEGIN_X86_64_V4_SPECIFIC_CODE \
    _Pragma("clang attribute push(__attribute__((target(\"arch=x86-64-v4,no-prefer-256-bit\"))),apply_to=function)")
#define BEGIN_X86_64_ICELAKE_SPECIFIC_CODE \
    _Pragma("clang attribute push(__attribute__((target(\"arch=icelake-server,no-prefer-256-bit\"))),apply_to=function)")
#define BEGIN_X86_64_SAPPHIRE_SPECIFIC_CODE \
    _Pragma("clang attribute push(__attribute__((target(\"arch=sapphirerapids,no-prefer-256-bit\"))),apply_to=function)")

#define END_TARGET_SPECIFIC_CODE \
    _Pragma("clang attribute pop")

/* Clang shows warning when there aren't any objects to apply pragma.
 * To prevent this warning we define this function inside every macros with pragmas.
 */
#   define DUMMY_FUNCTION_DEFINITION [[maybe_unused]] void _dummy_function_definition();


#define DECLARE_X86_64_V2_SPECIFIC_CODE(...) \
BEGIN_X86_64_V2_SPECIFIC_CODE \
namespace TargetSpecific::x86_64_v2 { \
    DUMMY_FUNCTION_DEFINITION \
    using namespace DB::TargetSpecific::x86_64_v2; \
    __VA_ARGS__ \
} \
END_TARGET_SPECIFIC_CODE

#define DECLARE_X86_64_V3_SPECIFIC_CODE(...) \
BEGIN_X86_64_V3_SPECIFIC_CODE \
namespace TargetSpecific::x86_64_v3 { \
    DUMMY_FUNCTION_DEFINITION \
    using namespace DB::TargetSpecific::x86_64_v3; \
    __VA_ARGS__ \
} \
END_TARGET_SPECIFIC_CODE

#define DECLARE_X86_64_V4_SPECIFIC_CODE(...) \
BEGIN_X86_64_V4_SPECIFIC_CODE \
namespace TargetSpecific::x86_64_v4 { \
    DUMMY_FUNCTION_DEFINITION \
    using namespace DB::TargetSpecific::x86_64_v4; \
    __VA_ARGS__ \
} \
END_TARGET_SPECIFIC_CODE

#define DECLARE_X86_ICELAKE_SPECIFIC_CODE(...) \
BEGIN_X86_64_ICELAKE_SPECIFIC_CODE \
namespace TargetSpecific::x86_64_icelake { \
    DUMMY_FUNCTION_DEFINITION \
    using namespace DB::TargetSpecific::x86_64_icelake; \
    __VA_ARGS__ \
} \
END_TARGET_SPECIFIC_CODE

#define DECLARE_X86_SAPPHIRE_SPECIFIC_CODE(...) \
BEGIN_X86_64_SAPPHIRE_SPECIFIC_CODE \
namespace TargetSpecific::x86_64_sapphirerapids { \
    DUMMY_FUNCTION_DEFINITION \
    using namespace DB::TargetSpecific::x86_64_sapphirerapids; \
    __VA_ARGS__ \
} \
END_TARGET_SPECIFIC_CODE

#else

#define USE_MULTITARGET_CODE 0

/* Multitarget code is disabled, just delete target-specific code.
 */
#define DECLARE_X86_64_V2_SPECIFIC_CODE(...)
#define DECLARE_X86_64_V3_SPECIFIC_CODE(...)
#define DECLARE_X86_64_V4_SPECIFIC_CODE(...)
#define DECLARE_X86_ICELAKE_SPECIFIC_CODE(...)
#define DECLARE_X86_SAPPHIRE_SPECIFIC_CODE(...)

#endif

#define DECLARE_DEFAULT_CODE(...) \
namespace TargetSpecific::Default { \
    using namespace DB::TargetSpecific::Default; \
    __VA_ARGS__ \
}


/// Only enable extra v3 and v4 by default
#define DECLARE_MULTITARGET_CODE(...) \
DECLARE_DEFAULT_CODE         (__VA_ARGS__) \
DECLARE_X86_64_V3_SPECIFIC_CODE    (__VA_ARGS__) \
DECLARE_X86_64_V4_SPECIFIC_CODE   (__VA_ARGS__) \

DECLARE_DEFAULT_CODE(
    constexpr auto BuildArch = TargetArch::Default;
)

DECLARE_X86_64_V2_SPECIFIC_CODE(
    constexpr auto BuildArch = TargetArch::x86_64_v2;
)

DECLARE_X86_64_V3_SPECIFIC_CODE(
    constexpr auto BuildArch = TargetArch::x86_64_v3;
)

DECLARE_X86_64_V4_SPECIFIC_CODE(
    constexpr auto BuildArch = TargetArch::x86_64_v4;
)

DECLARE_X86_ICELAKE_SPECIFIC_CODE(
    constexpr auto BuildArch = TargetArch::x86_64_icelake;
)

DECLARE_X86_SAPPHIRE_SPECIFIC_CODE(
    constexpr auto BuildArch = TargetArch::x86_64_sapphirerapids;
)


/** Runtime Dispatch helpers for class members.
  *
  * Example of usage:
  *
  * class TestClass
  * {
  * public:
  *     MULTITARGET_FUNCTION_X86_V4_V3(
  *     MULTITARGET_FUNCTION_HEADER(int), testFunctionImpl, MULTITARGET_FUNCTION_BODY((int value)
  *     {
  *          return value;
  *     })
  *     )
  *
  *     void testFunction(int value) {
  *         if (isArchSupported(TargetArch::x86_64_v4))
  *         {
  *             testFunctionImpl_x86_64_v4(value);
  *         }
  *         else if (isArchSupported(TargetArch::x86_64_v3))
  *         {
  *             testFunctionImpl_x86_64_v3(value);
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

#define MULTITARGET_FUNCTION_X86_V4_V3(FUNCTION_HEADER, name, FUNCTION_BODY) \
    FUNCTION_HEADER \
    \
    X86_64_V4_FUNCTION_SPECIFIC_ATTRIBUTE \
    name##_x86_64_v4 \
    FUNCTION_BODY \
    \
    FUNCTION_HEADER \
    \
    X86_64_V3_FUNCTION_SPECIFIC_ATTRIBUTE \
    name##_x86_64_v3 \
    FUNCTION_BODY \
    \
    FUNCTION_HEADER \
    \
    name \
    FUNCTION_BODY \

#define MULTITARGET_FUNCTION_X86_V3(FUNCTION_HEADER, name, FUNCTION_BODY) \
    FUNCTION_HEADER \
    \
    X86_64_V3_FUNCTION_SPECIFIC_ATTRIBUTE \
    name##_x86_64_v3 \
    FUNCTION_BODY \
    \
    FUNCTION_HEADER \
    \
    name \
    FUNCTION_BODY \


#else

#define MULTITARGET_FUNCTION_X86_V4_V3(FUNCTION_HEADER, name, FUNCTION_BODY) \
    FUNCTION_HEADER \
    \
    name \
    FUNCTION_BODY \

#define MULTITARGET_FUNCTION_X86_V3(FUNCTION_HEADER, name, FUNCTION_BODY) \
    FUNCTION_HEADER \
    \
    name \
    FUNCTION_BODY \

#endif

}

#if defined(__clang__)
#    pragma clang diagnostic pop
#endif
