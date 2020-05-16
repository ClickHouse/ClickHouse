#pragma once

#include <Core/Types.h>

/// This file contains macros and helpers for writing platform-dependent code.
/// 
/// Macroses DECLARE_<Arch>_SPECIFIC_CODE will wrap code inside them into the 
/// namespace TargetSpecific::<Arch> and enable Arch-specific compile options.
/// Thus, it's allowed to call functions inside these namespaces only after
/// checking platform in runtime (see IsArchSupported() below).
///
/// For similarities there is a macros DECLARE_DEFAULT_CODE, which wraps code
/// into the namespace TargetSpecific::Default but dosn't specify any additional
/// copile options.
/// 
/// Example:
/// 
/// DECLARE_DEFAULT_CODE (
/// int funcImpl() {
///     return 1;
/// }
/// ) // DECLARE_DEFAULT_CODE
/// 
/// DECLARE_AVX2_SPECIFIC_CODE (
/// int funcImpl() {
///     return 2;
/// }
/// ) // DECLARE_DEFAULT_CODE
/// 
/// int func() {
///     if (IsArchSupported(TargetArch::AVX2)) 
///         return TargetSpecifc::AVX2::funcImpl();
///     return TargetSpecifc::Default::funcImpl();
/// } 
/// 
/// Sometimes code may benefit from compiling with different options.
/// For these purposes use DECLARE_MULTITARGET_CODE macros. It will create several
/// copies of the code and compile it with different options. These copies are
/// available via TargetSpecifc namespaces described above.
/// 
/// Inside every TargetSpecific namespace there is a constexpr variable BuildArch, 
/// which indicates the target platform for current code.
/// 
/// Example:
/// 
/// DECLARE_MULTITARGET_CODE(
/// int funcImpl(int size, ...) {
///     int iteration_size = 1;
///     if constexpr (BuildArch == TargetArch::SSE4)
///         iteration_size = 2
///     else if constexpr (BuildArch == TargetArch::AVX || BuildArch == TargetArch::AVX2)
///         iteration_size = 4;
///     else if constexpr (BuildArch == TargetArch::AVX512)
///         iteration_size = 8;
///     for (int i = 0; i < size; i += iteration_size)
///     ...
/// }
/// ) // DECLARE_MULTITARGET_CODE
///
/// // All 5 versions of func are available here. Use runtime detection to choose one.
///
/// If you want to write IFunction or IExecutableFuncionImpl with runtime dispatching, see PerformanceAdaptors.h.

namespace DB
{

enum class TargetArch : UInt32
{
    Default  = 0, // Without any additional compiler options.
    SSE4     = (1 << 0),
    AVX      = (1 << 1),
    AVX2     = (1 << 2),
    AVX512F  = (1 << 3),
};

// Runtime detection.
bool IsArchSupported(TargetArch arch);

String ToString(TargetArch arch);

#if defined(__clang__)
#   define BEGIN_AVX512F_SPECIFIC_CODE \
        _Pragma("clang attribute push (__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt,abm,mmx,avx,avx2,avx512f\"))))")
#   define BEGIN_AVX2_SPECIFIC_CODE \
        _Pragma("clang attribute push (__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt,abm,mmx,avx,avx2\"))))")
#   define BEGIN_AVX_SPECIFIC_CODE \
        _Pragma("clang attribute push (__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt,abm,mmx,avx\"))))")
#   define BEGIN_SSE4_SPECIFIC_CODE \
        _Pragma("clang attribute push (__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt,abm,mmx\"))))")
#   define END_TARGET_SPECIFIC_CODE \
        _Pragma("clang attribute pop")
#elif defined(__GNUC__)
#   define BEGIN_AVX512F_SPECIFIC_CODE \
        _Pragma("GCC push_options") \
        _Pragma("GCC target(\"sse,sse2,sse3,ssse3,sse4,popcnt,abm,mmx,avx,avx2,avx512f,tune=native\")")
#   define BEGIN_AVX2_SPECIFIC_CODE \
        _Pragma("GCC push_options") \
        _Pragma("GCC target(\"sse,sse2,sse3,ssse3,sse4,popcnt,abm,mmx,avx,avx2,tune=native\")")
#   define BEGIN_AVX_SPECIFIC_CODE \
        _Pragma("GCC push_options") \
        _Pragma("GCC target(\"sse,sse2,sse3,ssse3,sse4,popcnt,abm,mmx,avx,tune=native\")")
#   define BEGIN_SSE4_SPECIFIC_CODE \
        _Pragma("GCC push_options") \
        _Pragma("GCC target(\"sse,sse2,sse3,ssse3,sse4,popcnt,abm,mmx,tune=native\")")
#   define END_TARGET_SPECIFIC_CODE \
        _Pragma("GCC pop_options")
#else
#   error "Only CLANG and GCC compilers are supported for vectorized code generation"
#endif

#define DECLARE_DEFAULT_CODE(...) \
namespace TargetSpecific::Default { \
    using namespace DB::TargetSpecific::Default; \
    __VA_ARGS__ \
}

#define DECLARE_SSE4_SPECIFIC_CODE(...) \
BEGIN_SSE4_SPECIFIC_CODE \
namespace TargetSpecific::SSE4 { \
    using namespace DB::TargetSpecific::SSE4; \
    __VA_ARGS__ \
} \
END_TARGET_SPECIFIC_CODE

#define DECLARE_AVX_SPECIFIC_CODE(...) \
BEGIN_AVX_SPECIFIC_CODE \
namespace TargetSpecific::AVX { \
    using namespace DB::TargetSpecific::AVX; \
    __VA_ARGS__ \
} \
END_TARGET_SPECIFIC_CODE

#define DECLARE_AVX2_SPECIFIC_CODE(...) \
BEGIN_AVX2_SPECIFIC_CODE \
namespace TargetSpecific::AVX2 { \
    using namespace DB::TargetSpecific::AVX2; \
    __VA_ARGS__ \
} \
END_TARGET_SPECIFIC_CODE

#define DECLARE_AVX512F_SPECIFIC_CODE(...) \
BEGIN_AVX512F_SPECIFIC_CODE \
namespace TargetSpecific::AVX512F { \
    using namespace DB::TargetSpecific::AVX512F; \
    __VA_ARGS__ \
} \
END_TARGET_SPECIFIC_CODE

#define DECLARE_MULTITARGET_CODE(...) \
DECLARE_DEFAULT_CODE         (__VA_ARGS__) \
DECLARE_SSE4_SPECIFIC_CODE   (__VA_ARGS__) \
DECLARE_AVX_SPECIFIC_CODE    (__VA_ARGS__) \
DECLARE_AVX2_SPECIFIC_CODE   (__VA_ARGS__) \
DECLARE_AVX512F_SPECIFIC_CODE(__VA_ARGS__)

DECLARE_DEFAULT_CODE(
    constexpr auto BuildArch = TargetArch::Default;
) // DECLARE_DEFAULT_CODE

DECLARE_SSE4_SPECIFIC_CODE(
    constexpr auto BuildArch = TargetArch::SSE4;
) // DECLARE_SSE4_SPECIFIC_CODE

DECLARE_AVX_SPECIFIC_CODE(
    constexpr auto BuildArch = TargetArch::AVX;
) // DECLARE_AVX_SPECIFIC_CODE

DECLARE_AVX2_SPECIFIC_CODE(
    constexpr auto BuildArch = TargetArch::AVX2;
) // DECLARE_AVX2_SPECIFIC_CODE

DECLARE_AVX512F_SPECIFIC_CODE(
    constexpr auto BuildArch = TargetArch::AVX512F;
) // DECLARE_AVX512F_SPECIFIC_CODE

}
