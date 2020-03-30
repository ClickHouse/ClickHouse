#pragma once

namespace DB::DynamicTarget
{

enum class TargetArch : int {
    Scalar,
    SSE4,
    AVX,
    AVX2,
    AVX512,
};

#if defined(__GNUC__)
// TODO: There are lots of different AVX512 :(
#   define BEGIN_AVX512_SPECIFIC_CODE \
        _Pragma("GCC push_options") \
        _Pragma("GCC target(\"sse,sse2,sse3,ssse3,sse4,popcnt,abm,mmx,avx,avx2,tune=native\")")
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
#elif defined(__clang__)
// TODO: There are lots of different AVX512 :(
#   define BEGIN_AVX512_SPECIFIC_CODE \
        _Pragma("clang attribute push (__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt,abm,mmx,avx,avx2\"))))")
#   define BEGIN_AVX2_SPECIFIC_CODE \
        _Pragma("clang attribute push (__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt,abm,mmx,avx,avx2\"))))")
#   define BEGIN_AVX_SPECIFIC_CODE \
        _Pragma("clang attribute push (__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt,abm,mmx,avx\"))))")
#   define BEGIN_SSE4_SPECIFIC_CODE \
        _Pragma("clang attribute push (__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt,abm,mmx\"))))")
#   define END_TARGET_SPECIFIC_CODE \
        _Pragma("clang attribute pop")
#else
#   error "Only CLANG and GCC compilers are supported"
#endif

#define DECLARE_DEFAULT_CODE (...) \
namespace TargetSpecific::Default { \
    __VA_ARGS__ \
}

#define DECLARE_SSE4_SPECIFIC_CODE (...) \
BEGIN_SSE4_SPECIFIC_CODE \
namespace TargetSpecific::SSE4 { \
    __VA_ARGS__ \
} \
END_TARGET_SPECIFIC_CODE

#define DECLARE_AVX_SPECIFIC_CODE (...) \
BEGIN_AVX_SPECIFIC_CODE \
namespace TargetSpecific::AVX { \
    __VA_ARGS__ \
} \
END_TARGET_SPECIFIC_CODE

#define DECLARE_AVX2_SPECIFIC_CODE (...) \
BEGIN_AVX2_SPECIFIC_CODE \
namespace TargetSpecific::AVX2 { \
    __VA_ARGS__ \
} \
END_TARGET_SPECIFIC_CODE

#define DECLARE_AVX512_SPECIFIC_CODE (...) \
BEGIN_AVX512_SPECIFIC_CODE \
namespace TargetSpecific::AVX512 { \
    __VA_ARGS__ \
} \
END_TARGET_SPECIFIC_CODE

#define DYNAMIC_CODE (...) \
DECLARE_DEFAULT_CODE        (__VA_ARGS__) \
DECLARE_SSE4_SPECIFIC_CODE  (__VA_ARGS__) \
DECLARE_AVX_SPECIFIC_CODE   (__VA_ARGS__) \
DECLARE_AVX2_SPECIFIC_CODE  (__VA_ARGS__) \
DECLARE_AVX512_SPECIFIC_CODE(__VA_ARGS__)

DECLARE_DEFAULT_CODE(
    constexpr auto BuildArch = TargetArch::Scalar;
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

DECLARE_AVX512_SPECIFIC_CODE(
    constexpr auto BuildArch = TargetArch::AVX512;
) // DECLARE_AVX512_SPECIFIC_CODE

} // namespace DB::DynamicTarget