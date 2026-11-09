# one_to_many_asymmetric.cc and the *_sse4.cc distance implementations use SSE3,
# SSSE3, or SSE4.1 always_inline intrinsics.  On SSE2-only builds (amd_compat) the
# compiler rejects calls to intrinsics that require target features not enabled
# globally.  Add per-file flags matching the SIMD level these files require.
if (ARCH_AMD64)
    set_source_files_properties(
        "${SCANN_SOURCE_DIR}/scann/distance_measures/one_to_many/one_to_many_asymmetric.cc"
        PROPERTIES COMPILE_OPTIONS "-msse4.1;-mssse3"
    )
    set_source_files_properties(
        "${SCANN_SOURCE_DIR}/scann/distance_measures/one_to_one/dot_product_sse4.cc"
        "${SCANN_SOURCE_DIR}/scann/distance_measures/one_to_one/l1_distance_sse4.cc"
        "${SCANN_SOURCE_DIR}/scann/distance_measures/one_to_one/l2_distance_sse4.cc"
        PROPERTIES COMPILE_OPTIONS "-msse4.1"
    )
endif()

# In debug builds Highway does not expose constexpr lane counts, so ScaNN aliases its
# `highway` namespace to `fallback`. The non-x86 dispatch calls
# `highway::OneToManyInt8FloatImpl`, but upstream only provides fallback implementations
# for `BFloat16` and `UInt4`. Add the missing `Int8` implementation to a generated header copy;
# release builds with Highway SIMD still use the original optimized implementation.
set(_asymmetric_src "${SCANN_SOURCE_DIR}/scann/distance_measures/one_to_many/one_to_many_asymmetric.h")
set(_asymmetric_dst "${CMAKE_CURRENT_BINARY_DIR}/scann/distance_measures/one_to_many/one_to_many_asymmetric.h")
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/scann/distance_measures/one_to_many")
configure_file("${_asymmetric_src}" "${_asymmetric_dst}" COPYONLY)
file(READ "${_asymmetric_dst}" _asymmetric_content)
scann_checked_replace(
    "namespace fallback {\n\ntemplate <bool kHasIndices, bool kIsSquaredL2"
    "namespace fallback {\n\n#include \"scann_one_to_many_int8_fallback.h\"\n\ntemplate <bool kHasIndices, bool kIsSquaredL2"
    _asymmetric_content "${_asymmetric_content}")
file(WRITE "${_asymmetric_dst}" "${_asymmetric_content}")

# ScaNN's SCANN_SSE4 attribute is empty and relies on Bazel applying SSE4.1
# flags to every translation unit that instantiates the corresponding header
# templates.  That assumption does not hold for amd_compat.  Give only these
# functions an SSE4.1 target instead of raising the ISA level of general source
# files such as reordering_helper.cc.
if (ARCH_AMD64)
    set(_attributes_src "${SCANN_SOURCE_DIR}/scann/utils/intrinsics/attributes.h")
    set(_attributes_dst "${CMAKE_CURRENT_BINARY_DIR}/scann/utils/intrinsics/attributes.h")
    file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/scann/utils/intrinsics")
    configure_file("${_attributes_src}" "${_attributes_dst}" COPYONLY)
    file(READ "${_attributes_dst}" _attributes_content)
    scann_checked_replace(
        "#include <x86intrin.h>\n\n#define SCANN_SSE4"
        "#include <x86intrin.h>\n\n#define SCANN_SSE4 __attribute((target(\"sse4.1\")))"
        _attributes_content "${_attributes_content}")
    file(WRITE "${_attributes_dst}" "${_attributes_content}")

    # These wrappers call SCANN_SSE4_INLINE methods and must carry the same target.
    set(_horizontal_sum_src "${SCANN_SOURCE_DIR}/scann/utils/intrinsics/horizontal_sum.h")
    set(_horizontal_sum_dst "${CMAKE_CURRENT_BINARY_DIR}/scann/utils/intrinsics/horizontal_sum.h")
    configure_file("${_horizontal_sum_src}" "${_horizontal_sum_dst}" COPYONLY)
    file(READ "${_horizontal_sum_dst}" _horizontal_sum_content)
    scann_checked_replace(
        "SCANN_INLINE float HorizontalSum(Sse4<float>"
        "SCANN_SSE4_INLINE float HorizontalSum(Sse4<float>"
        _horizontal_sum_content "${_horizontal_sum_content}")
    scann_checked_replace(
        "SCANN_INLINE double HorizontalSum(Sse4<double>"
        "SCANN_SSE4_INLINE double HorizontalSum(Sse4<double>"
        _horizontal_sum_content "${_horizontal_sum_content}")
    scann_checked_replace(
        "SCANN_INLINE void HorizontalSum2X(Sse4<"
        "SCANN_SSE4_INLINE void HorizontalSum2X(Sse4<"
        _horizontal_sum_content "${_horizontal_sum_content}")
    scann_checked_replace(
        "SCANN_INLINE void HorizontalSum3X(Sse4<"
        "SCANN_SSE4_INLINE void HorizontalSum3X(Sse4<"
        _horizontal_sum_content "${_horizontal_sum_content}")
    scann_checked_replace(
        "SCANN_INLINE void HorizontalSum4X(Sse4<"
        "SCANN_SSE4_INLINE void HorizontalSum4X(Sse4<"
        _horizontal_sum_content "${_horizontal_sum_content}")
    file(WRITE "${_horizontal_sum_dst}" "${_horizontal_sum_content}")
endif()

# scale_encoding_helpers.cc calls absl::little_endian::Load32(encoded.end() - sizeof(float))
# where encoded is absl::string_view.  When ABSL_USES_STD_STRING_VIEW is active (C++17+),
# absl::string_view is aliased to std::string_view, whose const_iterator is
# __wrap_iter<const char*> under ClickHouse's libc++ ABI.  __wrap_iter<const char*> has no
# implicit conversion to const void*, so the call fails to compile.
#
# Fix: replace encoded.end() - sizeof(float) with encoded.data() + encoded.size() - sizeof(float),
# which yields a plain const char* that converts to const void* without issue.
# The patched copy lives only in the build directory; the submodule source is not modified.
set(_scale_enc_src "${SCANN_SOURCE_DIR}/scann/utils/scale_encoding_helpers.cc")
set(_scale_enc_dst "${CMAKE_CURRENT_BINARY_DIR}/scale_encoding_helpers.cc")
configure_file("${_scale_enc_src}" "${_scale_enc_dst}" COPYONLY)
file(READ "${_scale_enc_dst}" _scale_enc_content)
scann_checked_replace(
    "Load32(encoded.end() - sizeof(float))"
    "Load32(encoded.data() + encoded.size() - sizeof(float))"
    _scale_enc_content "${_scale_enc_content}")
file(WRITE "${_scale_enc_dst}" "${_scale_enc_content}")
list(REMOVE_ITEM SCANN_SOURCES "${_scale_enc_src}")
list(APPEND SCANN_SOURCES "${_scale_enc_dst}")

# many_to_many_sfp8.cc uses Intel AMX tile instructions (Sapphire Rapids+).
# The build host does not support AMX; exclude this file.
# SFP8 is an optional distance format; ScaNN's core functionality is unaffected.
list(REMOVE_ITEM SCANN_SOURCES
    "${SCANN_SOURCE_DIR}/scann/distance_measures/many_to_many/many_to_many_sfp8.cc"
)

# one_to_many_symmetric.h defines DenseAccumulatingDistanceMeasureOneToManyInternalAvx1
# and Avx2 under #ifdef __SSE3__ (line 231), but three call sites and the extern
# template instantiation macro only check #ifdef __x86_64__.  On SSE2-only x86-64
# builds (amd_compat), __SSE3__ is not defined so the function definitions are absent
# while the call sites and extern declarations reference them, causing
# "undeclared identifier" and "no template named" errors at lines 645, 674, and 1787.
# Tighten all three guards to #if defined(__x86_64__) && defined(__SSE3__).
set(_oto_src "${SCANN_SOURCE_DIR}/scann/distance_measures/one_to_many/one_to_many_symmetric.h")
set(_oto_dst "${CMAKE_CURRENT_BINARY_DIR}/scann/distance_measures/one_to_many/one_to_many_symmetric.h")
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/scann/distance_measures/one_to_many")
configure_file("${_oto_src}" "${_oto_dst}" COPYONLY)
file(READ "${_oto_dst}" _oto_content)
scann_checked_replace(
    "#ifdef __x86_64__"
    "#if defined(__x86_64__) && defined(__SSE3__)"
    _oto_content "${_oto_content}" 3)
file(WRITE "${_oto_dst}" "${_oto_content}")

# On macOS, alignof(__m512i) may be less than 64 due to ABI differences,
# causing the static_assert(alignof(result) >= 64) in int8_tile.h to fail for
# all files that transitively include it (via sfp8_transposed.h).  Patch a copy
# of avx512.h in the build directory to add alignas(kRegisterBytes) to the
# registers_ member.  The build directory is placed before the ScaNN source
# directory in include paths so the patched copy is found first.
if (OS_DARWIN)
    set(_avx512_src "${SCANN_SOURCE_DIR}/scann/utils/intrinsics/avx512.h")
    set(_avx512_dst "${CMAKE_CURRENT_BINARY_DIR}/scann/utils/intrinsics/avx512.h")
    file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/scann/utils/intrinsics")
    configure_file("${_avx512_src}" "${_avx512_dst}" COPYONLY)
    file(READ "${_avx512_dst}" _avx512_content)
    scann_checked_replace(
        "  std::conditional_t<kNumRegisters == 1, IntelType, Avx512<T, 1>>"
        "  alignas(kRegisterBytes) std::conditional_t<kNumRegisters == 1, IntelType, Avx512<T, 1>>"
        _avx512_content "${_avx512_content}")
    file(WRITE "${_avx512_dst}" "${_avx512_content}")
endif()

# On FreeBSD (and any non-Linux OS), TryEnableAmx() in flags.cc calls
# syscall(SYS_arch_prctl, ...) which is Linux-specific and missing on FreeBSD.
# Patch a copy of flags.cc in the build directory to restrict that syscall to
# Linux by replacing the x86_64-only guard with a Linux+x86_64 guard.
if (NOT OS_LINUX)
    set(_flags_src "${SCANN_SOURCE_DIR}/scann/utils/intrinsics/flags.cc")
    set(_flags_dst "${CMAKE_CURRENT_BINARY_DIR}/flags.cc")
    configure_file("${_flags_src}" "${_flags_dst}" COPYONLY)
    file(READ "${_flags_dst}" _flags_content)
    scann_checked_replace(
        "#ifdef __x86_64__"
        "#if defined(__x86_64__) && defined(__linux__)"
        _flags_content "${_flags_content}")
    file(WRITE "${_flags_dst}" "${_flags_content}")
    list(REMOVE_ITEM SCANN_SOURCES "${_flags_src}")
    list(APPEND SCANN_SOURCES "${_flags_dst}")
endif()

# scann_cpu_info.cc: the x86 CPUID detection block is guarded by PLATFORM_IS_X86, which is
# defined for the _scann target below. That block was copied from TensorFlow and uses TF
# platform names (`string`, `uint32`, `uint64`, `CHECK`) that ClickHouse's TF-free build does
# not provide, so it never compiled before (and runtime SIMD detection silently fell back to
# SSE4). Inject the missing aliases so the detection actually compiles and enables AVX2/AVX-512.
set(_ci_cc_src "${SCANN_SOURCE_DIR}/scann/oss_wrappers/scann_cpu_info.cc")
set(_ci_cc_dst "${CMAKE_CURRENT_BINARY_DIR}/scann_cpu_info.cc")
configure_file("${_ci_cc_src}" "${_ci_cc_dst}" COPYONLY)
file(READ "${_ci_cc_dst}" _ci_cc_content)
scann_checked_replace(
[==[#include "scann/oss_wrappers/scann_cpu_info.h"]==]
[==[#include "scann/oss_wrappers/scann_cpu_info.h"

#include <cstdint>
#include <string>
#include "absl/log/check.h"
namespace research_scann {
using std::string;
using uint32 = uint32_t;
using uint64 = uint64_t;
}  // namespace research_scann]==]
    _ci_cc_content "${_ci_cc_content}")
file(WRITE "${_ci_cc_dst}" "${_ci_cc_content}")
list(REMOVE_ITEM SCANN_SOURCES "${_ci_cc_src}")
list(APPEND SCANN_SOURCES "${_ci_cc_dst}")
