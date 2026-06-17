if (NOT TARGET ch_contrib::highway)
    message(${RECONFIGURE_MESSAGE_LEVEL} "Can't use ScaNN without highway")
    return()
endif()

if (NOT TARGET ch_contrib::eigen)
    message(${RECONFIGURE_MESSAGE_LEVEL} "Can't use ScaNN without eigen")
    return()
endif()

set(SCANN_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/scann")

# ---------------------------------------------------------------------------
# 1. Expand .tpl.cc templates at configure time (Bazel does this at build time).
#    Each template substitutes {BATCH_SIZE} with 1..9.
#    Per-file SIMD flags are applied here because each template family targets
#    a different instruction set.
# ---------------------------------------------------------------------------
set(SCANN_TPL_SOURCES)

if (ARCH_AMD64)
    # Per-template SIMD flags — only meaningful on x86.
    set(SCANN_TPL_FLAGS_lut16_sse4               "-msse4.2")
    set(SCANN_TPL_FLAGS_lut16_avx2               "-mavx2;-mfma")
    set(SCANN_TPL_FLAGS_lut16_avx512_prefetch    "-mavx512f;-mavx512dq;-mavx512bw;-mfma")
    set(SCANN_TPL_FLAGS_lut16_avx512_smart       "-mavx512f;-mavx512dq;-mavx512bw;-mfma")
    set(SCANN_TPL_FLAGS_lut16_avx512_noprefetch  "-mavx512f;-mavx512dq;-mavx512bw;-mfma")
    set(SCANN_TPL_FLAGS_lut16_highway            "")  # highway handles its own dispatch
    set(SCANN_TPL_TEMPLATES
        lut16_sse4
        lut16_avx2
        lut16_avx512_prefetch
        lut16_avx512_smart
        lut16_avx512_noprefetch
        lut16_highway
    )
else()
    # On non-x86 platforms only the highway variant is compiled;
    # highway manages its own SIMD dispatch for ARM and other architectures.
    set(SCANN_TPL_TEMPLATES lut16_highway)
endif()

foreach(TPL ${SCANN_TPL_TEMPLATES})
    set(TPL_INPUT "${SCANN_SOURCE_DIR}/scann/hashes/internal/bazel_templates/${TPL}.tpl.cc")
    foreach(BATCH_SIZE RANGE 1 9)
        set(TPL_OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/tpl-out/${TPL}_${BATCH_SIZE}.cc")
        file(READ "${TPL_INPUT}" TPL_CONTENT)
        string(REPLACE "{BATCH_SIZE}" "${BATCH_SIZE}" TPL_CONTENT "${TPL_CONTENT}")
        file(WRITE "${TPL_OUTPUT}" "${TPL_CONTENT}")
        if (ARCH_AMD64 AND SCANN_TPL_FLAGS_${TPL})
            set_source_files_properties("${TPL_OUTPUT}" PROPERTIES
                COMPILE_OPTIONS "${SCANN_TPL_FLAGS_${TPL}}")
        endif()
        list(APPEND SCANN_TPL_SOURCES "${TPL_OUTPUT}")
    endforeach()
endforeach()

# ---------------------------------------------------------------------------
# 2. Generate protobuf sources
# ---------------------------------------------------------------------------
include("${ClickHouse_SOURCE_DIR}/contrib/google-protobuf-cmake/protobuf_generate.cmake")

set(SCANN_PROTO_FILES
    "${SCANN_SOURCE_DIR}/scann/data_format/features.proto"
    "${SCANN_SOURCE_DIR}/scann/distance_measures/one_to_many/scale_encoding.proto"
    "${SCANN_SOURCE_DIR}/scann/partitioning/kmeans_tree_partitioner.proto"
    "${SCANN_SOURCE_DIR}/scann/partitioning/linear_projection_tree.proto"
    "${SCANN_SOURCE_DIR}/scann/partitioning/partitioner.proto"
    "${SCANN_SOURCE_DIR}/scann/proto/auto_tuning.proto"
    "${SCANN_SOURCE_DIR}/scann/proto/brute_force.proto"
    "${SCANN_SOURCE_DIR}/scann/proto/centers.proto"
    "${SCANN_SOURCE_DIR}/scann/proto/crowding.proto"
    "${SCANN_SOURCE_DIR}/scann/proto/disjoint_restrict_token.proto"
    "${SCANN_SOURCE_DIR}/scann/proto/distance_measure.proto"
    "${SCANN_SOURCE_DIR}/scann/proto/exact_reordering.proto"
    "${SCANN_SOURCE_DIR}/scann/proto/hashed.proto"
    "${SCANN_SOURCE_DIR}/scann/proto/hash.proto"
    "${SCANN_SOURCE_DIR}/scann/proto/input_output.proto"
    "${SCANN_SOURCE_DIR}/scann/proto/metadata.proto"
    "${SCANN_SOURCE_DIR}/scann/proto/min_distance.proto"
    "${SCANN_SOURCE_DIR}/scann/proto/partitioning.proto"
    "${SCANN_SOURCE_DIR}/scann/proto/projection.proto"
    "${SCANN_SOURCE_DIR}/scann/proto/results.proto"
    "${SCANN_SOURCE_DIR}/scann/proto/scann.proto"
    "${SCANN_SOURCE_DIR}/scann/scann_ops/scann_assets.proto"
    "${SCANN_SOURCE_DIR}/scann/trees/kmeans_tree/kmeans_tree.proto"
)

# Append scann repo root to IMPORT_DIRS so "scann/proto/foo.proto" imports resolve correctly.
# Generated headers land at ${CMAKE_CURRENT_BINARY_DIR}/scann/proto/foo.pb.h,
# matching the #include paths used in scann source files.
# Back up and restore to avoid polluting the global variables for other modules.
#
# PROTOBUF_GENERATE_CPP_APPEND_PATH must be FALSE: the default (TRUE) adds a per-file
# -I flag for each proto's directory, causing get_proto_relative_path to strip the
# "scann/data_format/" prefix and emit flat "features.pb.h" instead of the path-preserving
# "scann/data_format/features.pb.h" that ScaNN sources expect.
set(_scann_old_append_path ${PROTOBUF_GENERATE_CPP_APPEND_PATH})
set(PROTOBUF_GENERATE_CPP_APPEND_PATH FALSE)
set(_scann_old_protobuf_import_dirs ${Protobuf_IMPORT_DIRS})
list(APPEND Protobuf_IMPORT_DIRS "${SCANN_SOURCE_DIR}")
# google/protobuf/timestamp.proto (well-known types) lives in the protobuf source tree.
list(APPEND Protobuf_IMPORT_DIRS "${ClickHouse_SOURCE_DIR}/contrib/google-protobuf/src")
PROTOBUF_GENERATE_CPP(SCANN_PROTO_SRCS SCANN_PROTO_HDRS ${SCANN_PROTO_FILES})
set(Protobuf_IMPORT_DIRS ${_scann_old_protobuf_import_dirs})
set(PROTOBUF_GENERATE_CPP_APPEND_PATH ${_scann_old_append_path})

# ScaNN production headers include gtest/gtest_prod.h solely for the FRIEND_TEST macro.
# Provide a minimal stub — no test infrastructure is needed.
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/gtest/gtest_prod.h"
    "#pragma once\n#define FRIEND_TEST(tc, tn) friend class tc##_##tn##_Test\n"
)

# ---------------------------------------------------------------------------
# 3. Collect ScaNN C++ source files
# ---------------------------------------------------------------------------
file(GLOB_RECURSE SCANN_SOURCES "${SCANN_SOURCE_DIR}/scann/*.cc")

# Exclude Bazel template originals (expanded above into tpl-out/)
# and TF/Python bindings which are not needed.
file(GLOB_RECURSE SCANN_EXCLUDE_SOURCES
    "${SCANN_SOURCE_DIR}/scann/hashes/internal/bazel_templates/*.tpl.cc"
    # TF kernels / ops / Python bindings are not needed.
    "${SCANN_SOURCE_DIR}/scann/scann_ops/cc/kernels/*.cc"
    "${SCANN_SOURCE_DIR}/scann/scann_ops/cc/ops/*.cc"
    "${SCANN_SOURCE_DIR}/scann/scann_ops/cc/python/*.cc"
    "${SCANN_SOURCE_DIR}/scann/scann_ops/cc/scann_npy.cc"
    # scann.cc (ScannInterface) uses cnpy (numpy I/O) and io_npy.h which require the
    # cnpy library not present in ClickHouse contrib.  ClickHouse integration uses
    # lower-level ScaNN APIs directly and provides its own serialization.
    "${SCANN_SOURCE_DIR}/scann/scann_ops/cc/scann.cc"
    # io_npy.cc depends on the cnpy library (numpy format I/O) which is not in ClickHouse contrib.
    "${SCANN_SOURCE_DIR}/scann/utils/io_npy.cc"
)
list(REMOVE_ITEM SCANN_SOURCES ${SCANN_EXCLUDE_SOURCES})

# kmeans_tree_partitioner.cc calls std::shared_ptr::unique() which was removed in C++20.
# Reverting to -std=c++17 restores the method (deprecated but present in C++17).
# Note: CXX_STANDARD is a target-level property; COMPILE_OPTIONS must be used for per-file overrides.
set_source_files_properties(
    "${SCANN_SOURCE_DIR}/scann/partitioning/kmeans_tree_partitioner.cc"
    PROPERTIES COMPILE_OPTIONS "-std=c++17"
)

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
string(REPLACE
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
# and Avx2 under #ifdef __SSE3__ (line 231), but the extern template instantiation
# macro at line 1787 only checks #ifdef __x86_64__.  On SSE2-only x86-64 builds
# (amd_compat), __SSE3__ is not defined so the function definitions are absent
# while the extern declarations are emitted, causing "no template named" errors.
# Tighten the guard to #if defined(__x86_64__) && defined(__SSE3__).  This is
# correct on all platforms: SSE3+ builds are unaffected; SSE2-only builds skip
# the Avx1/Avx2 extern declarations that reference non-existent definitions.
set(_oto_src "${SCANN_SOURCE_DIR}/scann/distance_measures/one_to_many/one_to_many_symmetric.h")
set(_oto_dst "${CMAKE_CURRENT_BINARY_DIR}/scann/distance_measures/one_to_many/one_to_many_symmetric.h")
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/scann/distance_measures/one_to_many")
configure_file("${_oto_src}" "${_oto_dst}" COPYONLY)
file(READ "${_oto_dst}" _oto_content)
set(_nl "\n")
string(REPLACE
    "#ifdef __x86_64__${_nl}#define SCANN_INSTANTIATE_ONE_TO_MANY("
    "#if defined(__x86_64__) && defined(__SSE3__)${_nl}#define SCANN_INSTANTIATE_ONE_TO_MANY("
    _oto_content "${_oto_content}")
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
    string(REPLACE
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
    string(REPLACE
        "#ifdef __x86_64__"
        "#if defined(__x86_64__) && defined(__linux__)"
        _flags_content "${_flags_content}")
    file(WRITE "${_flags_dst}" "${_flags_content}")
    list(REMOVE_ITEM SCANN_SOURCES "${_flags_src}")
    list(APPEND SCANN_SOURCES "${_flags_dst}")
endif()

# ---------------------------------------------------------------------------
# 4. Build the library
# ---------------------------------------------------------------------------
add_library(_scann
    ${SCANN_SOURCES}
    ${SCANN_TPL_SOURCES}
    ${SCANN_PROTO_SRCS}
)

target_include_directories(_scann SYSTEM PUBLIC
    # The build directory is listed first so patched headers (e.g. avx512.h on
    # macOS) and generated .pb.h files take precedence over the ScaNN sources.
    "${CMAKE_CURRENT_BINARY_DIR}"
    "${SCANN_SOURCE_DIR}"
)

# Required by ScaNN to skip Bazel-only code paths.
target_compile_definitions(_scann PUBLIC SCANN_CMAKE_MODE=1)

# On ppc64le, ClickHouse defines -D__SSE2__=1 for SSE2 emulation via AltiVec.
# Eigen detects __SSE2__ and includes its SSE PacketMath, which uses
# _mm_loadu_si64 — a newer intrinsic absent from the ppc64le emulation layer.
# Suppressing Eigen vectorization avoids the SSE code path.  The trade-off is
# slower Eigen matrix operations (K-means, GMM, PCA, projection) during index
# building on ppc64le; query throughput is unaffected because ScaNN's search
# hot path uses its own highway-based SIMD dispatch.
if (ARCH_PPC64LE)
    target_compile_definitions(_scann PRIVATE EIGEN_DONT_VECTORIZE)
endif()

target_compile_options(_scann PRIVATE -w)

target_link_libraries(_scann PUBLIC
    ch_contrib::abseil_swiss_tables
    ch_contrib::protobuf
    ch_contrib::highway
    ch_contrib::eigen
    Threads::Threads
)

add_library(ch_contrib::scann ALIAS _scann)
