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

# one_to_many_asymmetric.cc and dot_product_sse4.cc use SSE4.1 (_mm_cvtepi8_epi32,
# _mm_blend_epi16) and SSSE3 (_mm_shuffle_epi8, _mm_maddubs_epi16) always_inline
# intrinsics.  On SSE2-only builds (amd_compat) the compiler rejects always_inline
# calls to intrinsics that require target features not enabled globally.  Add per-file
# flags so these files are compiled with the SIMD level they actually require.
if (ARCH_AMD64)
    set_source_files_properties(
        "${SCANN_SOURCE_DIR}/scann/distance_measures/one_to_many/one_to_many_asymmetric.cc"
        PROPERTIES COMPILE_OPTIONS "-msse4.1;-mssse3"
    )
    set_source_files_properties(
        "${SCANN_SOURCE_DIR}/scann/distance_measures/one_to_one/dot_product_sse4.cc"
        PROPERTIES COMPILE_OPTIONS "-msse4.1"
    )
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
string(REPLACE
    "#ifdef __x86_64__"
    "#if defined(__x86_64__) && defined(__SSE3__)"
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
# Train the AH codebook on a sampled subset of residuals instead of the full
# dataset.  Upstream ScaNN's ComputeResidualsImpl computes residuals for every
# datapoint (e.g. ~2.9 GB for 1M x 768), which dominates index build time and
# peak memory.  The residuals are used only as the codebook training set
# (asymmetric_hashing2::TrainSingleMachine); the full dataset is still quantized
# point-by-point in BuildLeafSearchers (which re-residualizes each point via
# ResidualizeToFloat), so subsampling only the training set has negligible
# recall impact.  getSampleSize() returns 20% of the dataset, clamped to
# [100,000, 2,000,000].  The AH codebook is a set of 2-D, 16-center k-means whose
# training-set requirement is essentially independent of the dataset size, so 20%
# is ample; the floor keeps small datasets well-trained and the cap keeps build
# time bounded for very large datasets.
#
# Implemented as build-directory patches rather than edits to the scann
# submodule, mirroring the flags.cc / avx512.h patches above.  string(REPLACE)
# uses bracket arguments so the multi-line C++ needs no escaping, and the whole
# ComputeResidualsImpl body is replaced as a single anchor so the in-body
# dataset.size() uses are rewritten while the function's other dataset.size()
# uses are left untouched.

# 1) getSampleSize declaration in dataset.h (header: include order wins).
set(_ds_h_src "${SCANN_SOURCE_DIR}/scann/data_format/dataset.h")
set(_ds_h_dst "${CMAKE_CURRENT_BINARY_DIR}/scann/data_format/dataset.h")
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/scann/data_format")
configure_file("${_ds_h_src}" "${_ds_h_dst}" COPYONLY)
file(READ "${_ds_h_dst}" _ds_h_content)
string(REPLACE
[==[namespace research_scann {

template <typename T>
class TypedDataset;]==]
[==[namespace research_scann {

size_t getSampleSize(size_t data_size);

template <typename T>
class TypedDataset;]==]
    _ds_h_content "${_ds_h_content}")
file(WRITE "${_ds_h_dst}" "${_ds_h_content}")

# 2) getSampleSize definition in dataset.cc.
set(_ds_cc_src "${SCANN_SOURCE_DIR}/scann/data_format/dataset.cc")
set(_ds_cc_dst "${CMAKE_CURRENT_BINARY_DIR}/dataset.cc")
configure_file("${_ds_cc_src}" "${_ds_cc_dst}" COPYONLY)
file(READ "${_ds_cc_dst}" _ds_cc_content)
string(REPLACE
[==[namespace research_scann {

shared_ptr<DocidCollectionInterface> Dataset::ReleaseDocids() {]==]
[==[namespace research_scann {

size_t getSampleSize(size_t data_size) {
  // The AH codebook is 384 independent 2-D, 16-center k-means (num_dims_per_block=2,
  // INT8_LUT16), whose training-set requirement is essentially independent of the
  // dataset size, so a 20% sample is ample. Clamp it on both ends: a floor so small
  // datasets are still trained on enough points (datasets below ~500k end up fully
  // sampled, which is cheap), and a cap so huge datasets do not blow up build time or
  // peak memory (2M samples already saturate the 2-D codebooks).
  size_t sample_size = static_cast<size_t>(data_size / 5);  // 20%
  sample_size = std::max(sample_size, static_cast<size_t>(100000));    // floor
  sample_size = std::min(sample_size, static_cast<size_t>(2000000));   // cap
  sample_size = std::min(sample_size, static_cast<size_t>(data_size));
  return sample_size;
}

shared_ptr<DocidCollectionInterface> Dataset::ReleaseDocids() {]==]
    _ds_cc_content "${_ds_cc_content}")
file(WRITE "${_ds_cc_dst}" "${_ds_cc_content}")
list(REMOVE_ITEM SCANN_SOURCES "${_ds_cc_src}")
list(APPEND SCANN_SOURCES "${_ds_cc_dst}")

# 3) Rewrite ComputeResidualsImpl in tree_ah_hybrid_residual.cc to sample.
set(_tahr_src "${SCANN_SOURCE_DIR}/scann/tree_x_hybrid/tree_ah_hybrid_residual.cc")
set(_tahr_dst "${CMAKE_CURRENT_BINARY_DIR}/tree_ah_hybrid_residual.cc")
configure_file("${_tahr_src}" "${_tahr_dst}" COPYONLY)
file(READ "${_tahr_dst}" _tahr_content)
string(REPLACE
[==[#include <numeric>
#include <unordered_set>]==]
[==[#include <numeric>
#include <random>
#include <unordered_set>]==]
    _tahr_content "${_tahr_content}")
string(REPLACE
[==[  vector<uint32_t> tokens_by_datapoint(dataset.size());
  for (uint32_t token : Seq(datapoints_by_token.size())) {
    for (DatapointIndex dp_idx : datapoints_by_token[token]) {
      tokens_by_datapoint[dp_idx] = token;
    }
  }

  vector<float> residuals_storage;
  auto loop_body = [&](size_t dp_idx, bool is_first_dp)
                       SCANN_INLINE_LAMBDA -> Status {
    const uint32_t token = tokens_by_datapoint[dp_idx];
    SCANN_ASSIGN_OR_RETURN(auto residual, get_residual(dataset[dp_idx], token));

    if (is_first_dp) {
      residuals_storage =
          vector<float>(dataset.size() * residual.dimensionality());
    } else {
      DCHECK_EQ(residuals_storage.size(),
                dataset.size() * residual.dimensionality());
    }
    std::copy(residual.values().begin(), residual.values().end(),
              residuals_storage.begin() + dp_idx * residual.dimensionality());
    return OkStatus();
  };

  if (dataset.empty()) return DenseDataset<float>();
  SCANN_RETURN_IF_ERROR(loop_body(0, true));
  SCANN_RETURN_IF_ERROR(ParallelForWithStatus<1>(
      Seq(1, dataset.size()), parallelization_pool,
      [&](size_t dp_idx) { return loop_body(dp_idx, false); }));
  return DenseDataset<float>(std::move(residuals_storage), dataset.size());
}]==]
[==[  if (dataset.empty()) return DenseDataset<float>();

  vector<uint32_t> tokens_by_datapoint(dataset.size());
  for (uint32_t token : Seq(datapoints_by_token.size())) {
    for (DatapointIndex dp_idx : datapoints_by_token[token]) {
      tokens_by_datapoint[dp_idx] = token;
    }
  }

  // The residuals are used only as the training set for the AH codebook (see
  // TrainSingleMachine in base/internal/tree_x_hybrid_factory.cc), so training
  // on a representative subset rather than the full dataset cuts build time and
  // peak memory roughly in half with negligible recall impact. Quantization of
  // the full dataset happens separately in BuildLeafSearchers and is unaffected.
  const size_t sample_size = getSampleSize(dataset.size());

  // Select sample_size datapoint indices via reservoir sampling. The seed is
  // fixed so the sample (and therefore the codebook) is deterministic.
  vector<DatapointIndex> sample_indices(sample_size);
  {
    std::mt19937 g(2023);
    std::uniform_int_distribution<uint64_t> dis;
    for (size_t i = 0; i < dataset.size(); ++i) {
      if (i < sample_size) {
        sample_indices[i] = i;
      } else {
        uint64_t rdx = dis(
            g, std::uniform_int_distribution<uint64_t>::param_type(0, i));
        if (rdx < sample_size) sample_indices[rdx] = i;
      }
    }
  }

  vector<float> residuals_storage;
  auto loop_body = [&](size_t sample_pos, bool is_first_dp)
                       SCANN_INLINE_LAMBDA -> Status {
    const DatapointIndex dp_idx = sample_indices[sample_pos];
    const uint32_t token = tokens_by_datapoint[dp_idx];
    SCANN_ASSIGN_OR_RETURN(auto residual, get_residual(dataset[dp_idx], token));

    if (is_first_dp) {
      residuals_storage =
          vector<float>(sample_size * residual.dimensionality());
    } else {
      DCHECK_EQ(residuals_storage.size(),
                sample_size * residual.dimensionality());
    }
    std::copy(residual.values().begin(), residual.values().end(),
              residuals_storage.begin() +
                  sample_pos * residual.dimensionality());
    return OkStatus();
  };

  SCANN_RETURN_IF_ERROR(loop_body(0, true));
  SCANN_RETURN_IF_ERROR(ParallelForWithStatus<1>(
      Seq(1, sample_size), parallelization_pool,
      [&](size_t sample_pos) { return loop_body(sample_pos, false); }));
  return DenseDataset<float>(std::move(residuals_storage), sample_size);
}]==]
    _tahr_content "${_tahr_content}")
file(WRITE "${_tahr_dst}" "${_tahr_content}")
list(REMOVE_ITEM SCANN_SOURCES "${_tahr_src}")
list(APPEND SCANN_SOURCES "${_tahr_dst}")

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
# Eigen detects __SSE2__ and includes its SSE PacketMath, which calls
# _mm_loadu_si64 — a newer intrinsic absent from the ppc64le emulation layer.
# _mm_loadu_si64(p) and _mm_loadl_epi64((__m128i const*)(p)) are functionally
# identical (load 64 bits into the low half, zero the high half); the only
# difference is the parameter type (void const* vs __m128i const*).  On ppc64le
# in LE mode, _mm_loadl_epi64 uses *(long long*)ptr which tolerates unaligned
# addresses, matching the semantics of _mm_loadu_si64.  A forced-include compat
# header defines the missing intrinsic via its equivalent so Eigen's SSE2
# vectorization path compiles without disabling SIMD entirely.
if (ARCH_PPC64LE)
    set(_ppc_compat_h "${CMAKE_CURRENT_BINARY_DIR}/scann_ppc64le_compat.h")
    file(WRITE "${_ppc_compat_h}"
        "#pragma once\n"
        "#if defined(__powerpc64__) && defined(__SSE2__) && !defined(_mm_loadu_si64)\n"
        "#  define _mm_loadu_si64(p) _mm_loadl_epi64((__m128i const *)(p))\n"
        "#endif\n"
    )
    target_compile_options(_scann PUBLIC "-include" "${_ppc_compat_h}")
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
