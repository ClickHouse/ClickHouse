// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// =============================================================================
// Direct reproduction of Arrow's ByteStreamSplit using xsimd.
// Copy-pasted from arrow/util/byte_stream_split_internal.h with minimal
// adaptation (replaced Arrow-specific utilities).
//
// Build:
//   clang++ -std=c++20 -O3 -mavx2 -I/path/to/xsimd/include \
//           bss_benchmark_xsimd.cpp -o bss_benchmark_xsimd
// =============================================================================

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <vector>

#include <xsimd/xsimd.hpp>

// -----------------------------------------------------------------------------
// Replacements for Arrow internal utilities
// -----------------------------------------------------------------------------

#define ARROW_LITTLE_ENDIAN 1

namespace arrow::util {

template <typename T>
inline T SafeLoadAs(const uint8_t* unaligned) {
  T ret;
  std::memcpy(&ret, unaligned, sizeof(T));
  return ret;
}

inline void SafeStore(uint8_t* unaligned, uint64_t value) {
  std::memcpy(unaligned, &value, sizeof(uint64_t));
}

}  // namespace arrow::util

// Arrow's ReversePow2: returns log2(v) for power-of-two v, 0 otherwise
constexpr int ReversePow2(int v) {
  if (v <= 0 || (v & (v - 1)) != 0) return 0;
  int r = 0;
  while (v > 1) { v >>= 1; ++r; }
  return r;
}

// Arrow's SizedInt: maps byte count to integer type
template <int N> struct SizedIntHelper;
template <> struct SizedIntHelper<1> { using type = int8_t; };
template <> struct SizedIntHelper<2> { using type = int16_t; };
template <> struct SizedIntHelper<4> { using type = int32_t; };
template <> struct SizedIntHelper<8> { using type = int64_t; };
template <int N> using SizedInt = typename SizedIntHelper<N>::type;

// =============================================================================
// Arrow's xsimd ByteStreamSplit implementation — copied verbatim where possible
// =============================================================================

namespace arrow::util::internal {

// ---------------------------------------------------------------------------
// DECODE (from arrow/util/byte_stream_split_internal.h)
// ---------------------------------------------------------------------------

template <typename Arch, int kNumStreams>
void ByteStreamSplitDecodeSimd(const uint8_t* data, int width, int64_t num_values,
                                int64_t stride, uint8_t* out) {
  using simd_batch = xsimd::batch<int8_t, Arch>;
  constexpr int kBatchSize = static_cast<int>(simd_batch::size);

  static_assert(kBatchSize >= 16, "The smallest SIMD size is 128 bits");

  if constexpr (kBatchSize > 16) {
    if (num_values < kBatchSize) {
      using Arch128 = typename xsimd::make_sized_batch_t<int8_t, 16>::arch_type;
      return ByteStreamSplitDecodeSimd<Arch128, kNumStreams>(data, width, num_values,
                                                              stride, out);
    }
  }

  static_assert(kNumStreams <= kBatchSize,
                "The algorithm works when the number of streams is smaller than the SIMD "
                "batch size.");
  assert(width == kNumStreams);
  constexpr int kNumStreamsLog2 = ReversePow2(kNumStreams);
  static_assert(kNumStreamsLog2 != 0,
                "The algorithm works for a number of streams being a power of two.");
  constexpr int64_t kBlockSize = kBatchSize * kNumStreams;

  const int64_t size = num_values * kNumStreams;
  const int64_t num_blocks = size / kBlockSize;

  // First handle suffix.
  const int64_t num_processed_elements = (num_blocks * kBlockSize) / kNumStreams;
  for (int64_t i = num_processed_elements; i < num_values; ++i) {
    uint8_t gathered_byte_data[kNumStreams];
    for (int b = 0; b < kNumStreams; ++b) {
      const int64_t byte_index = b * stride + i;
      gathered_byte_data[b] = data[byte_index];
    }
    std::memcpy(out + i * kNumStreams, gathered_byte_data, kNumStreams);
  }

  constexpr int kNumStreamsHalf = kNumStreams / 2U;

  for (int64_t block_index = 0; block_index < num_blocks; ++block_index) {
    simd_batch stage[kNumStreamsLog2 + 1][kNumStreams];

    for (int i = 0; i < kNumStreams; ++i) {
      stage[0][i] =
          simd_batch::load_unaligned(&data[block_index * kBatchSize + i * stride]);
    }

    for (int step = 0; step < kNumStreamsLog2; ++step) {
      for (int i = 0; i < kNumStreamsHalf; ++i) {
        stage[step + 1U][i * 2] =
            xsimd::zip_lo(stage[step][i], stage[step][kNumStreamsHalf + i]);
        stage[step + 1U][i * 2 + 1U] =
            xsimd::zip_hi(stage[step][i], stage[step][kNumStreamsHalf + i]);
      }
    }

    for (int i = 0; i < kNumStreams; ++i) {
      xsimd::store_unaligned(
          reinterpret_cast<int8_t*>(out + (block_index * kNumStreams + i) * kBatchSize),
          stage[kNumStreamsLog2][i]);
    }
  }
}

// ---------------------------------------------------------------------------
// ENCODE helpers: zip_lo_n / zip_hi_n (from Arrow)
// ---------------------------------------------------------------------------

template <typename Arch, int kNumBytes>
auto zip_lo_n(const xsimd::batch<int8_t, Arch>& a,
              const xsimd::batch<int8_t, Arch>& b) -> xsimd::batch<int8_t, Arch> {
  using simd_batch = xsimd::batch<int8_t, Arch>;
  constexpr int kBatchSize = static_cast<int>(simd_batch::size);

  if constexpr (kNumBytes == kBatchSize) {
    return a;
  } else if constexpr (kNumBytes <= 8) {
    return xsimd::bitwise_cast<int8_t>(
        xsimd::zip_lo(xsimd::bitwise_cast<SizedInt<kNumBytes>>(a),
                      xsimd::bitwise_cast<SizedInt<kNumBytes>>(b)));
  } else if constexpr (kNumBytes == 16 && kBatchSize == 32) {
    return xsimd::bitwise_cast<int8_t>(
        xsimd::shuffle(xsimd::bitwise_cast<int64_t>(a), xsimd::bitwise_cast<int64_t>(b),
                       xsimd::batch_constant<uint64_t, Arch, 0, 1, 4, 5>{}));
  }
}

template <typename Arch, int kNumBytes>
auto zip_hi_n(const xsimd::batch<int8_t, Arch>& a,
              const xsimd::batch<int8_t, Arch>& b) -> xsimd::batch<int8_t, Arch> {
  using simd_batch = xsimd::batch<int8_t, Arch>;
  constexpr int kBatchSize = static_cast<int>(simd_batch::size);

  if constexpr (kNumBytes == kBatchSize) {
    return b;
  } else if constexpr (kNumBytes <= 8) {
    return xsimd::bitwise_cast<int8_t>(
        xsimd::zip_hi(xsimd::bitwise_cast<SizedInt<kNumBytes>>(a),
                      xsimd::bitwise_cast<SizedInt<kNumBytes>>(b)));
  } else if constexpr (kNumBytes == 16 && kBatchSize == 32) {
    return xsimd::bitwise_cast<int8_t>(
        xsimd::shuffle(xsimd::bitwise_cast<int64_t>(a), xsimd::bitwise_cast<int64_t>(b),
                       xsimd::batch_constant<uint64_t, Arch, 2, 3, 6, 7>{}));
  }
}

// ---------------------------------------------------------------------------
// ENCODE (from arrow/util/byte_stream_split_internal.h)
// ---------------------------------------------------------------------------

template <typename Arch, int kNumStreams>
void ByteStreamSplitEncodeSimd(const uint8_t* raw_values, int width,
                                const int64_t num_values, uint8_t* output_buffer_raw) {
  using simd_batch = xsimd::batch<int8_t, Arch>;
  constexpr int kBatchSize = static_cast<int>(simd_batch::size);

  static_assert(kBatchSize >= 16, "The smallest SIMD size is 128 bits");

  if constexpr (kBatchSize > 16) {
    if (num_values < kBatchSize) {
      using Arch128 = typename xsimd::make_sized_batch_t<int8_t, 16>::arch_type;
      return ByteStreamSplitEncodeSimd<Arch128, kNumStreams>(
          raw_values, width, num_values, output_buffer_raw);
    }
  }

  assert(width == kNumStreams);
  static_assert(kNumStreams <= kBatchSize,
                "The algorithm works when the number of streams is smaller than the SIMD "
                "batch size.");
  constexpr int kBlockSize = kBatchSize * kNumStreams;
  static_assert(ReversePow2(kNumStreams) != 0,
                "The algorithm works for a number of streams being a power of two.");

  const int64_t size = num_values * kNumStreams;
  const int64_t num_blocks = size / kBlockSize;
  int8_t* output_buffer_streams[kNumStreams];
  for (int i = 0; i < kNumStreams; ++i) {
    output_buffer_streams[i] =
        reinterpret_cast<int8_t*>(&output_buffer_raw[num_values * i]);
  }

  // First handle suffix.
  const int64_t num_processed_elements = (num_blocks * kBlockSize) / kNumStreams;
  for (int64_t i = num_processed_elements; i < num_values; ++i) {
    for (int j = 0; j < kNumStreams; ++j) {
      const uint8_t byte_in_value = raw_values[i * kNumStreams + j];
      output_buffer_raw[j * num_values + i] = byte_in_value;
    }
  }

  constexpr int kNumValuesInBatch = kBatchSize / kNumStreams;
  static_assert(kNumValuesInBatch > 0);
  constexpr int kNumBytes = 2 * kNumValuesInBatch;
  constexpr int kNumStepsByte = ReversePow2(kNumValuesInBatch) + 1;
  constexpr int kNumStepsLarge = ReversePow2(static_cast<int>(kBatchSize) / kNumBytes);
  constexpr int kNumSteps = kNumStepsByte + kNumStepsLarge;
  static_assert(kNumSteps == ReversePow2(kBatchSize));

  constexpr int kNumStreamsHalf = kNumStreams / 2;

  for (int64_t block_index = 0; block_index < num_blocks; ++block_index) {
    simd_batch stage[kNumSteps + 1][kNumStreams];

    for (int i = 0; i < kNumStreams; ++i) {
      stage[0][i] = simd_batch::load_unaligned(
          &raw_values[(block_index * kNumStreams + i) * kBatchSize]);
    }

    // Phase 1: byte-level zips on pairs
    for (int i = 0; i < kNumStreamsHalf; ++i) {
      for (int step = 0; step < kNumStepsByte; ++step) {
        stage[step + 1][i * 2] =
            xsimd::zip_lo(stage[step][i * 2], stage[step][i * 2 + 1]);
        stage[step + 1][i * 2 + 1] =
            xsimd::zip_hi(stage[step][i * 2], stage[step][i * 2 + 1]);
      }
    }

    // Phase 2: large-type zips on halves
    for (int step = kNumStepsByte; step < kNumSteps; ++step) {
      for (int i = 0; i < kNumStreamsHalf; ++i) {
        stage[step + 1][i * 2] =
            zip_lo_n<Arch, kNumBytes>(stage[step][i], stage[step][i + kNumStreamsHalf]);
        stage[step + 1][i * 2 + 1] =
            zip_hi_n<Arch, kNumBytes>(stage[step][i], stage[step][i + kNumStreamsHalf]);
      }
    }

    for (int i = 0; i < kNumStreams; ++i) {
      xsimd::store_unaligned(&output_buffer_streams[i][block_index * kBatchSize],
                             stage[kNumSteps][i]);
    }
  }
}

// ---------------------------------------------------------------------------
// Scalar implementations (identical to Arrow)
// ---------------------------------------------------------------------------

inline void DoSplitStreams(const uint8_t* src, int width, int64_t nvalues,
                           uint8_t** dest_streams) {
  constexpr int kBlockSize = 32;

  while (nvalues >= kBlockSize) {
    for (int stream = 0; stream < width; ++stream) {
      uint8_t* dest = dest_streams[stream];
      for (int i = 0; i < kBlockSize; i += 8) {
        uint64_t a = src[stream + i * width];
        uint64_t b = src[stream + (i + 1) * width];
        uint64_t c = src[stream + (i + 2) * width];
        uint64_t d = src[stream + (i + 3) * width];
        uint64_t e = src[stream + (i + 4) * width];
        uint64_t f = src[stream + (i + 5) * width];
        uint64_t g = src[stream + (i + 6) * width];
        uint64_t h = src[stream + (i + 7) * width];
#if ARROW_LITTLE_ENDIAN
        uint64_t r = a | (b << 8) | (c << 16) | (d << 24) | (e << 32) | (f << 40) |
                     (g << 48) | (h << 56);
#else
        uint64_t r = (a << 56) | (b << 48) | (c << 40) | (d << 32) | (e << 24) |
                     (f << 16) | (g << 8) | h;
#endif
        arrow::util::SafeStore(&dest[i], r);
      }
      dest_streams[stream] += kBlockSize;
    }
    src += width * kBlockSize;
    nvalues -= kBlockSize;
  }

  for (int stream = 0; stream < width; ++stream) {
    uint8_t* dest = dest_streams[stream];
    for (int64_t i = 0; i < nvalues; ++i) {
      dest[i] = src[stream + i * width];
    }
  }
}

inline void DoMergeStreams(const uint8_t** src_streams, int width, int64_t nvalues,
                           uint8_t* dest) {
  constexpr int kBlockSize = 128;

  while (nvalues >= kBlockSize) {
    for (int stream = 0; stream < width; ++stream) {
      const uint8_t* src = src_streams[stream];
      for (int i = 0; i < kBlockSize; i += 8) {
        uint64_t v = arrow::util::SafeLoadAs<uint64_t>(&src[i]);
#if ARROW_LITTLE_ENDIAN
        dest[stream + i * width] = static_cast<uint8_t>(v);
        dest[stream + (i + 1) * width] = static_cast<uint8_t>(v >> 8);
        dest[stream + (i + 2) * width] = static_cast<uint8_t>(v >> 16);
        dest[stream + (i + 3) * width] = static_cast<uint8_t>(v >> 24);
        dest[stream + (i + 4) * width] = static_cast<uint8_t>(v >> 32);
        dest[stream + (i + 5) * width] = static_cast<uint8_t>(v >> 40);
        dest[stream + (i + 6) * width] = static_cast<uint8_t>(v >> 48);
        dest[stream + (i + 7) * width] = static_cast<uint8_t>(v >> 56);
#else
        dest[stream + i * width] = static_cast<uint8_t>(v >> 56);
        dest[stream + (i + 1) * width] = static_cast<uint8_t>(v >> 48);
        dest[stream + (i + 2) * width] = static_cast<uint8_t>(v >> 40);
        dest[stream + (i + 3) * width] = static_cast<uint8_t>(v >> 32);
        dest[stream + (i + 4) * width] = static_cast<uint8_t>(v >> 24);
        dest[stream + (i + 5) * width] = static_cast<uint8_t>(v >> 16);
        dest[stream + (i + 6) * width] = static_cast<uint8_t>(v >> 8);
        dest[stream + (i + 7) * width] = static_cast<uint8_t>(v);
#endif
      }
      src_streams[stream] += kBlockSize;
    }
    dest += width * kBlockSize;
    nvalues -= kBlockSize;
  }

  for (int stream = 0; stream < width; ++stream) {
    const uint8_t* src = src_streams[stream];
    for (int64_t i = 0; i < nvalues; ++i) {
      dest[stream + i * width] = src[i];
    }
  }
}

template <int kNumStreams>
void ByteStreamSplitEncodeScalar(const uint8_t* raw_values, int width,
                                 const int64_t num_values, uint8_t* out) {
  assert(width == kNumStreams);
  std::array<uint8_t*, kNumStreams> dest_streams;
  for (int stream = 0; stream < kNumStreams; ++stream) {
    dest_streams[stream] = &out[stream * num_values];
  }
  DoSplitStreams(raw_values, kNumStreams, num_values, dest_streams.data());
}

inline void ByteStreamSplitEncodeScalarDynamic(const uint8_t* raw_values, int width,
                                               const int64_t num_values, uint8_t* out) {
  // Arrow uses SmallVector<uint8_t*, 16> here; std::vector is equivalent for bench
  std::vector<uint8_t*> dest_streams(width);
  for (int stream = 0; stream < width; ++stream) {
    dest_streams[stream] = &out[stream * num_values];
  }
  DoSplitStreams(raw_values, width, num_values, dest_streams.data());
}

template <int kNumStreams>
void ByteStreamSplitDecodeScalar(const uint8_t* data, int width, int64_t num_values,
                                 int64_t stride, uint8_t* out) {
  assert(width == kNumStreams);
  std::array<const uint8_t*, kNumStreams> src_streams;
  for (int stream = 0; stream < kNumStreams; ++stream) {
    src_streams[stream] = &data[stream * stride];
  }
  DoMergeStreams(src_streams.data(), kNumStreams, num_values, out);
}

inline void ByteStreamSplitDecodeScalarDynamic(const uint8_t* data, int width,
                                               int64_t num_values, int64_t stride,
                                               uint8_t* out) {
  std::vector<const uint8_t*> src_streams(width);
  for (int stream = 0; stream < width; ++stream) {
    src_streams[stream] = &data[stream * stride];
  }
  DoMergeStreams(src_streams.data(), width, num_values, out);
}

// ---------------------------------------------------------------------------
// SimdDispatch wrappers — pick best available arch at compile time
// Arrow does runtime dispatch; we just target AVX2 directly.
// ---------------------------------------------------------------------------

using TargetArch = xsimd::avx2;

template <int kNumStreams>
void ByteStreamSplitEncodeSimdDispatch(const uint8_t* raw_values, int width,
                                        const int64_t num_values, uint8_t* out) {
  return ByteStreamSplitEncodeSimd<TargetArch, kNumStreams>(raw_values, width, num_values,
                                                            out);
}

template <int kNumStreams>
void ByteStreamSplitDecodeSimdDispatch(const uint8_t* data, int width,
                                        int64_t num_values, int64_t stride,
                                        uint8_t* out) {
  return ByteStreamSplitDecodeSimd<TargetArch, kNumStreams>(data, width, num_values,
                                                            stride, out);
}

// ---------------------------------------------------------------------------
// Top-level dispatch (matches Arrow exactly)
// ---------------------------------------------------------------------------

inline void ByteStreamSplitEncode(const uint8_t* raw_values, int width,
                                  const int64_t num_values, uint8_t* out) {
  switch (width) {
    case 1:
      std::memcpy(out, raw_values, num_values);
      return;
    case 2:
      return ByteStreamSplitEncodeSimdDispatch<2>(raw_values, width, num_values, out);
    case 4:
      return ByteStreamSplitEncodeSimdDispatch<4>(raw_values, width, num_values, out);
    case 8:
      return ByteStreamSplitEncodeSimdDispatch<8>(raw_values, width, num_values, out);
    case 16:
      return ByteStreamSplitEncodeScalar<16>(raw_values, width, num_values, out);
  }
  return ByteStreamSplitEncodeScalarDynamic(raw_values, width, num_values, out);
}

inline void ByteStreamSplitDecode(const uint8_t* data, int width, int64_t num_values,
                                  int64_t stride, uint8_t* out) {
  switch (width) {
    case 1:
      std::memcpy(out, data, num_values);
      return;
    case 2:
      return ByteStreamSplitDecodeSimdDispatch<2>(data, width, num_values, stride, out);
    case 4:
      return ByteStreamSplitDecodeSimdDispatch<4>(data, width, num_values, stride, out);
    case 8:
      return ByteStreamSplitDecodeSimdDispatch<8>(data, width, num_values, stride, out);
    case 16:
      return ByteStreamSplitDecodeScalar<16>(data, width, num_values, stride, out);
  }
  return ByteStreamSplitDecodeScalarDynamic(data, width, num_values, stride, out);
}

}  // namespace arrow::util::internal

// =============================================================================
//  Benchmark harness
// =============================================================================

namespace bss = arrow::util::internal;
using Clock = std::chrono::high_resolution_clock;

static void fill_pattern(std::vector<uint8_t>& buf, uint64_t seed) {
  uint64_t s = seed;
  for (auto& b : buf) {
    s = s * 6364136223846793005ULL + 1442695040888963407ULL;
    b = static_cast<uint8_t>(s >> 56);
  }
}

static bool verify_toplevel_one(int W, int64_t N, const char* label) {
  const int64_t sz = N * W;
  std::vector<uint8_t> original(sz), encoded(sz, 0xCD), decoded(sz, 0xEF);
  fill_pattern(original, static_cast<uint64_t>(W) * 1000 + N);

  bss::ByteStreamSplitEncode(original.data(), W, N, encoded.data());
  bss::ByteStreamSplitDecode(encoded.data(), W, N, N, decoded.data());

  if (memcmp(original.data(), decoded.data(), sz) != 0) {
    for (int64_t i = 0; i < sz; ++i) {
      if (original[i] != decoded[i]) {
        printf("    FAIL  W=%-3d  N=%-6lld  %-35s  mismatch@%lld: %02x!=%02x\n", W,
               (long long)N, label, (long long)i, original[i], decoded[i]);
        return false;
      }
    }
  }
  printf("    OK    W=%-3d  N=%-6lld  %s\n", W, (long long)N, label);
  return true;
}

static bool verify_toplevel() {
  printf("=== Correctness: top-level Encode/Decode ===\n");
  bool ok = true;
  for (int W : {1, 2, 4, 8, 16}) {
    ok &= verify_toplevel_one(W, 1, "N=1");
    ok &= verify_toplevel_one(W, 2, "N=2");
    ok &= verify_toplevel_one(W, 7, "N=7 (odd)");
    ok &= verify_toplevel_one(W, 103, "N=103 (prime)");
    ok &= verify_toplevel_one(W, 256, "N=256 (aligned)");
    ok &= verify_toplevel_one(W, 257, "N=257 (aligned+1)");
    ok &= verify_toplevel_one(W, 1024, "N=1024");
    printf("\n");
  }
  for (int W : {3, 5, 6, 7, 9, 10, 12, 20, 32}) {
    ok &= verify_toplevel_one(W, 1, "N=1 (dynamic)");
    ok &= verify_toplevel_one(W, 103, "N=103 (dynamic)");
    ok &= verify_toplevel_one(W, 512, "N=512 (dynamic)");
    printf("\n");
  }
  return ok;
}

template <int W>
static bool verify_scalar_templated(int64_t N, const char* label) {
  const int64_t sz = N * W;
  std::vector<uint8_t> original(sz), encoded(sz, 0xCD), decoded(sz, 0xEF);
  fill_pattern(original, static_cast<uint64_t>(W) * 2000 + N);

  bss::ByteStreamSplitEncodeScalar<W>(original.data(), W, N, encoded.data());
  bss::ByteStreamSplitDecodeScalar<W>(encoded.data(), W, N, N, decoded.data());

  if (memcmp(original.data(), decoded.data(), sz) != 0) {
    for (int64_t i = 0; i < sz; ++i) {
      if (original[i] != decoded[i]) {
        printf("    FAIL  Scalar<%d>  N=%-6lld  %-30s  mismatch@%lld\n", W, (long long)N,
               label, (long long)i);
        return false;
      }
    }
  }
  printf("    OK    Scalar<%d>  N=%-6lld  %s\n", W, (long long)N, label);
  return true;
}

static bool verify_scalar_dynamic(int W, int64_t N, const char* label) {
  const int64_t sz = N * W;
  std::vector<uint8_t> original(sz), encoded(sz, 0xCD), decoded(sz, 0xEF);
  fill_pattern(original, static_cast<uint64_t>(W) * 3000 + N);

  bss::ByteStreamSplitEncodeScalarDynamic(original.data(), W, N, encoded.data());
  bss::ByteStreamSplitDecodeScalarDynamic(encoded.data(), W, N, N, decoded.data());

  if (memcmp(original.data(), decoded.data(), sz) != 0) {
    for (int64_t i = 0; i < sz; ++i) {
      if (original[i] != decoded[i]) {
        printf("    FAIL  Dynamic W=%-3d  N=%-6lld  %-30s  mismatch@%lld\n", W,
               (long long)N, label, (long long)i);
        return false;
      }
    }
  }
  printf("    OK    Dynamic W=%-3d  N=%-6lld  %s\n", W, (long long)N, label);
  return true;
}

static bool verify_scalar_all() {
  printf("=== Correctness: scalar paths ===\n");
  bool ok = true;
  for (int64_t N : {1, 7, 103, 256, 1024}) {
    ok &= verify_scalar_templated<2>(N, "Scalar<2>");
    ok &= verify_scalar_templated<4>(N, "Scalar<4>");
    ok &= verify_scalar_templated<8>(N, "Scalar<8>");
    ok &= verify_scalar_templated<16>(N, "Scalar<16>");
  }
  printf("\n");
  for (int W : {2, 3, 4, 5, 7, 8, 10, 16, 20, 32}) {
    for (int64_t N : {1, 103, 512}) {
      ok &= verify_scalar_dynamic(W, N, "ScalarDynamic");
    }
  }
  printf("\n");
  return ok;
}

template <int W>
static bool verify_simd_one(int64_t N, const char* label) {
  const int64_t sz = N * W;
  std::vector<uint8_t> original(sz), encoded(sz, 0xCD), decoded(sz, 0xEF);
  fill_pattern(original, static_cast<uint64_t>(W) * 4000 + N);

  bss::ByteStreamSplitEncodeSimdDispatch<W>(original.data(), W, N, encoded.data());
  bss::ByteStreamSplitDecodeSimdDispatch<W>(encoded.data(), W, N, N, decoded.data());

  if (memcmp(original.data(), decoded.data(), sz) != 0) {
    for (int64_t i = 0; i < sz; ++i) {
      if (original[i] != decoded[i]) {
        printf("    FAIL  SIMD<%d>  N=%-6lld  %-30s  mismatch@%lld\n", W, (long long)N,
               label, (long long)i);
        return false;
      }
    }
  }
  printf("    OK    SIMD<%d>  N=%-6lld  %s\n", W, (long long)N, label);
  return true;
}

static bool verify_simd_all() {
  printf("=== Correctness: SIMD paths ===\n");
  bool ok = true;
  for (int64_t N : {1, 2, 3, 7, 15, 16, 17, 31, 32, 33, 103, 256, 257, 1024}) {
    ok &= verify_simd_one<2>(N, "SIMD<2>");
    ok &= verify_simd_one<4>(N, "SIMD<4>");
    ok &= verify_simd_one<8>(N, "SIMD<8>");
  }
  printf("\n");
  return ok;
}

static bool verify_cross_one(int W, int64_t N) {
  const int64_t sz = N * W;
  std::vector<uint8_t> original(sz);
  std::vector<uint8_t> enc_toplevel(sz, 0), enc_dynamic(sz, 0);
  fill_pattern(original, static_cast<uint64_t>(W) * 5000 + N);

  bss::ByteStreamSplitEncode(original.data(), W, N, enc_toplevel.data());
  bss::ByteStreamSplitEncodeScalarDynamic(original.data(), W, N, enc_dynamic.data());

  if (memcmp(enc_toplevel.data(), enc_dynamic.data(), sz) != 0) {
    printf("    FAIL  Cross W=%-3d N=%-6lld  toplevel vs dynamic encode mismatch\n", W,
           (long long)N);
    return false;
  }

  std::vector<uint8_t> dec(sz, 0xEF);
  bss::ByteStreamSplitDecode(enc_toplevel.data(), W, N, N, dec.data());
  if (memcmp(original.data(), dec.data(), sz) != 0) {
    printf("    FAIL  Cross W=%-3d N=%-6lld  decode mismatch\n", W, (long long)N);
    return false;
  }

  printf("    OK    Cross W=%-3d N=%-6lld\n", W, (long long)N);
  return true;
}

static bool verify_cross() {
  printf("=== Cross-validation: scalar vs top-level ===\n");
  bool ok = true;
  for (int W : {1, 2, 4, 8, 16, 3, 5, 7, 10, 20}) {
    for (int64_t N : {1, 103, 1024}) {
      ok &= verify_cross_one(W, N);
    }
  }
  printf("\n");
  return ok;
}

static bool verify_stride() {
  printf("=== Stride test ===\n");
  bool ok = true;
  for (int W : {2, 4, 8}) {
    const int64_t N_full = 256;
    const int64_t N_sub = 100;
    const int64_t sz_full = N_full * W;
    const int64_t sz_sub = N_sub * W;

    std::vector<uint8_t> original(sz_full);
    std::vector<uint8_t> encoded(sz_full, 0);
    fill_pattern(original, static_cast<uint64_t>(W) * 6000);

    bss::ByteStreamSplitEncode(original.data(), W, N_full, encoded.data());

    std::vector<uint8_t> decoded_sub(sz_sub, 0xEF);
    bss::ByteStreamSplitDecode(encoded.data(), W, N_sub, N_full, decoded_sub.data());

    if (memcmp(original.data(), decoded_sub.data(), sz_sub) != 0) {
      printf("    FAIL  Stride W=%-3d  N_full=%lld  N_sub=%lld\n", W, (long long)N_full,
             (long long)N_sub);
      ok = false;
    } else {
      printf("    OK    Stride W=%-3d  N_full=%lld  N_sub=%lld\n", W, (long long)N_full,
             (long long)N_sub);
    }
  }
  printf("\n");
  return ok;
}

static bool verify_edge_cases() {
  printf("=== Edge cases ===\n");
  bool ok = true;
  {
    uint8_t dummy_in[16] = {}, dummy_out[16] = {};
    bss::ByteStreamSplitEncode(dummy_in, 4, 0, dummy_out);
    bss::ByteStreamSplitDecode(dummy_in, 4, 0, 0, dummy_out);
    printf("    OK    N=0 (no crash)\n");
  }
  {
    const int64_t N = 128;
    std::vector<uint8_t> src(N), dst(N, 0);
    fill_pattern(src, 9999);
    bss::ByteStreamSplitEncode(src.data(), 1, N, dst.data());
    if (memcmp(src.data(), dst.data(), N) == 0) {
      printf("    OK    W=1 encode == memcpy\n");
    } else {
      printf("    FAIL  W=1 encode != memcpy\n");
      ok = false;
    }
    std::vector<uint8_t> dec(N, 0);
    bss::ByteStreamSplitDecode(dst.data(), 1, N, N, dec.data());
    if (memcmp(src.data(), dec.data(), N) == 0) {
      printf("    OK    W=1 decode == memcpy\n");
    } else {
      printf("    FAIL  W=1 decode != memcpy\n");
      ok = false;
    }
  }
  printf("\n");
  return ok;
}

// =============================================================================
//  Benchmark infrastructure
// =============================================================================

struct Stats { double min_gbps, med_gbps; };

using EncodeFn = void (*)(const uint8_t*, int, int64_t, uint8_t*);
using DecodeFn = void (*)(const uint8_t*, int, int64_t, int64_t, uint8_t*);

static Stats bench_encode(EncodeFn fn, int W, int64_t data_size, int rounds, int inner) {
  const int64_t N = data_size / W;
  const int64_t sz = N * W;
  std::vector<uint8_t> src(sz, 0xAB), dst(sz, 0);

  for (int i = 0; i < 3; ++i) fn(src.data(), W, N, dst.data());

  std::vector<double> results;
  results.reserve(rounds);
  for (int r = 0; r < rounds; ++r) {
    auto t0 = Clock::now();
    for (int i = 0; i < inner; ++i) fn(src.data(), W, N, dst.data());
    auto t1 = Clock::now();
    double sec = std::chrono::duration<double>(t1 - t0).count() / inner;
    results.push_back((sz / 1e9) / sec);
  }
  std::sort(results.begin(), results.end());
  return {results.front(), results[results.size() / 2]};
}

static Stats bench_decode(DecodeFn fn, int W, int64_t data_size, int rounds, int inner) {
  const int64_t N = data_size / W;
  const int64_t sz = N * W;
  std::vector<uint8_t> src(sz, 0xAB), encoded(sz, 0), decoded(sz, 0);

  bss::ByteStreamSplitEncode(src.data(), W, N, encoded.data());

  for (int i = 0; i < 3; ++i) fn(encoded.data(), W, N, N, decoded.data());

  std::vector<double> results;
  results.reserve(rounds);
  for (int r = 0; r < rounds; ++r) {
    auto t0 = Clock::now();
    for (int i = 0; i < inner; ++i) fn(encoded.data(), W, N, N, decoded.data());
    auto t1 = Clock::now();
    double sec = std::chrono::duration<double>(t1 - t0).count() / inner;
    results.push_back((sz / 1e9) / sec);
  }
  std::sort(results.begin(), results.end());
  return {results.front(), results[results.size() / 2]};
}

static void print_row(const char* label, Stats s) {
  printf("    %-32s  min=%6.1f  med=%6.1f GB/s\n", label, s.min_gbps, s.med_gbps);
}

static void encode_toplevel_wrapper(const uint8_t* in, int w, int64_t n, uint8_t* out) {
  bss::ByteStreamSplitEncode(in, w, n, out);
}
static void decode_toplevel_wrapper(const uint8_t* in, int w, int64_t n, int64_t s,
                                    uint8_t* out) {
  bss::ByteStreamSplitDecode(in, w, n, s, out);
}

int main() {
  bool ok = true;
  ok &= verify_toplevel();
  ok &= verify_scalar_all();
  ok &= verify_simd_all();
  ok &= verify_cross();
  ok &= verify_stride();
  ok &= verify_edge_cases();

  if (!ok) {
    printf("Correctness failures detected — aborting.\n");
    return 1;
  }
  printf("All correctness tests passed.\n\n");

  const int64_t DATA_SIZE = 256LL << 20;
  const int rounds = 10, inner = 4;

  printf("============================================================\n");
  printf("  Benchmark: %lld MiB data, %d rounds × %d inner\n",
         (long long)(DATA_SIZE >> 20), rounds, inner);
  printf("============================================================\n\n");

  printf("--- Top-level dispatch (ByteStreamSplitEncode/Decode) ---\n");
  for (int W : {1, 2, 4, 8, 16, 20, 32, 64, 128}) {
    printf("  W=%d:\n", W);
    print_row("Encode (top-level)",
              bench_encode(encode_toplevel_wrapper, W, DATA_SIZE, rounds, inner));
    print_row("Decode (top-level)",
              bench_decode(decode_toplevel_wrapper, W, DATA_SIZE, rounds, inner));
  }
  printf("\n");

  return 0;
}