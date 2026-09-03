/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION &
 * AFFILIATES. All rights reserved. SPDX-License-Identifier:
 * LicenseRef-NvidiaProprietary
 *
 * NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
 * property and proprietary rights in and to this material, related
 * documentation and any modifications thereto. Any use, reproduction,
 * disclosure or distribution of this material and related documentation
 * without an express license agreement from NVIDIA CORPORATION or
 * its affiliates is strictly prohibited.
 */

#pragma once

#include <cassert>

#include "nvcomp/shared_types.h"

namespace nvcomp
{

/**
 * @brief Format specification for ANS compression.
 *
 * A single packed byte (kept as 1 byte to enable compressed HLIF buffer compatibility
 * within 5.x); the 3 unused high bits are reserved and kept 0. Use the accessors below.
 */
struct ANSFormatSpecHeader
{
  enum class Mode : uint8_t
  {
    Char = 0,
    Fp16 = 1,
    Fp8 = 2
  };

  // `mode` is a plain 2-bit integer field (not Mode): an enum bitfield trips
  // bitfield-enum-conversion warnings under clang / nvcc 13.x's host pass, and a
  // uint8_t field keeps all bitfields packed in 1 byte. The accessors convert
  // to/from Mode.
  uint8_t sub_chunk_log2 : 3; // max_sub_chunk_count as log2 (0 = auto; 2..6 for 4..64)
  uint8_t mode : 2; // ANS decode mode (Mode)
  uint8_t reserved : 3;

  // max_sub_chunk_count must be 0 (auto) or a power-of-2 in the inclusive range
  // [4, 64]; sub_chunk_log2 is only 3 bits, so any other value would silently
  // truncate. Invalid input asserts in debug and falls back to auto (0) in release
  // rather than writing a corrupt count.
  void set_max_sub_chunk_count(uint8_t count)
  {
    if (count == 0)
    {
      sub_chunk_log2 = 0; // auto
      return;
    }

    const bool is_pow2 = (count & static_cast<uint8_t>(count - 1)) == 0;
    const bool in_range = count >= 4 && count <= 64;
    if (!is_pow2 || !in_range)
    {
      assert(false && "max_sub_chunk_count must be 0 or a power-of-2 in [4, 64]");
      sub_chunk_log2 = 0; // fall back to auto instead of truncating
      return;
    }

    // count is a validated power-of-2 in [4, 64], so log2 is 2..6 (fits in 3 bits).
    uint8_t log2 = 0;
    while ((1u << log2) < count)
    {
      ++log2;
    }
    sub_chunk_log2 = log2;
  }

  uint8_t get_max_sub_chunk_count() const
  {
    return sub_chunk_log2 == 0 ? 0 : static_cast<uint8_t>(1u << sub_chunk_log2);
  }

  // Maps the (char/uchar/fp16/fp8) data type to the 3 ANS decode modes; char and
  // uchar both map to Mode::Char (decompression treats them identically).
  void set_data_type(nvcompType_t data_type)
  {
    Mode m = Mode::Char;
    switch (data_type)
    {
      case NVCOMP_TYPE_FLOAT16:
        m = Mode::Fp16;
        break;
      case NVCOMP_TYPE_FLOAT8_E4M3:
        m = Mode::Fp8;
        break;
      default:
        m = Mode::Char;
        break;
    }
    mode = static_cast<uint8_t>(m);
  }

  nvcompType_t get_data_type() const
  {
    switch (static_cast<Mode>(mode))
    {
      case Mode::Fp16:
        return NVCOMP_TYPE_FLOAT16;
      case Mode::Fp8:
        return NVCOMP_TYPE_FLOAT8_E4M3;
      default:
        return NVCOMP_TYPE_CHAR;
    }
  }
};

static_assert(sizeof(ANSFormatSpecHeader) == 1, "ANSFormatSpecHeader must serialize as a single byte");

/**
 * @brief Format specification for Bitcomp compression
 */
struct BitcompFormatSpecHeader
{
  /**
   * @brief Bitcomp algorithm options.
   *
   * - 0 : Default algorithm, usually gives the best compression ratios
   * - 1 : "Sparse" algorithm, works well on sparse data (with lots of zeroes),
   *        and is usually faster than the default algorithm.
   */
  int algorithm;
  /**
   * @brief One of nvcomp's possible data types
   */
  nvcompType_t data_type;
};

/**
 * @brief Format specification for Cascaded compression
 */
struct CascadedFormatSpecHeader
{
  /**
   * @brief The size of each internal chunk of data to decompress independently
   * with
   *
   * Cascaded compression. The value should be in the range of [512, 16384]
   * depending on the datatype of the input and the shared memory size of
   * the GPU being used.  This is not the size of chunks passed into the API.
   * Recommended size is 4096.
   *
   * @note Not currently used and a default of 4096 is just used.
   */
  size_t internal_chunk_bytes;
  /**
   * @brief The datatype used to define the bit-width for compression
   */
  nvcompType_t type;
  /**
   * @brief The number of Run Length Encodings to perform.
   */
  int num_RLEs;
  /**
   * @brief The number of Delta Encodings to perform.
   */
  int num_deltas;
  /**
   * @brief Whether or not to bitpack the final layers.
   */
  int use_bp;
};

/**
 * @brief Format specification for Deflate compression
 */
struct DeflateFormatSpecHeader
{
  /**
   * @brief Compression algorithm to use.
   *
   * - 0: highest-throughput, entropy-only compression (use for symmetric
   * compression/decompression performance)
   * - 1: high-throughput, low compression ratio (default)
   * - 2: medium-throughput, medium compression ratio, beat Zlib level 1 on the
   * compression ratio
   * - 3: placeholder for further compression level support, will fall into
   * MEDIUM_COMPRESSION at this point
   * - 4: lower-throughput, higher compression ratio, beat Zlib level 6 on the
   * compression ratio
   * - 5: lowest-throughput, highest compression ratio
   */
  int algorithm;
};

/**
 * @brief Format specification for GDeflate compression
 */
struct GdeflateFormatSpecHeader
{
  /**
   * @brief Compression algorithm to use.
   *
   * - 0: highest-throughput, entropy-only compression (use for symmetric
   * compression/decompression performance)
   * - 1: high-throughput, low compression ratio (default)
   * - 2: medium-throughput, medium compression ratio, beat Zlib level 1 on the
   * compression ratio
   * - 3: placeholder for further compression level support, will fall into
   * MEDIUM_COMPRESSION at this point
   * - 4: lower-throughput, higher compression ratio, beat Zlib level 6 on the
   * compression ratio
   * - 5: lowest-throughput, highest compression ratio
   */
  int algorithm;
};

/**
 * @brief Format specification for Gzip compression
 */
struct GzipFormatSpecHeader
{
  /**
   * @brief Compression algorithm to use.
   *
   * - 0: highest-throughput, lowest compression ratio, entropy-only compression (use for symmetric
   * compression/decompression performance)
   * - 1: high-throughput, low compression ratio (default)
   * - 2: medium-throughput, medium compression ratio, beat Zlib level 1 on the
   * compression ratio
   * - 3: placeholder for further compression level support, will fall into
   * MEDIUM_COMPRESSION at this point
   * - 4: lower-throughput, higher compression ratio, beat Zlib level 6 on the
   * compression ratio
   * - 5: lowest-throughput, highest compression ratio
   */
  uint8_t algorithm;
};

/**
 * @brief Format specification for LZ4 compression
 */
struct LZ4FormatSpecHeader
{
  /**
   * @brief LZ4 data type to use.
   */
  union
  {
    nvcompType_t data_type;
    unsigned char bytes[4];
  };
};

/**
 * @brief Format specification for Snappy compression
 */
struct SnappyFormatSpecHeader
{
  // Empty for now
};

/**
 * @brief Format specification for Zstd compression
 */
struct ZstdFormatSpecHeader
{
  // Empty for now
};

} // namespace nvcomp
