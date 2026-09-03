/*
 * SPDX-FileCopyrightText: Copyright (c) 2021-2025 NVIDIA CORPORATION & AFFILIATES.
 * All rights reserved. SPDX-License-Identifier: LicenseRef-NvidiaProprietary
 *
 * NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
 * property and proprietary rights in and to this material, related
 * documentation and any modifications thereto. Any use, reproduction,
 * disclosure or distribution of this material and related documentation
 * without an express license agreement from NVIDIA CORPORATION or
 * its affiliates is strictly prohibited.
*/

#pragma once

#include <cstddef>
#include <cstdint>

#include <nvcomp_export.h>

namespace gdeflate
{

/**
 * @brief The maximum supported uncompressed chunk size in bytes for the Gdeflate CPU compressor.
 */
static constexpr size_t nvcompGdeflateCPUCompressionMaxAllowedChunkSize = 1u << 16;

/**
 * @brief The minimum and maximum compression level allowed for the Gdeflate CPU compressor.
 */
static constexpr int nvcompGdeflateCPUMinCompressionLevel = 0;
static constexpr int nvcompGdeflateCPUMaxCompressionLevel = 12;

/**
 * @brief The most restrictive of the minimum alignment requirements for void-type memory buffers
 * used for input and output, passed to compression functions.
 *
 * @note In all cases, typed memory buffers must still be aligned to their type's size,
 * e.g., 4 bytes for `int`.
 */
static constexpr size_t nvcompGdeflateCPURequiredCompressionAlignment = 1;

/**
 * @brief Get the maximum size that a chunk of size at most max_uncompressed_chunk_bytes
 * could compress to. That is, the minimum amount of output memory required to be given
 * \ref compressCPU for each chunk.
 *
 * @throw nvcomp::NVCompException on error.
 *
 * @param[in] max_uncompressed_chunk_bytes The maximum size of a chunk before compression.
 * @param[out] max_compressed_chunk_bytes The maximum possible compressed size of the chunk.
 */
NVCOMP_EXPORT
void compressCPUGetMaxOutputChunkSize(size_t max_uncompressed_chunk_bytes, size_t *max_compressed_chunk_bytes);

/**
 * @brief Perform compression on the CPU.
 *
 * @throw nvcomp::NVCompException on error.
 *
 * @param[in] in_ptr Pointers on the CPU, to uncompressed batched items.
 * @param[in] in_bytes The size of each uncompressed batch item on the CPU.
 * @param[in] max_uncompressed_chunk_bytes The maximum size of an uncompressed chunk in bytes.
 * @param[in] batch_size The number of batch items.
 * @param[out] out_ptr Pointers on the CPU, to the output location for each compressed batch item (output).
 * @param[out] out_bytes The compressed size of each chunk on the CPU (output).
 * @param[in] level The selected compression level (between 0 and 12, both inclusive).
 */
NVCOMP_EXPORT
void compressCPU(
  const void *const *in_ptr,
  const size_t *in_bytes,
  const size_t max_uncompressed_chunk_bytes,
  size_t batch_size,
  void *const *out_ptr,
  size_t *out_bytes,
  int level = 12
);

/**
 * @brief The most restrictive of the minimum alignment requirements for void-type memory buffers
 * used for input and output, passed to decompression functions.
 *
 * @note In all cases, typed memory buffers must still be aligned to their type's size,
 * e.g., 4 bytes for `int`.
 */
static constexpr size_t nvcompGdeflateCPURequiredDecompressionAlignment = 1;

/**
 * @brief Perform decompression on the CPU.
 *
 * @throw nvcomp::NVCompException on error.
 *
 * @param[in] in_ptr Pointers on the CPU, to the compressed chunks.
 * @param[in] in_bytes The size of each compressed batch item on the CPU in bytes.
 * @param[in] batch_size The number of batch items.
 * @param[out] out_ptr Pointers on the CPU, indicating where to decompress each chunk (output).
 * @param[in] out_buffer_bytes The size of each output chunk buffer in bytes.
 * @param[out] out_bytes The size of each decompressed batch item on the CPU in bytes. (output).
 */
NVCOMP_EXPORT
void decompressCPU(
  const void *const *in_ptr,
  const size_t *in_bytes,
  size_t batch_size,
  void *const *out_ptr,
  size_t *out_buffer_bytes,
  size_t *out_bytes
);

} // namespace gdeflate
