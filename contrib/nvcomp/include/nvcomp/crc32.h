/*
 * SPDX-FileCopyrightText: Copyright (c) 2017-2025 NVIDIA CORPORATION & AFFILIATES.
 * All rights reserved. SPDX-License-Identifier: LicenseRef-NvidiaProprietary
 *
 * NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
 * property and proprietary rights in and to this material, related
 * documentation and any modifications thereto. Any use, reproduction,
 * disclosure or distribution of this material and related documentation
 * without an express license agreement from NVIDIA CORPORATION or
 * its affiliates is strictly prohibited.
*/

#ifndef NVCOMP_CRC32_H
#define NVCOMP_CRC32_H

#include "nvcomp.h"

#ifndef __cplusplus
#include <stdbool.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief CRC32 model specification.
 */
typedef struct
{
  /**
   * @brief Polynomial used for CRC calculation.
   */
  uint32_t poly;
  /**
   * @brief Initial value for CRC shift register.
   */
  uint32_t init;
  /**
   * @brief Flag indicating whether input bytes should be reflected.
   */
  bool ref_in;
  /**
   * @brief Flag indicating whether the final CRC value should be reflected.
   * 
   * The reflection is done before XOR-ing with @ref xorout.
   */
  bool ref_out;
  /**
   * @brief Value with which to XOR the final CRC result.
   *
   * If @ref ref_in is true, the XOR operation is applied after the final CRC
   * value is reflected.
   */
  uint32_t xorout;
  /**
   * @brief These bytes are unused and must be zeroed. This ensures
   *        compatibility if additional fields are added in the future.
   */
  char reserved[16];
} nvcompCRC32Spec_t;

/**
 * @brief Standard CRC32 (aka CRC-32/PKZIP) model preset.
 */
static const nvcompCRC32Spec_t nvcompCRC32 = {0x04C11DB7, 0xFFFFFFFF, true, true, 0xFFFFFFFF, {0}};
/**
 * @brief CRC32-C (aka CRC-32/ISCSI) model preset.
 */
static const nvcompCRC32Spec_t nvcompCRC32_C = {0x1EDC6F41, 0xFFFFFFFF, true, true, 0xFFFFFFFF, {0}};
/**
 * @brief CRC32-D (aka CRC-32/BASE91-D) model preset.
 */
static const nvcompCRC32Spec_t nvcompCRC32_D = {0xA833982B, 0xFFFFFFFF, true, true, 0xFFFFFFFF, {0}};
/**
 * @brief CRC32-Q (aka CRC-32/AIXM) model preset.
 */
static const nvcompCRC32Spec_t nvcompCRC32_Q = {0x814141AB, 0x00000000, false, false, 0x00000000, {0}};
/**
 * @brief CRC-32/MEF model preset.
 */
static const nvcompCRC32Spec_t nvcompCRC32_MEF = {0x741B8CD7, 0xFFFFFFFF, true, true, 0x00000000, {0}};
/**
 * @brief CRC-32/XFER model preset.
 */
static const nvcompCRC32Spec_t nvcompCRC32_XFER = {0x000000AF, 0x00000000, false, false, 0x00000000, {0}};
/**
 * @brief CRC-32/BZIP2 (aka CRC-32/AAL-5) model preset.
 */
static const nvcompCRC32Spec_t nvcompCRC32_BZIP2 = {0x04C11DB7, 0xFFFFFFFF, false, false, 0xFFFFFFFF, {0}};
/**
 * @brief CRC-32/POSIX (aka CRC-32/CKSUM) model preset.
 */
static const nvcompCRC32Spec_t nvcompCRC32_POSIX = {0x04C11DB7, 0x00000000, false, false, 0xFFFFFFFF, {0}};
/**
 * @brief CRC-32/JAMCRC model preset.
 */
static const nvcompCRC32Spec_t nvcompCRC32_JAMCRC = {0x04C11DB7, 0xFFFFFFFF, true, true, 0x00000000, {0}};
/**
 * @brief CRC-32/MPEG-2 model preset.
 */
static const nvcompCRC32Spec_t nvcompCRC32_MPEG_2 = {0x04C11DB7, 0xFFFFFFFF, false, false, 0x00000000, {0}};
/**
 * @brief CRC-32/AUTOSAR model preset.
 */
static const nvcompCRC32Spec_t nvcompCRC32_AUTOSAR = {0xF4ACFB13, 0xFFFFFFFF, true, true, 0xFFFFFFFF, {0}};
/**
 * @brief CRC-32/CD-ROM-EDC model preset.
 */
static const nvcompCRC32Spec_t nvcompCRC32_CD_ROM_EDC = {0x8001801B, 0x00000000, true, true, 0x00000000, {0}};

/**
 * @brief Enumeration of kernel kinds for CRC32 computation.
 */
typedef enum nvcompCRC32KernelKind_t
{
  /**
   * @brief Let each warp process its own chunk of input data.
   */
  nvcompCRC32WarpKernel = 0,
  /**
   * @brief Let one or more blocks process each chunk of input data.
   */
  nvcompCRC32BlockKernel = 1,
} nvcompCRC32KernelKind_t;

/**
 * @brief Configuration for CRC32 kernel execution.
 */
typedef struct
{
  /**
   * @brief Type of kernel to use for CRC32 computation.
   */
  nvcompCRC32KernelKind_t kernel_kind;
  /**
   * @brief Number of bytes each thread read in each processing step.
   */
  int32_t bytes_per_read;
  /**
   * @brief Number of thread blocks to use per message.
   * 
   * Only relevant if @ref kernel_kind is @ref nvcompCRC32BlockKernel. Ignored if
   * @ref kernel_kind is @ref nvcompCRC32WarpKernel.
   */
  int32_t blocks_per_msg;
  /**
   * @brief These bytes are unused and must be zeroed. This ensures
   *        compatibility if additional fields are added in the future.
   */
  char reserved[20];
} nvcompCRC32KernelConf_t;

/**
 * @brief Options for batched CRC32 computation.
 */
typedef struct
{
  /**
   * @brief The CRC32 specification to use.
   */
  nvcompCRC32Spec_t spec;
  /**
   * @brief The kernel configuration to use.
   */
  nvcompCRC32KernelConf_t kernel_conf;
  /**
   * @brief These bytes are unused and must be zeroed. This ensures
   *        compatibility if additional fields are added in the future.
   */
  char reserved[64];
} nvcompBatchedCRC32Opts_t;

/**
 * @brief Enumeration specifying segment types for streaming CRC32 computation.
 */
typedef enum nvcompCRC32SegmentKind_t
{
  /**
   * @brief Single segment (complete message).
   */
  nvcompCRC32OnlySegment = 0,
  /**
   * @brief First segment of a message that may be followed by further segments.
   */
  nvcompCRC32FirstSegment,
  /**
   * @brief Non-first segment of a message that may be followed by further segments.
   */
  nvcompCRC32MidSegment,
  /**
   * @brief Last segment of a message.
   *
   * If the segment is also the first segment, @ref nvcompCRC32OnlySegment
   * should be used instead.
   *
   * This enumerator can also be used to retroactively mark the last processed
   * segment as the last segment of a message. For details, see @ref
   * nvcompBatchedCRC32Async.
   */
  nvcompCRC32LastSegment,
} nvcompCRC32SegmentKind_t;

/**
 * @brief Perform CRC32 checksum calculation asynchronously.
 *
 * All pointers must point to device-accessible locations.
 *
 * This function supports streaming CRC32 computation, where the input data
 * might not be visible all at once but only in individual segments. This is
 * controlled by the @p segment_kind parameter. See @ref
 * nvcompCRC32SegmentKind_t for details. If the input data nevertheless is
 * visible all at once,
 * @ref nvcompCRC32OnlySegment should be passed as @p segment_kind. If a segment
 * is processed as if it may be followed by further segments, but it
 * subsequently turns out to have been the last segment, the CRC32 calculation
 * can be finalized by passing a null pointer as @p device_input_chunk_ptrs and
 * @ref nvcompCRC32LastSegment as @p segment_kind.
 *
 * @note The length of a chunk is allowed to be zero. Length-zero chunks may be
 * useful in situations where the number of segments is message-dependent. Rather
 * than having to perform potentially complicated input and output permutations,
 * the missing chunks can be represented as length-zero chunks.
 *
 * @param[in] device_input_chunk_ptrs Array with size \p num_chunks of pointers
 * to the input data chunks. Both the pointers and the input data should reside
 * in device-accessible memory. The data chunks do not have any alignment
 * requirements.
 * @param[in] device_input_chunk_bytes Array with size \p num_chunks of sizes of
 * the input chunks in bytes. The sizes should reside in device-accessible
 * memory.
 * @param[in] num_chunks The number of chunks to compute checksums of.
 * @param[out] device_crc32_ptr Array with size \p num_chunks on the GPU to be
 * filled with the CRC32 checksum of each chunk.
 * @param[in] opts The CRC32 options.
 * @param[in] segment_kind The @ref nvcompCRC32SegmentKind_t to use.
 * @param[out] device_statuses Array with size \p num_chunks of statuses in
 * device-accessible memory. For each chunk the status will be set to
 * `nvcompSuccess` if the CRC32 calculation is successful, or an error code
 * otherwise. Can be NULL if desired, in which case error status is not
 * reported.
 * @param[in] stream The CUDA stream to operate on.
 *
 * @return nvcompSuccess if successfully launched, and an error code otherwise.
 */
NVCOMP_EXPORT
nvcompStatus_t nvcompBatchedCRC32Async(
  const void *const *device_input_chunk_ptrs,
  const size_t *device_input_chunk_bytes,
  size_t num_chunks,
  uint32_t *device_crc32_ptr,
  nvcompBatchedCRC32Opts_t opts,
  nvcompCRC32SegmentKind_t segment_kind,
  nvcompStatus_t *device_statuses,
  cudaStream_t stream
);

/**
 * @brief Value to pass as @p device_input_chunk_bytes to @ref
 * nvcompBatchedCRC32GetHeuristicConf when specifying the maximum input chunk
 * size in @p max_input_chunk_bytes.
 * 
 * Equal to a null pointer.
 */
static const size_t *const nvcompCRC32IgnoredInputChunkBytes = NULL;

/**
 * @brief Value to pass as @p max_input_chunk_bytes to @ref
 * nvcompBatchedCRC32GetHeuristicConf to indicate that max input chunk bytes
 * should be deduced from @p device_input_chunk_bytes.
 * 
 * Equal to 0.
 */
static const size_t nvcompCRC32DeducedMaxInputChunkBytes = 0;

/**
 * @brief Heuristically determine a performant kernel configuration for CRC32
 * computation based on input data characteristics.
 *
 * This function is particularly useful when all chunks are of a similar size,
 * both within and across @ref nvcompBatchedCRC32Async calls. If, in addition,
 * the number of chunks is the same or similar across @ref
 * nvcompBatchedCRC32Async calls, reusing the configuration obtained from this
 * function for all @ref nvcompBatchedCRC32Async calls should work well.
 *
 * The result depends on the GPU model, the number of chunks, and the maximum
 * input chunk size. The latter can be passed directly in @p
 * max_input_chunk_bytes or can be deduced from @p device_input_chunk_bytes.
 * When directly specifying @p max_input_chunk_bytes, @p
 * device_input_chunk_bytes should be passed as @ref
 * nvcompCRC32IgnoredInputChunkBytes or a null pointer. When deducing @p
 * max_input_chunk_bytes from @p device_input_chunk_bytes, @p
 * max_input_chunk_bytes should be set to @ref
 * nvcompCRC32DeducedMaxInputChunkBytes or 0.
 *
 * This function is always synchronous with respect to the host. When directly
 * passing the maximum input chunk size in @p max_input_chunk_bytes, no
 * synchronization with the device happens and @p stream is ignored. When
 * deducing @p max_input_chunk_bytes from @p device_input_chunk_bytes, the
 * function synchronizes with @p stream. On devices that do not support
 * stream-ordered memory allocation, the function synchronizes with the entire
 * device in this case.
 * 
 * @param[in] device_input_chunk_bytes Array with size @p num_chunks of sizes of
 * the input chunks in bytes, residing in device-accessible memory, or @ref
 * nvcompCRC32IgnoredInputChunkBytes if @p max_input_chunk_bytes is directly
 * specified. In the former case, the data chunks do not have any alignment
 * requirements.
 * @param[in] num_chunks The number of chunks to compute checksums of.
 * @param[out] kernel_conf Pointer to the kernel configuration to be filled.
 * @param[in] max_input_chunk_bytes Maximum input chunk size in bytes, or @ref
 * nvcompCRC32DeducedMaxInputChunkBytes to deduce from @p device_input_chunk_bytes.
 * @param[in] stream The CUDA stream to operate on. Ignored if @p
 * max_input_chunk_bytes is directly specified.
 *
 * @return nvcompSuccess if successful, and an error code otherwise.
 */
NVCOMP_EXPORT
nvcompStatus_t nvcompBatchedCRC32GetHeuristicConf(
  const size_t *device_input_chunk_bytes,
  size_t num_chunks,
  nvcompCRC32KernelConf_t *kernel_conf,
  size_t max_input_chunk_bytes,
  cudaStream_t stream
);

/**
 * @brief Explicitly search for the optimal CRC32 kernel configuration by
 * benchmarking.
 *
 * In most cases, @ref nvcompBatchedCRC32GetHeuristicConf should provide a
 * sufficiently performant kernel configuration using much less time and fewer
 * resources. When performance is of paramount importance, this function can be
 * used to explicitly search for the optimal kernel configuration. Note that
 * this only makes sense when processing a large number of batches and the
 * number and length of chunks are very similar across batches so that the same
 * kernel configuration can be used.
 * 
 * This function is always synchronous with respect to the host and synchronizes
 * with @p stream. On devices that do not support stream-ordered memory
 * allocation, the function synchronizes with the entire device.
 *
 * @param[in] device_input_chunk_ptrs Array with size @p num_chunks of pointers
 * to the input data chunks in device-accessible memory. The data chunks do not
 * have any alignment requirements.
 * @param[in] device_input_chunk_bytes Array with size @p num_chunks of sizes of
 * the input chunks in bytes, residing in device-accessible memory.
 * @param[in] num_chunks The number of chunks to use for benchmarking.
 * @param[out] device_crc32_ptr Array with size @p num_chunks on the GPU to be
 * used for benchmark outputs.
 * @param[in] spec The CRC32 specification to use for benchmarking.
 * @param[out] kernel_conf Pointer to the kernel configuration to be filled with
 * optimal settings.
 * @param[in] stream The CUDA stream to operate on.
 *
 * @return nvcompSuccess if successful, and an error code otherwise.
 */
NVCOMP_EXPORT
nvcompStatus_t nvcompBatchedCRC32SearchConf(
  const void *const *device_input_chunk_ptrs,
  const size_t *device_input_chunk_bytes,
  size_t num_chunks,
  uint32_t *device_crc32_ptr,
  nvcompCRC32Spec_t spec,
  nvcompCRC32KernelConf_t *kernel_conf,
  cudaStream_t stream
);

#ifdef __cplusplus
}
#endif
#endif // NVCOMP_CRC32_H
