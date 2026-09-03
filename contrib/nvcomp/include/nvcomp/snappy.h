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

#ifndef NVCOMP_SNAPPY_H
#define NVCOMP_SNAPPY_H

#include "nvcomp.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Snappy compression options for the low-level API
 */
typedef struct
{
  /**
   * @brief These bytes are unused and must be zeroed. This ensures
   *        compatibility if additional fields are added in the future.
   */
  char reserved[64];
} nvcompBatchedSnappyCompressOpts_t;

/**
 * @brief Snappy decompression options for the low-level API
 */
typedef struct
{
  /**
   * @brief Decompression backend to use.
   */
  nvcompDecompressBackend_t backend;
  /**
   * @brief Whether to sort chunks before hardware decompression for better load balancing.
   *        Only used when the backend is the hardware decompression engine.
   */
  int sort_before_hw_decompress;
  /**
   * @brief These bytes are unused and must be zeroed. This ensures
   *        compatibility if additional fields are added in the future.
   */
  char reserved[56];
} nvcompBatchedSnappyDecompressOpts_t;

/**
 * @brief Default Snappy compression options
 */
static const nvcompBatchedSnappyCompressOpts_t nvcompBatchedSnappyCompressDefaultOpts = {{0}};

/**
 * @brief Default Snappy decompression options
 */
static const nvcompBatchedSnappyDecompressOpts_t nvcompBatchedSnappyDecompressDefaultOpts =
  {NVCOMP_DECOMPRESS_BACKEND_DEFAULT, 0 /* sort_before_hw_decompress */, {0}};

/**
 * @brief The maximum supported uncompressed chunk size in bytes for the Snappy compressor.
 */
static const size_t nvcompSnappyCompressionMaxAllowedChunkSize = 1 << 24;

/**
 * @brief The maximum supported compressed and decompressed chunk size in bytes for the Snappy decompressor.
 * @note To maximize decompression performance, users are encouraged to compress in smaller chunks, for example 64KiB.
 * @note The hardware Decompression Engine (DE) may have different maximum chunk size limits.
 * This can be queried through `cuDeviceGetAttribute` with `CU_DEVICE_ATTRIBUTE_MEM_DECOMPRESS_MAXIMUM_LENGTH`.
 */
static const size_t nvcompSnappyDecompressionMaxAllowedChunkSize = (1ull << 31) - 1;

/**
 * @brief The most restrictive of the minimum alignment requirements for void-type CUDA memory buffers
 * used for input, output, or temporary memory, passed to compression functions.
 *
 * @note In all cases, typed memory buffers must still be aligned to their type's size,
 * e.g., 4 bytes for `int`.
 */
static const size_t nvcompSnappyRequiredCompressionAlignment = 1;

/**
 * @brief Get the minimum buffer alignment requirements for compression.
 *
 * @note Providing buffers with alignments above the minimum requirements
 * (e.g., 16- or 32-byte alignment) may help improve performance.
 *
 * @param[in] compress_opts Compression options.
 * @param[out] alignment_requirements The minimum buffer alignment requirements
 * for compression.
 *
 * @return nvcompSuccess if successful, and an error code otherwise.
 */
NVCOMP_EXPORT
nvcompStatus_t nvcompBatchedSnappyCompressGetRequiredAlignments(
  nvcompBatchedSnappyCompressOpts_t compress_opts,
  nvcompAlignmentRequirements_t *alignment_requirements
);

/**
 * @brief Get the amount of temporary memory required on the GPU for compression
 * asynchronously.
 *
 * @note This function does not interact with the device, its result can be used immediately.
 *
 * @param[in] num_chunks The number of chunks of memory in the batch.
 * @param[in] max_uncompressed_chunk_bytes The maximum size of a chunk in the
 * batch.
 * @param[in] compress_opts Compression options.
 * @param[out] temp_bytes The amount of GPU memory that will be temporarily
 * required during compression. The value is returned on the host side.
 * @param[in] max_total_uncompressed_bytes Upper bound on the total uncompressed
 * size of all chunks
 *
 * @return nvcompSuccess if successful, and an error code otherwise.
 */
NVCOMP_EXPORT
nvcompStatus_t nvcompBatchedSnappyCompressGetTempSizeAsync(
  size_t num_chunks,
  size_t max_uncompressed_chunk_bytes,
  nvcompBatchedSnappyCompressOpts_t compress_opts,
  size_t *temp_bytes,
  size_t max_total_uncompressed_bytes
);

/**
 * @brief Get the amount of temporary memory required on the GPU for compression
 * synchronously.
 *
 * @note This function may perform operations on the stream; if so, it will synchronize it internally.
 * Therefore, it does not require additional synchronization after it returns,
 * and the result can be used immediately.
 *
 * @param[in] device_uncompressed_chunk_ptrs Array with size \p num_chunks of pointers
 * to the uncompressed data chunks. Both the pointers and the uncompressed data
 * should reside in device-accessible memory.
 * Each chunk must be aligned to the value in the `input` member of the
 * \ref nvcompAlignmentRequirements_t object output by
 * `nvcompBatchedSnappyCompressGetRequiredAlignments` when called with the same
 * \p compress_opts.
 * @param[in] device_uncompressed_chunk_bytes Array with size \p num_chunks of
 * sizes of the uncompressed chunks in bytes.
 * The sizes should reside in device-accessible memory.
 * @param[in] num_chunks The number of chunks of memory in the batch.
 * @param[in] max_uncompressed_chunk_bytes The maximum size of a chunk in the
 * batch.
 * @param[in] compress_opts Compression options.
 * @param[out] temp_bytes The amount of GPU memory that will be temporarily
 * required during compression. The value is returned on the host side.
 * @param[in] max_total_uncompressed_bytes Upper bound on the total uncompressed
 * size of all chunks
 * @param[in] stream The CUDA stream to operate on.
 *
 * @return nvcompSuccess if successful, and an error code otherwise.
 */
NVCOMP_EXPORT
nvcompStatus_t nvcompBatchedSnappyCompressGetTempSizeSync(
  const void *const *const device_uncompressed_chunk_ptrs,
  const size_t *const device_uncompressed_chunk_bytes,
  size_t num_chunks,
  size_t max_uncompressed_chunk_bytes,
  nvcompBatchedSnappyCompressOpts_t compress_opts,
  size_t *temp_bytes,
  size_t max_total_uncompressed_bytes,
  cudaStream_t stream
);

/**
 * @brief Get the maximum size that a chunk of size at most max_uncompressed_chunk_bytes
 * could compress to. That is, the minimum amount of output memory required to be given
 * \ref nvcompBatchedSnappyCompressAsync for each chunk.
 *
 * @param[in] max_uncompressed_chunk_bytes The maximum size of a chunk before compression.
 * @param[in] compress_opts Snappy compression options.
 * @param[out] max_compressed_chunk_bytes The maximum possible compressed size of the chunk.
 *
 * @return nvcompSuccess if successful, and an error code otherwise.
 */
NVCOMP_EXPORT
nvcompStatus_t nvcompBatchedSnappyCompressGetMaxOutputChunkSize(
  size_t max_uncompressed_chunk_bytes,
  nvcompBatchedSnappyCompressOpts_t compress_opts,
  size_t *max_compressed_chunk_bytes
);

/**
 * @brief Perform batched asynchronous compression.
 *
 * @warning Violating any of the conditions listed in the parameter descriptions
 * below may result in undefined behaviour.
 *
 * @note This function performs operations on the stream, and does not synchronize it,
 * therefore, it requires synchronization or stream-ordered operations to use its results.
 *
 * @param[in] device_uncompressed_chunk_ptrs Array with size \p num_chunks of pointers
 * to the uncompressed data chunks. Both the pointers and the uncompressed data
 * should reside in device-accessible memory.
 * Each chunk must be aligned to the value in the `input` member of the
 * \ref nvcompAlignmentRequirements_t object output by
 * `nvcompBatchedSnappyCompressGetRequiredAlignments` when called with the same
 * \p compress_opts.
 * @param[in] device_uncompressed_chunk_bytes Array with size \p num_chunks of
 * sizes of the uncompressed chunks in bytes.
 * The sizes should reside in device-accessible memory.
 * @param[in] max_uncompressed_chunk_bytes The size of the largest uncompressed chunk.
 * This parameter is currently unused. Set it to either the actual value
 * or zero.
 * @param[in] num_chunks Number of chunks of data to compress.
 * @param[in] device_temp_ptr The temporary GPU workspace, could be NULL in case
 * temporary memory is not needed.
 * Must be aligned to the value in the `temp` member of the
 * \ref nvcompAlignmentRequirements_t object output by
 * `nvcompBatchedSnappyCompressGetRequiredAlignments` when called with the same
 * \p compress_opts.
 * @param[in] temp_bytes The size of the temporary GPU memory pointed to by
 * `device_temp_ptr`.
 * @param[out] device_compressed_chunk_ptrs Array with size \p num_chunks of pointers
 * to the output compressed buffers. Both the pointers and the compressed
 * buffers should reside in device-accessible memory. Each compressed buffer
 * should be preallocated with the size given by
 * `nvcompBatchedSnappyCompressGetMaxOutputChunkSize`.
 * Each compressed buffer must be aligned to the value in the `output` member of the
 * \ref nvcompAlignmentRequirements_t object output by
 * `nvcompBatchedSnappyCompressGetRequiredAlignments` when called with the same
 * \p compress_opts.
 * @param[out] device_compressed_chunk_bytes Array with size \p num_chunks,
 * to be filled with the compressed sizes of each chunk.
 * The buffer should be preallocated in device-accessible memory.
 * @param[in] compress_opts Snappy compression options.
 * @param[out] device_statuses Array with size \p num_chunks of statuses in
 * device-accessible memory. This argument needs to be preallocated. For each
 * chunk, if the compression is successful, the status will be set to
 * `nvcompSuccess`, and an error code otherwise.
 * Can be NULL if desired, in which case error status is not reported.
 * @param[in] stream The CUDA stream to operate on.
 *
 * @return nvcompSuccess if successfully launched, and an error code otherwise.
 */
NVCOMP_EXPORT
nvcompStatus_t nvcompBatchedSnappyCompressAsync(
  const void *const *device_uncompressed_chunk_ptrs,
  const size_t *device_uncompressed_chunk_bytes,
  size_t max_uncompressed_chunk_bytes,
  size_t num_chunks,
  void *device_temp_ptr,
  size_t temp_bytes,
  void *const *device_compressed_chunk_ptrs,
  size_t *device_compressed_chunk_bytes,
  nvcompBatchedSnappyCompressOpts_t compress_opts,
  nvcompStatus_t *device_statuses,
  cudaStream_t stream
);

/**
 * @brief The most restrictive of the minimum alignment requirements for void-type CUDA memory buffers
 * used for input, output, or temporary memory, passed to decompression functions.
 *
 * @note In all cases, typed memory buffers must still be aligned to their type's size,
 * e.g., 4 bytes for `int`.
 */
static const size_t nvcompSnappyRequiredDecompressionAlignment = 1;

/**
 * @brief Get the minimum buffer alignment requirements for decompression.
 *
 * @note Providing buffers with alignments above the minimum requirements
 * (e.g., 16- or 32-byte alignment) may help improve performance.
 *
 * @param[in] decompress_opts Decompression options.
 * @param[out] alignment_requirements The minimum buffer alignment requirements
 * for decompression.
 *
 * @return nvcompSuccess if successful, and an error code otherwise.
 */
NVCOMP_EXPORT
nvcompStatus_t nvcompBatchedSnappyDecompressGetRequiredAlignments(
  nvcompBatchedSnappyDecompressOpts_t decompress_opts,
  nvcompAlignmentRequirements_t *alignment_requirements
);

/**
 * @brief Get the amount of temporary memory required on the GPU for decompression
 * asynchronously.
 *
 * @note This function does not interact with the device, its result can be used immediately.
 *
 * @param[in] num_chunks Number of chunks of data to be decompressed.
 * @param[in] max_uncompressed_chunk_bytes The size of the largest chunk in bytes
 * when uncompressed.
 * @param[in] decompress_opts Decompression options.
 * @param[out] temp_bytes The amount of GPU memory that will be temporarily required
 * during decompression. The value is returned on the host side.
 * @param[in] max_total_uncompressed_bytes The total decompressed size of all the chunks.
 *
 * @return nvcompSuccess if successful, and an error code otherwise.
 */
NVCOMP_EXPORT
nvcompStatus_t nvcompBatchedSnappyDecompressGetTempSizeAsync(
  size_t num_chunks,
  size_t max_uncompressed_chunk_bytes,
  nvcompBatchedSnappyDecompressOpts_t decompress_opts,
  size_t *temp_bytes,
  size_t max_total_uncompressed_bytes
);

/**
 * @brief Get the amount of temporary memory required on the GPU for decompression
 * synchronously.
 *
 * @note This function may perform operations on the stream; if so, it will synchronize it internally.
 * Therefore, it does not require additional synchronization after it returns,
 * and the result can be used immediately.
 *
 * @param[in] device_compressed_chunk_ptrs Array with size \p num_chunks of pointers
 * in device-accessible memory to device-accessible compressed buffers.
 * Each chunk must be aligned to the value in the `input` member of the
 * \ref nvcompAlignmentRequirements_t object output by
 * `nvcompBatchedSnappyDecompressGetRequiredAlignments`.
 * @param[in] device_compressed_chunk_bytes Array with size \p num_chunks of sizes of
 * the compressed buffers in bytes. The sizes should reside in device-accessible memory.
 * @param[in] num_chunks Number of chunks of data to be decompressed.
 * @param[in] max_uncompressed_chunk_bytes The size of the largest chunk in bytes
 * when uncompressed.
 * @param[out] temp_bytes The amount of GPU memory that will be temporarily required
 * during decompression. The value is returned on the host side.
 * @param[in] max_total_uncompressed_bytes  The total decompressed size of all the chunks.
 * @param[in] decompress_opts Decompression options.
 * @param[out] device_statuses Array with size \p num_chunks of statuses in
 * device-accessible memory. This argument needs to be preallocated. For each
 * chunk, if the data can be parsed successfully, the status will be set to
 * `nvcompSuccess`, and an error code otherwise.
 * Can be NULL if desired, in which case error status is not reported.
 * @param[in] stream The CUDA stream to operate on.
 *
 * @return nvcompSuccess if successful, and an error code otherwise.
 */
NVCOMP_EXPORT
nvcompStatus_t nvcompBatchedSnappyDecompressGetTempSizeSync(
  const void *const *const device_compressed_chunk_ptrs,
  const size_t *const device_compressed_chunk_bytes,
  size_t num_chunks,
  size_t max_uncompressed_chunk_bytes,
  size_t *temp_bytes,
  size_t max_total_uncompressed_bytes,
  nvcompBatchedSnappyDecompressOpts_t decompress_opts,
  nvcompStatus_t *device_statuses,
  cudaStream_t stream
);

/**
 * @brief Asynchronously compute the number of bytes of uncompressed data for
 * each compressed chunk.
 *
 * @warning Violating any of the conditions listed in the parameter descriptions
 * below may result in undefined behaviour.
 *
 * @note This function performs operations on the stream, and does not synchronize it,
 * therefore, it requires synchronization or stream-ordered operations to use its results.
 *
 * @param[in] device_compressed_chunk_ptrs Array with size \p num_chunks of
 * pointers in device-accessible memory to compressed buffers.
 * Each chunk must be aligned to the value in the `input` member of the
 * \ref nvcompAlignmentRequirements_t object output by
 * `nvcompBatchedSnappyDecompressGetRequiredAlignments`.
 * @param[in] device_compressed_chunk_bytes Array with size \p num_chunks of sizes
 * of the compressed buffers in bytes. The sizes should reside in device-accessible memory.
 * @param[out] device_uncompressed_chunk_bytes Array with size \p num_chunks
 * to be filled with the sizes, in bytes, of each uncompressed data chunk.
 * This argument needs to be preallocated in device-accessible memory.
 * @param[in] num_chunks Number of data chunks to compute sizes of.
 * @param[in] stream The CUDA stream to operate on.
 *
 * @return nvcompSuccess if successful, and an error code otherwise.
 */
NVCOMP_EXPORT
nvcompStatus_t nvcompBatchedSnappyGetDecompressSizeAsync(
  const void *const *device_compressed_chunk_ptrs,
  const size_t *device_compressed_chunk_bytes,
  size_t *device_uncompressed_chunk_bytes,
  size_t num_chunks,
  cudaStream_t stream
);

/**
 * @brief Perform batched asynchronous decompression.
 *
 * @warning Violating any of the conditions listed in the parameter descriptions
 * below may result in undefined behaviour.
 * 
 * @warning Providing a corrupt buffer for decompression will result in undefined 
 * behavior irrespective of the decompression backend used.
 *
 * @note This function performs operations on the stream, and does not synchronize it,
 * therefore, it requires synchronization or stream-ordered operations to use its results.
 *
 * @param[in] device_compressed_chunk_ptrs Array with size \p num_chunks of pointers
 * in device-accessible memory to device-accessible compressed buffers.
 * Each chunk must be aligned to the value in the `input` member of the
 * \ref nvcompAlignmentRequirements_t object output by
 * `nvcompBatchedSnappyDecompressGetRequiredAlignments`.
 * @param[in] device_compressed_chunk_bytes Array with size \p num_chunks of sizes of
 * the compressed buffers in bytes. The sizes should reside in device-accessible memory.
 * @param[in] device_uncompressed_buffer_bytes Array with size \p num_chunks of sizes,
 * in bytes, of the output buffers to be filled with uncompressed data for each chunk.
 * The sizes should reside in device-accessible memory. If a
 * size is not large enough to hold all decompressed data, the decompressor
 * will set the status in \p device_statuses corresponding to the
 * overflow chunk to `nvcompErrorCannotDecompress`.
 * @param[out] device_uncompressed_chunk_bytes Array with size \p num_chunks to
 * be filled with the actual number of bytes decompressed for every chunk.
 * This argument needs to be preallocated.
 * When `NVCOMP_DECOMPRESS_BACKEND_HARDWARE` is specified in \p decompress_opts.backend,
 * this parameter is required. For `NVCOMP_DECOMPRESS_BACKEND_CUDA`, it is optional
 * and may be set to NULL if reporting the actual sizes is not necessary.
 * @param[in] num_chunks Number of chunks of data to decompress.
 * @param[in] device_temp_ptr The temporary GPU space, could be NULL in case temporary space is not needed.
 * Must be aligned to the value in the `temp` member of the
 * \ref nvcompAlignmentRequirements_t object output by
 * `nvcompBatchedSnappyDecompressGetRequiredAlignments`.
 * @param[in] temp_bytes The size of the temporary GPU space.
 * @param[out] device_uncompressed_chunk_ptrs Array with size \p num_chunks of
 * pointers in device-accessible memory to decompressed data. Each uncompressed
 * buffer needs to be preallocated in device-accessible memory, have the size
 * specified by the corresponding entry in \p device_uncompressed_buffer_bytes,
 * and be aligned to the value in the `output` member of the
 * \ref nvcompAlignmentRequirements_t object output by
 * `nvcompBatchedSnappyDecompressGetRequiredAlignments`.
 * @param[in] decompress_opts Decompression options.
 * @param[out] device_statuses Array with size \p num_chunks of statuses in
 * device-accessible memory. This argument needs to be preallocated. For each
 * chunk, if the decompression is successful, the status will be set to
 * `nvcompSuccess`. Passing corrupt, invalid, or insufficient data leads to
 * undefined behavior or out-of-bound errors. Error reporting cannot be guaranteed
 * in this scenario as only a limited validation is performed to maintain performance.
 * Can be NULL if desired, in which case error status is not reported.
 * @param[in] stream The CUDA stream to operate on.
 *
 * @return nvcompSuccess if successfully launched, and an error code otherwise.
 */
NVCOMP_EXPORT
nvcompStatus_t nvcompBatchedSnappyDecompressAsync(
  const void *const *device_compressed_chunk_ptrs,
  const size_t *device_compressed_chunk_bytes,
  const size_t *device_uncompressed_buffer_bytes,
  size_t *device_uncompressed_chunk_bytes,
  size_t num_chunks,
  void *const device_temp_ptr,
  size_t temp_bytes,
  void *const *device_uncompressed_chunk_ptrs,
  nvcompBatchedSnappyDecompressOpts_t decompress_opts,
  nvcompStatus_t *device_statuses,
  cudaStream_t stream
);

#ifdef __cplusplus
}
#endif

#endif // NVCOMP_SNAPPY_H
