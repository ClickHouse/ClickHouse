/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES.
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

#include <cuda_runtime_api.h>

#include <istream>
#include <ostream>

#include <nvcomp/gzip.h>
#include <nvcomp/shared_types.h>
#include <nvcomp_export.h>

/**
 * @brief Compute the number of bytes to allocate for the decompression workspace.
 *
 *  @param[out] temp_bytes Number of bytes needed for scratch space.
 *
 * @return nvcompSuccess if successful, and an error code otherwise.
 */
NVCOMP_EXPORT
nvcompStatus_t nvcompGzipStreamingDecompressGetTempSize(size_t *temp_bytes);

/**
 * @brief Perform synchronous decompression streaming from stream to stream.
 *
 *  @param[in] input_stream Input stream with the compressed buffer.
 *  @param[in] output_stream Output stream to write the uncompressed buffer.
 *  @param[in] temp_bytes Size of internal buffer for scratch space.
 *  @param[in] device_temp_ptr Pointer to device memory allocated to store scratch data.
 *  @param[in] stream The CUDA stream to operate on.
 *
 * @return nvcompSuccess if successfully decompressed, and an error code otherwise.
 */
NVCOMP_EXPORT
nvcompStatus_t nvcompGzipStreamingDecompress(
  std::istream &input_stream,
  std::ostream &output_stream,
  const size_t temp_bytes,
  void *const device_temp_ptr,
  cudaStream_t stream
);

/**
 * @brief Compute the number of device bytes to allocate for the compression workspace.
 *
 *  @param[in] opts Compression options (see nvcompBatchedGzipCompressOpts_t) applied to every window.
 *  @param[out] temp_bytes Number of device bytes needed for scratch space.
 *
 * @return nvcompSuccess if successful, and an error code otherwise.
 */
NVCOMP_EXPORT
nvcompStatus_t nvcompGzipStreamingCompressGetTempSize(nvcompBatchedGzipCompressOpts_t opts, size_t *temp_bytes);

/**
 * @brief Perform synchronous compression streaming from stream to stream.
 *
 *  @param[in] input_stream Input stream with the uncompressed buffer.
 *  @param[in] output_stream Output stream to write the compressed (gzip) buffer.
 *  @param[in] temp_bytes Size of the device scratch space (>= nvcompGzipStreamingCompressGetTempSize).
 *  @param[in] device_temp_ptr Pointer to device memory allocated to store scratch data.
 *  @param[in] opts Compression options (see nvcompBatchedGzipCompressOpts_t) applied to every window.
 *  @param[in] stream The CUDA stream to operate on.
 *
 * @return nvcompSuccess if successfully compressed, and an error code otherwise.
 */
NVCOMP_EXPORT
nvcompStatus_t nvcompGzipStreamingCompress(
  std::istream &input_stream,
  std::ostream &output_stream,
  const size_t temp_bytes,
  void *const device_temp_ptr,
  nvcompBatchedGzipCompressOpts_t opts,
  cudaStream_t stream
);
