/*
 * SPDX-FileCopyrightText: Copyright (c) 2020-2025 NVIDIA CORPORATION & AFFILIATES.
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

#include "deflate.h"
#include "formatSpec.hpp"
#include "nvcompManager.hpp"

namespace nvcomp
{

/**
 * @brief High-level interface class for the Deflate compressor.
 *
 * @note If user_stream is specified, the lifetime of the DeflateManager instance must not
 * extend beyond that of the user_stream.
 */
struct DeflateManager : detail::PimplManager
{
  /**
   * @brief Constructor of DeflateManager.
   *
   * @param[in] uncomp_chunk_size Internal chunk size used to partition the input data.
   * @param[in] compress_opts Compression options to use.
   * @param[in] decompress_opts Decompression options to use.
   * @param[in] user_stream The CUDA stream to operate on.
   * @param[in] checksum_policy The checksum policy to use during compression and decompression.
   * @param[in] bitstream_kind Setting to configure how the manager compresses the input.
   */
  NVCOMP_EXPORT
  DeflateManager(
    size_t uncomp_chunk_size,
    const nvcompBatchedDeflateCompressOpts_t &compress_opts = nvcompBatchedDeflateCompressDefaultOpts,
    const nvcompBatchedDeflateDecompressOpts_t &decompress_opts = nvcompBatchedDeflateDecompressDefaultOpts,
    cudaStream_t user_stream = 0,
    ChecksumPolicy checksum_policy = NoComputeNoVerify,
    BitstreamKind bitstream_kind = BitstreamKind::NVCOMP_NATIVE
  );

  /**
   * @brief Destructor of DeflateManager.
   */
  NVCOMP_EXPORT
  ~DeflateManager() noexcept;
};

} // namespace nvcomp
