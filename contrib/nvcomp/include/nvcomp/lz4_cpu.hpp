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

#include "nvcompManager.hpp"

namespace nvcomp
{

/**
 * @brief CPU-side high-level interface for LZ4 compression/decompression (HLIF format).
 */
struct LZ4CPUManager : detail::PimplManager
{

  /**
   * @brief Constructor for LZ4CPUManager.
   *
   * @param uncomp_chunk_size The uncompressed chunk size. Default is 65536.
   * @param compression_level The compression level. Default is 9.
   * @param num_threads The number of threads to use. Default is 0 
                        (use maximum available hardware concurrency threads).
   *
   * @note CPU compression does not support calculating or verifying checksums and
   * will skip the checksum data during decompression if it is provided
   * in the HLIF compressed buffer. In addition, only NVCOMP_NATIVE bitstream
   * kind is supported for CPU compression and decompression.
   */
  NVCOMP_EXPORT
  LZ4CPUManager(size_t uncomp_chunk_size = 65536, int compression_level = 9, unsigned int num_threads = 0);

  /**
   * @brief Destructor for LZ4CPUManager.
   */
  NVCOMP_EXPORT
  ~LZ4CPUManager() noexcept;
};

} // namespace nvcomp
