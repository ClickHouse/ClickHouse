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
 * @brief Construct an nvcompManagerBase instance from a given compressed buffer.
 *
 * @param[in] comp_buffer The HLIF compressed buffer from which we intend to create the manager.
 *                        The buffer needs to be host accessible.
 * 
 * @param[in] num_threads The number of threads to use.

 * @return The constructed manager instance.
 */
NVCOMP_EXPORT
std::shared_ptr<nvcompManagerBase> create_cpu_manager(const uint8_t *comp_buffer, unsigned int num_threads = 0);

/**
 * @brief Returns the compression format for a given HLIF compressed buffer. 
 *
 * @param[in] comp_buffer The HLIF compressed buffer from which we intend to create the manager.
 *                        The buffer needs to be host accessible.
 *
 * @return Compression format.
 */
NVCOMP_EXPORT
nvcompFormatType_t get_cpu_compression_format(const uint8_t *comp_buffer);

} // namespace nvcomp
