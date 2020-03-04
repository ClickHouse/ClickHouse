/******************************************************************************
 * Copyright (c) 2011, Duane Merrill.  All rights reserved.
 * Copyright (c) 2011-2017, NVIDIA CORPORATION.  All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the NVIDIA CORPORATION nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL NVIDIA CORPORATION BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 ******************************************************************************/

/**
 * \file
 * CUB umbrella include file
 */

#pragma once


// Block
#include "block/block_histogram.cuh"
#include "block/block_discontinuity.cuh"
#include "block/block_exchange.cuh"
#include "block/block_load.cuh"
#include "block/block_radix_rank.cuh"
#include "block/block_radix_sort.cuh"
#include "block/block_reduce.cuh"
#include "block/block_scan.cuh"
#include "block/block_store.cuh"
//#include "block/block_shift.cuh"

// Device
#include "device/device_histogram.cuh"
#include "device/device_partition.cuh"
#include "device/device_radix_sort.cuh"
#include "device/device_reduce.cuh"
#include "device/device_run_length_encode.cuh"
#include "device/device_scan.cuh"
#include "device/device_segmented_radix_sort.cuh"
#include "device/device_segmented_reduce.cuh"
#include "device/device_select.cuh"
#include "device/device_spmv.cuh"

// Grid
//#include "grid/grid_barrier.cuh"
#include "grid/grid_even_share.cuh"
#include "grid/grid_mapping.cuh"
#include "grid/grid_queue.cuh"

// Thread
#include "thread/thread_load.cuh"
#include "thread/thread_operators.cuh"
#include "thread/thread_reduce.cuh"
#include "thread/thread_scan.cuh"
#include "thread/thread_store.cuh"

// Warp
#include "warp/warp_reduce.cuh"
#include "warp/warp_scan.cuh"

// Iterator
#include "iterator/arg_index_input_iterator.cuh"
#include "iterator/cache_modified_input_iterator.cuh"
#include "iterator/cache_modified_output_iterator.cuh"
#include "iterator/constant_input_iterator.cuh"
#include "iterator/counting_input_iterator.cuh"
#include "iterator/tex_obj_input_iterator.cuh"
#include "iterator/tex_ref_input_iterator.cuh"
#include "iterator/transform_input_iterator.cuh"

// Util
#include "util_arch.cuh"
#include "util_debug.cuh"
#include "util_device.cuh"
#include "util_macro.cuh"
#include "util_ptx.cuh"
#include "util_type.cuh"

