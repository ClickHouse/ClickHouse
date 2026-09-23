#pragma once

#include <cuda_runtime_api.h>

namespace DB::GPU
{

/** The streams the process's GPU work runs on, one of each, so that every part of it names a
  * stream by what the stream carries. Both sides of the build include this: the host, and the
  * island that nvcc compiles, which is why nothing but the CUDA runtime is included here.
  *
  * - `compute`: the kernels that reduce and group, the copies that append expanded values to
  *   their columns, and the reads of results. It is the legacy default stream, which is also
  *   cuDF's default and where the island's allocations go.
  * - `upload`: copies from pinned memory to the device, so that they run beside the kernels.
  * - `decompression`: nvcomp's kernels, so that expanding one buffer runs beside grouping another.
  *
  * The last two do not synchronize with the default stream; nothing on one stream waits for
  * another except through a `DeviceEvent`.
  */
struct StreamRegistry
{
    cudaStream_t compute = nullptr;
    cudaStream_t upload = nullptr;
    cudaStream_t decompression = nullptr;

    /// The process's registry, made on first use, after the device is initialized.
    static const StreamRegistry & get();
};

}
