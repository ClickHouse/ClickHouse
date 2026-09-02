#pragma once

/// Shared plumbing for the .cu translation units. This header is nvcc-only and must
/// never be included from the clang side of the build -- see src/GPU/GPUKernels.h for
/// the interface those two sides actually share.

#include "../GPUKernels.h"

#include <cuda_runtime.h>

#include <cstdio>

namespace ch_gpu
{

/// 256 threads is a reasonable default occupancy target across Turing and later, and
/// keeps the block-reduction shared-memory footprint at 8 slots.
constexpr int block_size = 256;

/// Cap on the number of blocks in the first reduction stage, so stage two always fits
/// in a single block.
constexpr int max_blocks = 1024;

/// Last error detail, per thread. Thread-local because ClickHouse calls into these
/// entry points from many query threads at once.
inline thread_local char last_error[256] = "";

inline void clearError()
{
    last_error[0] = '\0';
}

inline int fail(int status, const char * what, cudaError_t err)
{
    std::snprintf(last_error, sizeof(last_error), "%s: %s", what, cudaGetErrorString(err));
    return status;
}

/// Frees whatever was allocated so far and reports the failure. Passing already-null
/// pointers is fine -- cudaFree(nullptr) is a no-op.
inline int failAndFree(int status, const char * what, cudaError_t err, void * a, void * b, void * c)
{
    cudaFree(a);
    cudaFree(b);
    cudaFree(c);
    return fail(status, what, err);
}

}
