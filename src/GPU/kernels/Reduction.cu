#include "Common.cuh"

namespace
{

/// Sum across one warp. Every lane of the warp must reach this, hence the full mask.
__device__ __forceinline__ int64_t warpReduceSum(int64_t v)
{
    for (int offset = warpSize / 2; offset > 0; offset >>= 1)
        v += __shfl_down_sync(0xffffffffu, v, offset);
    return v;
}

/// Sum across the whole block: warp-level shuffles first, then one warp folds the
/// per-warp results. Shared memory holds one slot per warp.
__device__ __forceinline__ int64_t blockReduceSum(int64_t v)
{
    constexpr int warps_per_block = ch_gpu::block_size / 32;
    __shared__ int64_t warp_sums[warps_per_block];

    const int lane = threadIdx.x % warpSize;
    const int warp = threadIdx.x / warpSize;

    v = warpReduceSum(v);
    if (lane == 0)
        warp_sums[warp] = v;
    __syncthreads();

    /// Warp 0 is entirely resident, so the shuffles below keep their full mask valid;
    /// lanes past the warp count contribute an identity element.
    v = (threadIdx.x < warps_per_block) ? warp_sums[threadIdx.x] : int64_t(0);
    if (warp == 0)
        v = warpReduceSum(v);
    return v;
}

/// One partial sum per block. Run twice: once over the input, once over the partials.
__global__ void sumInt64Kernel(const int64_t * data, size_t n, int64_t * partials)
{
    int64_t acc = 0;
    for (size_t i = blockIdx.x * size_t(blockDim.x) + threadIdx.x; i < n; i += size_t(blockDim.x) * gridDim.x)
        acc += data[i];

    acc = blockReduceSum(acc);

    if (threadIdx.x == 0)
        partials[blockIdx.x] = acc;
}

}

extern "C" int ch_gpu_sum_int64(const int64_t * data, size_t n, int64_t * out)
{
    if (out == nullptr)
        return CH_GPU_INVALID_ARGUMENT;

    *out = 0;

    /// An empty range is answered before the pointer check on purpose: an empty
    /// std::span carries a null data() pointer, and summing nothing is still zero.
    if (n == 0)
    {
        ch_gpu::clearError();
        return CH_GPU_OK;
    }

    if (data == nullptr)
        return CH_GPU_INVALID_ARGUMENT;

    const size_t wanted_blocks = (n + ch_gpu::block_size - 1) / ch_gpu::block_size;
    const int blocks = int(wanted_blocks < ch_gpu::max_blocks ? wanted_blocks : ch_gpu::max_blocks);

    int64_t * ddata = nullptr;
    int64_t * dpartials = nullptr;
    cudaError_t err = cudaSuccess;

    if ((err = cudaMalloc(&ddata, n * sizeof(int64_t))) != cudaSuccess)
        return ch_gpu::failAndFree(CH_GPU_ALLOC_FAILED, "cudaMalloc(data)", err, ddata, dpartials, nullptr);
    if ((err = cudaMalloc(&dpartials, size_t(blocks) * sizeof(int64_t))) != cudaSuccess)
        return ch_gpu::failAndFree(CH_GPU_ALLOC_FAILED, "cudaMalloc(partials)", err, ddata, dpartials, nullptr);

    if ((err = cudaMemcpy(ddata, data, n * sizeof(int64_t), cudaMemcpyHostToDevice)) != cudaSuccess)
        return ch_gpu::failAndFree(CH_GPU_COPY_FAILED, "cudaMemcpy(data H2D)", err, ddata, dpartials, nullptr);

    /// Stage one: `blocks` partial sums.
    sumInt64Kernel<<<blocks, ch_gpu::block_size>>>(ddata, n, dpartials);
    if ((err = cudaGetLastError()) != cudaSuccess)
        return ch_gpu::failAndFree(CH_GPU_LAUNCH_FAILED, "sumInt64Kernel stage 1", err, ddata, dpartials, nullptr);

    /// Stage two: fold the partials in a single block, writing the result in place.
    /// `blocks <= max_blocks` guarantees the grid-stride loop covers them all.
    sumInt64Kernel<<<1, ch_gpu::block_size>>>(dpartials, size_t(blocks), dpartials);
    if ((err = cudaGetLastError()) != cudaSuccess)
        return ch_gpu::failAndFree(CH_GPU_LAUNCH_FAILED, "sumInt64Kernel stage 2", err, ddata, dpartials, nullptr);

    if ((err = cudaMemcpy(out, dpartials, sizeof(int64_t), cudaMemcpyDeviceToHost)) != cudaSuccess)
        return ch_gpu::failAndFree(CH_GPU_COPY_FAILED, "cudaMemcpy(out D2H)", err, ddata, dpartials, nullptr);

    cudaFree(ddata);
    cudaFree(dpartials);

    ch_gpu::clearError();
    return CH_GPU_OK;
}
