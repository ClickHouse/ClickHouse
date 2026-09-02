#include "Common.cuh"

namespace
{

__global__ void addInt64Kernel(const int64_t * a, const int64_t * b, int64_t * out, size_t n)
{
    /// Grid-stride rather than one-element-per-thread: the launch geometry then stops
    /// depending on `n`, and a column longer than the grid still works.
    for (size_t i = blockIdx.x * size_t(blockDim.x) + threadIdx.x; i < n; i += size_t(blockDim.x) * gridDim.x)
        out[i] = a[i] + b[i];
}

}

extern "C" int ch_gpu_add_int64(const int64_t * a, const int64_t * b, int64_t * out, size_t n)
{
    /// Checked before the pointers: an empty column yields an empty std::span, whose
    /// data() is null, and adding nothing is a no-op rather than a misuse.
    if (n == 0)
    {
        ch_gpu::clearError();
        return CH_GPU_OK;
    }

    if (a == nullptr || b == nullptr || out == nullptr)
        return CH_GPU_INVALID_ARGUMENT;

    const size_t bytes = n * sizeof(int64_t);

    int64_t * da = nullptr;
    int64_t * db = nullptr;
    int64_t * dout = nullptr;
    cudaError_t err = cudaSuccess;

    if ((err = cudaMalloc(&da, bytes)) != cudaSuccess)
        return ch_gpu::failAndFree(CH_GPU_ALLOC_FAILED, "cudaMalloc(a)", err, da, db, dout);
    if ((err = cudaMalloc(&db, bytes)) != cudaSuccess)
        return ch_gpu::failAndFree(CH_GPU_ALLOC_FAILED, "cudaMalloc(b)", err, da, db, dout);
    if ((err = cudaMalloc(&dout, bytes)) != cudaSuccess)
        return ch_gpu::failAndFree(CH_GPU_ALLOC_FAILED, "cudaMalloc(out)", err, da, db, dout);

    if ((err = cudaMemcpy(da, a, bytes, cudaMemcpyHostToDevice)) != cudaSuccess)
        return ch_gpu::failAndFree(CH_GPU_COPY_FAILED, "cudaMemcpy(a H2D)", err, da, db, dout);
    if ((err = cudaMemcpy(db, b, bytes, cudaMemcpyHostToDevice)) != cudaSuccess)
        return ch_gpu::failAndFree(CH_GPU_COPY_FAILED, "cudaMemcpy(b H2D)", err, da, db, dout);

    const size_t wanted_blocks = (n + ch_gpu::block_size - 1) / ch_gpu::block_size;
    const int blocks = int(wanted_blocks < ch_gpu::max_blocks ? wanted_blocks : ch_gpu::max_blocks);

    addInt64Kernel<<<blocks, ch_gpu::block_size>>>(da, db, dout, n);

    /// The launch itself is asynchronous, so a bad configuration surfaces here while an
    /// in-kernel fault surfaces on the next synchronizing call (the cudaMemcpy below).
    if ((err = cudaGetLastError()) != cudaSuccess)
        return ch_gpu::failAndFree(CH_GPU_LAUNCH_FAILED, "addInt64Kernel launch", err, da, db, dout);

    if ((err = cudaMemcpy(out, dout, bytes, cudaMemcpyDeviceToHost)) != cudaSuccess)
        return ch_gpu::failAndFree(CH_GPU_COPY_FAILED, "cudaMemcpy(out D2H)", err, da, db, dout);

    cudaFree(da);
    cudaFree(db);
    cudaFree(dout);

    ch_gpu::clearError();
    return CH_GPU_OK;
}
