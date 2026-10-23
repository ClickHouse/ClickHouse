#include <GPU/GPUGather.cuh>

#include <string>

namespace DB::GPU
{

namespace
{

constexpr int threads_per_block = 256;

template <typename T>
__global__ void gatherRowsOf(const T * values, const int32_t * indices, size_t rows, T * out)
{
    const size_t row = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    if (row < rows)
        out[row] = values[indices[row]];
}

template <typename T>
void launchGather(const char * values, const int32_t * indices, size_t rows, char * out, cuda::stream_ref stream)
{
    const auto blocks = static_cast<unsigned>((rows + threads_per_block - 1) / threads_per_block);
    gatherRowsOf<T><<<blocks, threads_per_block, 0, stream.get()>>>(
        reinterpret_cast<const T *>(values), indices, rows, reinterpret_cast<T *>(out));
    checkCuda(cudaGetLastError(), "Cannot launch the gather of the rows of a column");
}

}

__host__ void gatherFixedRows(
    const char * values, size_t element_size, const int32_t * indices, size_t rows, char * out, cuda::stream_ref stream)
{
    if (rows == 0)
        return;

    switch (element_size)
    {
        case 1: launchGather<uint8_t>(values, indices, rows, out, stream); return;
        case 2: launchGather<uint16_t>(values, indices, rows, out, stream); return;
        case 4: launchGather<uint32_t>(values, indices, rows, out, stream); return;
        case 8: launchGather<uint64_t>(values, indices, rows, out, stream); return;
        default:
            throwGPUError(("a gather of values of " + std::to_string(element_size) + " bytes").c_str());
    }
}

}
