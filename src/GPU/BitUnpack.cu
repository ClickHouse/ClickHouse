#include <GPU/BitUnpack.cuh>

#include <GPU/GPUTypes.cuh>

#include <cuda_runtime_api.h>

namespace DB::GPU
{

namespace
{

constexpr unsigned threads_per_block = 256;

/// A row of blocks per chunk, a thread per integer.
__global__ void unpackKernel(const PackedChunk * chunks)
{
    const PackedChunk chunk = chunks[blockIdx.y];
    const uint32_t index = blockIdx.x * blockDim.x + threadIdx.x;
    if (index >= chunk.count)
        return;

    const uint64_t first_bit = static_cast<uint64_t>(index) * chunk.bits;
    const unsigned char * at = chunk.source + first_bit / 8;
    uint64_t word = 0;
    for (unsigned i = 0; i < 8; ++i)
        word |= static_cast<uint64_t>(at[i]) << (8 * i);

    const uint64_t mask = (uint64_t{1} << chunk.bits) - 1;
    const uint64_t value = chunk.reference + ((word >> (first_bit % 8)) & mask);

    /// Byte by byte, as a chunk of a block may start anywhere a value of the block does.
    unsigned char * out = chunk.destination + static_cast<size_t>(index) * chunk.element_size;
    for (uint32_t i = 0; i < chunk.element_size; ++i)
        out[i] = static_cast<unsigned char>(value >> (8 * i));
}

}

__host__ void unpackChunks(const PackedChunk * chunks, size_t num_chunks, size_t max_count, cuda::stream_ref stream)
{
    if (num_chunks == 0 || max_count == 0)
        return;

    const dim3 grid(static_cast<unsigned>((max_count + threads_per_block - 1) / threads_per_block), static_cast<unsigned>(num_chunks));
    unpackKernel<<<grid, threads_per_block, 0, stream.get()>>>(chunks);
    checkCuda(cudaGetLastError(), "Cannot unpack the integers of GDeflate chunks on the device");
}

}
