#include <GPU/BitUnpack.cuh>

#include <GPU/GPUTypes.cuh>

#include <Compression/BitPackingFormat.h>

#include <cuda_runtime_api.h>

namespace DB::GPU
{

namespace
{

using namespace BitPackingFormat;

constexpr unsigned threads_per_block = 256;
/// A thread unpacks 8 bytes of integers of a chunk, whatever their width, so a chunk takes the same blocks for any.
constexpr size_t bytes_per_thread = 8;
constexpr unsigned blocks_per_chunk = chunk_bytes / bytes_per_thread / threads_per_block;

__device__ uint32_t loadUInt32(const unsigned char * at)
{
    uint32_t value = 0;
    for (unsigned i = 0; i < 4; ++i)
        value |= static_cast<uint32_t>(at[i]) << (8 * i);
    return value;
}

__device__ uint64_t loadUInt64(const unsigned char * at)
{
    uint64_t value = 0;
    for (unsigned i = 0; i < 8; ++i)
        value |= static_cast<uint64_t>(at[i]) << (8 * i);
    return value;
}

__device__ bool isElementSize(uint32_t size)
{
    return size == 1 || size == 2 || size == 4 || size == 8;
}

/// A row of blocks per chunk per stream, a thread per `bytes_per_thread` of integers.
__global__ void unpackKernel(const BitPackingStream * streams, uint32_t * statuses)
{
    const BitPackingStream s = streams[blockIdx.z];
    const uint32_t element_size = s.stream[1];
    const size_t tail_bytes = s.stream[2];
    const size_t num_chunks = loadUInt32(s.stream + 4);
    if (!isElementSize(element_size) || s.stream[0] != version || tail_bytes >= element_size || tail_bytes > s.unpacked_bytes)
    {
        if (blockIdx.x == 0 && blockIdx.y == 0 && threadIdx.x == 0)
            statuses[blockIdx.z] = 1;
        return;
    }
    const size_t packed_bytes = s.unpacked_bytes - tail_bytes;

    /// The first thread of a stream checks its header against what is expected of it, and copies the tail.
    if (blockIdx.x == 0 && blockIdx.y == 0 && threadIdx.x == 0)
    {
        uint32_t status = 0;
        if (packed_bytes % element_size != 0 || num_chunks != numChunks(packed_bytes))
            status = 1;
        else
        {
            size_t at = dataOffset(num_chunks);
            for (size_t i = 0; i < num_chunks; ++i)
            {
                const uint32_t bits = s.stream[descriptorOffset(i)];
                const size_t count = min(chunk_bytes, packed_bytes - i * chunk_bytes) / element_size;
                if (!(bits <= max_packed_bits || (bits == raw_bits && element_size == 8)) || bits > 8 * element_size)
                    status = 2;
                at += paddedBytes(packedBytes(count, bits));
            }
            if (at + tail_bytes + padding_bytes != s.stream_bytes)
                status = 3;
            else
                for (size_t i = 0; i < tail_bytes; ++i)
                    s.destination[packed_bytes + i] = s.stream[at + i];
        }
        statuses[blockIdx.z] = status;
    }

    if (blockIdx.y >= num_chunks)
        return;

    const size_t chunk = blockIdx.y;
    const size_t count = min(chunk_bytes, packed_bytes - chunk * chunk_bytes) / element_size;
    const size_t per_thread = bytes_per_thread / element_size;
    const size_t first = (static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x) * per_thread;
    if (first >= count)
        return;
    const size_t last = min(first + per_thread, count);

    /// Where the chunk's bytes start: after those of the chunks before it.
    size_t at = dataOffset(num_chunks);
    for (size_t i = 0; i < chunk; ++i)
    {
        const size_t count_before = min(chunk_bytes, packed_bytes - i * chunk_bytes) / element_size;
        at += paddedBytes(packedBytes(count_before, s.stream[descriptorOffset(i)]));
    }

    const unsigned char * const descriptor = s.stream + descriptorOffset(chunk);
    const uint32_t bits = descriptor[0];
    const uint64_t reference = loadUInt64(descriptor + 8);
    const unsigned char * const data = s.stream + at;
    const uint64_t mask = bits == raw_bits ? ~uint64_t{0} : (uint64_t{1} << bits) - 1;

    for (size_t index = first; index < last; ++index)
    {
        uint64_t value;
        if (bits == raw_bits)
        {
            value = loadUInt64(data + index * 8);
        }
        else if (bits == 0)
        {
            value = reference;
        }
        else
        {
            const size_t first_bit = index * bits;
            value = reference + ((loadUInt64(data + first_bit / 8) >> (first_bit % 8)) & mask);
        }

        /// Byte by byte, as a chunk of a block may start anywhere a value of the block does.
        unsigned char * out = s.destination + (chunk * chunk_bytes / element_size + index) * element_size;
        for (uint32_t i = 0; i < element_size; ++i)
            out[i] = static_cast<unsigned char>(value >> (8 * i));
    }
}

}

__host__ void unpackStreams(const BitPackingStream * streams, size_t num_streams, size_t max_chunks, uint32_t * statuses, cuda::stream_ref stream)
{
    if (num_streams == 0 || max_chunks == 0)
        return;

    const dim3 grid(blocks_per_chunk, static_cast<unsigned>(max_chunks), static_cast<unsigned>(num_streams));
    unpackKernel<<<grid, threads_per_block, 0, stream.get()>>>(streams, statuses);
    checkCuda(cudaGetLastError(), "Cannot unpack the integers of BitPacking blocks on the device");
}

}
