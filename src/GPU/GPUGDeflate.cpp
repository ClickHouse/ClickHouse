#include <GPU/GPUGDeflate.h>

#if USE_GPU

#include <GPU/GPUDevice.h>
#include <GPU/GPUMemory.h>

#include <Common/Exception.h>
#include <base/types.h>

#include <nvcomp/gdeflate.h>

#include <cstring>
#include <string>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int GPU_ERROR;
}

namespace DB::GPU
{

namespace
{

constexpr size_t chunk_alignment = 4;

size_t alignUp(size_t bytes, size_t alignment)
{
    return (bytes + alignment - 1) / alignment * alignment;
}

size_t numChunks(size_t uncompressed_bytes)
{
    return (uncompressed_bytes + gdeflate_chunk_bytes - 1) / gdeflate_chunk_bytes;
}

size_t headerBytes(size_t num_chunks)
{
    return sizeof(UInt32) * (1 + num_chunks);
}

UInt32 readUInt32(const char * at)
{
    UInt32 value;
    memcpy(&value, at, sizeof(value));
    return value;
}

void checkNvcomp(nvcompStatus_t status, const char * what)
{
    if (status != nvcompSuccess)
        throw Exception(ErrorCodes::GPU_ERROR, "{}: nvcomp status {}", what, static_cast<int>(status));
}

nvcompBatchedGdeflateCompressOpts_t compressOpts(int algorithm)
{
    nvcompBatchedGdeflateCompressOpts_t opts = nvcompBatchedGdeflateCompressDefaultOpts;
    opts.algorithm = algorithm;
    return opts;
}

/// Each thread that compresses or expands a block works on a stream of its own.
cuda::stream_ref threadStream()
{
    thread_local StreamPtr stream = createStream();
    return cuda::stream_ref{stream.get()};
}

/// Device copies of the arrays nvcomp takes, one per chunk.
template <typename T>
const T * toDevice(DeviceBuffer & buffer, const std::vector<T> & values)
{
    buffer.clear();
    buffer.append(std::string_view(reinterpret_cast<const char *>(values.data()), values.size() * sizeof(T)));
    return reinterpret_cast<const T *>(buffer.data());
}

}

size_t gdeflateMaxCompressedBytes(size_t uncompressed_bytes, int algorithm)
{
    size_t max_chunk = 0;
    checkNvcomp(
        nvcompBatchedGdeflateCompressGetMaxOutputChunkSize(gdeflate_chunk_bytes, compressOpts(algorithm), &max_chunk),
        "Cannot bound the size of a GDeflate chunk");
    const size_t num_chunks = numChunks(uncompressed_bytes);
    return headerBytes(num_chunks) + num_chunks * alignUp(max_chunk, chunk_alignment);
}

size_t gdeflateCompress(const char * source, size_t source_bytes, char * dest, int algorithm)
{
    const size_t num_chunks = numChunks(source_bytes);
    const UInt32 count = static_cast<UInt32>(num_chunks);
    memcpy(dest, &count, sizeof(count));
    if (num_chunks == 0)
        return headerBytes(0);

    const nvcompBatchedGdeflateCompressOpts_t opts = compressOpts(algorithm);
    size_t max_chunk = 0;
    checkNvcomp(nvcompBatchedGdeflateCompressGetMaxOutputChunkSize(gdeflate_chunk_bytes, opts, &max_chunk), "Cannot bound the size of a GDeflate chunk");
    max_chunk = alignUp(max_chunk, 8);

    const cuda::stream_ref stream = threadStream();
    DeviceBuffer input(stream);
    DeviceBuffer output(stream);
    DeviceBuffer temp(stream);
    DeviceBuffer in_pointers(stream);
    DeviceBuffer in_sizes(stream);
    DeviceBuffer out_pointers(stream);
    DeviceBuffer out_sizes(stream);

    std::vector<size_t> compressed_sizes(num_chunks);
    waitOnFailure(stream, [&]
    {
        input.append(std::string_view(source, source_bytes));
        output.grow(num_chunks * max_chunk);

        std::vector<const void *> in_ptrs(num_chunks);
        std::vector<size_t> in_bytes(num_chunks);
        std::vector<void *> out_ptrs(num_chunks);
        for (size_t i = 0; i < num_chunks; ++i)
        {
            in_ptrs[i] = input.data() + i * gdeflate_chunk_bytes;
            in_bytes[i] = std::min(gdeflate_chunk_bytes, source_bytes - i * gdeflate_chunk_bytes);
            out_ptrs[i] = output.data() + i * max_chunk;
        }

        size_t temp_bytes = 0;
        checkNvcomp(
            nvcompBatchedGdeflateCompressGetTempSize(num_chunks, gdeflate_chunk_bytes, opts, &temp_bytes, source_bytes, stream.get()),
            "Cannot size the scratch space of a GDeflate compression");
        temp.grow(std::max<size_t>(temp_bytes, 1));
        out_sizes.grow(num_chunks * sizeof(size_t));

        checkNvcomp(
            nvcompBatchedGdeflateCompressAsync(
                toDevice(in_pointers, in_ptrs),
                toDevice(in_sizes, in_bytes),
                gdeflate_chunk_bytes,
                num_chunks,
                temp.data(),
                temp_bytes,
                const_cast<void * const *>(toDevice(out_pointers, out_ptrs)),
                reinterpret_cast<size_t *>(out_sizes.data()),
                opts,
                nullptr,
                stream.get()),
            "Cannot compress with GDeflate on the device");

        checkCuda(
            cudaMemcpyAsync(compressed_sizes.data(), out_sizes.data(), num_chunks * sizeof(size_t), cudaMemcpyDeviceToHost, stream.get()),
            "Cannot copy the sizes of GDeflate chunks back");
        synchronizeStream(stream);

        size_t at = headerBytes(num_chunks);
        for (size_t i = 0; i < num_chunks; ++i)
        {
            if (compressed_sizes[i] > max_chunk)
                throw Exception(ErrorCodes::CANNOT_COMPRESS, "GDeflate compressed a chunk to {} bytes, more than {}", compressed_sizes[i], max_chunk);
            const UInt32 size = static_cast<UInt32>(compressed_sizes[i]);
            memcpy(dest + sizeof(UInt32) * (1 + i), &size, sizeof(size));
            checkCuda(
                cudaMemcpyAsync(dest + at, output.data() + i * max_chunk, compressed_sizes[i], cudaMemcpyDeviceToHost, stream.get()),
                "Cannot copy a GDeflate chunk back");
            at = alignUp(at + compressed_sizes[i], chunk_alignment);
        }
        synchronizeStream(stream);
    });

    size_t at = headerBytes(num_chunks);
    for (size_t i = 0; i < num_chunks; ++i)
    {
        const size_t end = at + compressed_sizes[i];
        const size_t next = alignUp(end, chunk_alignment);
        memset(dest + end, 0, next - end);
        at = next;
    }
    return at;
}

void forEachGDeflateChunk(
    std::string_view block,
    size_t uncompressed_bytes,
    const std::function<void(size_t offset, size_t compressed_bytes, size_t decompressed_bytes)> & on_chunk)
{
    if (block.size() < sizeof(UInt32))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "A GDeflate block of {} bytes", block.size());

    const size_t num_chunks = readUInt32(block.data());
    if (num_chunks != numChunks(uncompressed_bytes) || headerBytes(num_chunks) > block.size())
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "A GDeflate block of {} bytes holds {} chunks, where {} expanded bytes make {}",
            block.size(),
            num_chunks,
            uncompressed_bytes,
            numChunks(uncompressed_bytes));

    size_t at = headerBytes(num_chunks);
    for (size_t i = 0; i < num_chunks; ++i)
    {
        const size_t compressed_bytes = readUInt32(block.data() + sizeof(UInt32) * (1 + i));
        if (at + compressed_bytes > block.size())
            throw Exception(
                ErrorCodes::CANNOT_DECOMPRESS, "Chunk {} of a GDeflate block of {} bytes ends at {}", i, block.size(), at + compressed_bytes);
        on_chunk(at, compressed_bytes, std::min(gdeflate_chunk_bytes, uncompressed_bytes - i * gdeflate_chunk_bytes));
        at = alignUp(at + compressed_bytes, chunk_alignment);
    }
}

void gdeflateDecompress(const char * source, size_t source_bytes, char * dest, size_t uncompressed_bytes)
{
    std::vector<size_t> offsets;
    std::vector<size_t> compressed_bytes;
    std::vector<size_t> decompressed_bytes;
    forEachGDeflateChunk({source, source_bytes}, uncompressed_bytes, [&](size_t offset, size_t compressed, size_t decompressed)
    {
        offsets.push_back(offset);
        compressed_bytes.push_back(compressed);
        decompressed_bytes.push_back(decompressed);
    });

    const size_t num_chunks = offsets.size();
    if (num_chunks == 0)
        return;

    const cuda::stream_ref stream = threadStream();
    DeviceBuffer input(stream);
    DeviceBuffer output(stream);
    DeviceBuffer temp(stream);
    DeviceBuffer in_pointers(stream);
    DeviceBuffer in_sizes(stream);
    DeviceBuffer out_pointers(stream);
    DeviceBuffer out_sizes(stream);
    DeviceBuffer results(stream);

    std::vector<nvcompStatus_t> statuses(num_chunks);
    std::vector<size_t> actual_bytes(num_chunks);
    waitOnFailure(stream, [&]
    {
        input.append(std::string_view(source, source_bytes));
        output.grow(uncompressed_bytes);

        std::vector<const void *> in_ptrs(num_chunks);
        std::vector<void *> out_ptrs(num_chunks);
        for (size_t i = 0; i < num_chunks; ++i)
        {
            in_ptrs[i] = input.data() + offsets[i];
            out_ptrs[i] = output.data() + i * gdeflate_chunk_bytes;
        }

        size_t temp_bytes = 0;
        checkNvcomp(
            nvcompBatchedGdeflateDecompressGetTempSize(
                num_chunks, gdeflate_chunk_bytes, nvcompBatchedGdeflateDecompressDefaultOpts, &temp_bytes, uncompressed_bytes, stream.get()),
            "Cannot size the scratch space of a GDeflate expansion");
        temp.grow(std::max<size_t>(temp_bytes, 1));

        char * const result_bytes = results.grow(num_chunks * (sizeof(size_t) + sizeof(nvcompStatus_t)));
        auto * device_actual = reinterpret_cast<size_t *>(result_bytes);
        auto * device_statuses = reinterpret_cast<nvcompStatus_t *>(result_bytes + num_chunks * sizeof(size_t));

        checkNvcomp(
            nvcompBatchedGdeflateDecompressAsync(
                toDevice(in_pointers, in_ptrs),
                toDevice(in_sizes, compressed_bytes),
                toDevice(out_sizes, decompressed_bytes),
                device_actual,
                num_chunks,
                temp.data(),
                temp_bytes,
                const_cast<void * const *>(toDevice(out_pointers, out_ptrs)),
                nvcompBatchedGdeflateDecompressDefaultOpts,
                device_statuses,
                stream.get()),
            "Cannot expand GDeflate on the device");

        checkCuda(cudaMemcpyAsync(dest, output.data(), uncompressed_bytes, cudaMemcpyDeviceToHost, stream.get()), "Cannot copy expanded bytes back");
        checkCuda(
            cudaMemcpyAsync(actual_bytes.data(), device_actual, num_chunks * sizeof(size_t), cudaMemcpyDeviceToHost, stream.get()),
            "Cannot copy the sizes of expanded GDeflate chunks back");
        checkCuda(
            cudaMemcpyAsync(statuses.data(), device_statuses, num_chunks * sizeof(nvcompStatus_t), cudaMemcpyDeviceToHost, stream.get()),
            "Cannot copy the statuses of a GDeflate expansion back");
        synchronizeStream(stream);
    });

    for (size_t i = 0; i < num_chunks; ++i)
    {
        if (statuses[i] != nvcompSuccess || actual_bytes[i] != decompressed_bytes[i])
            throw Exception(
                ErrorCodes::CANNOT_DECOMPRESS,
                "GDeflate chunk {} expanded to {} bytes with nvcomp status {}, expected {}",
                i,
                actual_bytes[i],
                static_cast<int>(statuses[i]),
                decompressed_bytes[i]);
    }
}

}

#endif
