#include <GPU/GPUDecompression.h>

#if USE_GPU

#include <GPU/CompressedHostColumn.h>
#include <GPU/GPUDevice.h>
#include <GPU/GPUMemory.h>

#include <Common/Exception.h>
#include <base/scope_guard.h>

#include <nvcomp/gdeflate.h>
#include <nvcomp/lz4.h>
#include <nvcomp/zstd.h>

#include <algorithm>
#include <cstring>
#include <utility>

namespace DB::ErrorCodes
{
    extern const int GPU_ERROR;
}

namespace DB::GPU
{

namespace
{

void checkNvcomp(nvcompStatus_t status, const char * what)
{
    if (status != nvcompSuccess)
        throw Exception(ErrorCodes::GPU_ERROR, "{}: nvcomp status {}", what, static_cast<int>(status));
}

nvcompStatus_t decompressTempBytes(
    GPUCodec codec, size_t num_blocks, size_t max_decompressed, size_t decompressed_total, size_t * temp_bytes, cuda::stream_ref stream)
{
    switch (codec)
    {
        case GPUCodec::LZ4:
            return nvcompBatchedLZ4DecompressGetTempSize(
                num_blocks, max_decompressed, nvcompBatchedLZ4DecompressDefaultOpts, temp_bytes, decompressed_total, stream.get());
        case GPUCodec::ZSTD:
            return nvcompBatchedZstdDecompressGetTempSize(
                num_blocks, max_decompressed, nvcompBatchedZstdDecompressDefaultOpts, temp_bytes, decompressed_total, stream.get());
        case GPUCodec::GDEFLATE:
            return nvcompBatchedGdeflateDecompressGetTempSize(
                num_blocks, max_decompressed, nvcompBatchedGdeflateDecompressDefaultOpts, temp_bytes, decompressed_total, stream.get());
    }
    throw Exception(ErrorCodes::GPU_ERROR, "Unknown GPU codec {}", static_cast<int>(codec));
}

nvcompStatus_t decompressAsync(
    GPUCodec codec,
    const void * const * device_compressed_ptrs,
    const size_t * device_compressed_bytes,
    const size_t * device_decompressed_bytes,
    size_t * device_actual_bytes,
    size_t num_blocks,
    void * device_temp,
    size_t temp_bytes,
    void * const * device_value_ptrs,
    nvcompStatus_t * device_statuses,
    cuda::stream_ref stream)
{
    switch (codec)
    {
        case GPUCodec::LZ4:
            return nvcompBatchedLZ4DecompressAsync(
                device_compressed_ptrs,
                device_compressed_bytes,
                device_decompressed_bytes,
                device_actual_bytes,
                num_blocks,
                device_temp,
                temp_bytes,
                device_value_ptrs,
                nvcompBatchedLZ4DecompressDefaultOpts,
                device_statuses,
                stream.get());
        case GPUCodec::ZSTD:
            return nvcompBatchedZstdDecompressAsync(
                device_compressed_ptrs,
                device_compressed_bytes,
                device_decompressed_bytes,
                device_actual_bytes,
                num_blocks,
                device_temp,
                temp_bytes,
                device_value_ptrs,
                nvcompBatchedZstdDecompressDefaultOpts,
                device_statuses,
                stream.get());
        case GPUCodec::GDEFLATE:
            return nvcompBatchedGdeflateDecompressAsync(
                device_compressed_ptrs,
                device_compressed_bytes,
                device_decompressed_bytes,
                device_actual_bytes,
                num_blocks,
                device_temp,
                temp_bytes,
                device_value_ptrs,
                nvcompBatchedGdeflateDecompressDefaultOpts,
                device_statuses,
                stream.get());
    }
    throw Exception(ErrorCodes::GPU_ERROR, "Unknown GPU codec {}", static_cast<int>(codec));
}

template <typename T>
T * arrayAt(char * base, size_t & offset, size_t count)
{
    T * const values = reinterpret_cast<T *>(base + offset);
    offset += count * sizeof(T);
    return values;
}

/// The chunks of one codec that one batched call of nvcomp expands, in the arrays it takes: where each is on the device,
/// and where it goes. nvcomp expands a chunk by a warp, so that thousands in one launch fill the device, and their
/// statuses are checked with one wait on the device.
struct NvcompBatch
{
    GPUCodec codec;
    std::vector<const void *> compressed;
    std::vector<void *> destinations;
    std::vector<size_t> compressed_bytes;
    std::vector<size_t> decompressed_bytes;
    size_t decompressed_total = 0;
    size_t max_decompressed = 0;

    size_t size() const { return compressed.size(); }

    void add(const char * from, size_t from_bytes, char * to, size_t to_bytes)
    {
        compressed.push_back(from);
        compressed_bytes.push_back(from_bytes);
        destinations.push_back(to);
        decompressed_bytes.push_back(to_bytes);
        decompressed_total += to_bytes;
        max_decompressed = std::max(max_decompressed, to_bytes);
    }
};

}

struct DecompressionBatch::State
{
    explicit State(cuda::stream_ref stream_) : stream(stream_) { }

    NvcompBatch & batchOf(GPUCodec codec)
    {
        for (NvcompBatch & batch : batches)
        {
            if (batch.codec == codec)
                return batch;
        }
        NvcompBatch & batch = batches.emplace_back();
        batch.codec = codec;
        return batch;
    }

    /// Queues the call of nvcomp and the copy of its statuses back to `host_results`.
    void launch(const NvcompBatch & batch);

    /// Once the call is done, that every chunk expanded as its header says.
    void check(const NvcompBatch & batch) const;

    const cuda::stream_ref stream;

    /// The compressed bytes of each column on the device.
    std::vector<DeviceBuffer> uploaded;
    std::vector<NvcompBatch> batches;

    PinnedBuffer host_arguments;
    PinnedBuffer host_results;
    DeviceBuffer device_arguments{stream};
    DeviceBuffer device_results{stream};
    DeviceBuffer device_temp{stream};
};

DecompressionBatch::DecompressionBatch(cuda::stream_ref stream)
    : state(std::make_unique<State>(stream))
{
}

DecompressionBatch::~DecompressionBatch() = default;

void DecompressionBatch::add(const CompressedHostColumn & column, char * destination)
{
    if (column.empty())
        return;

    const cuda::stream_ref stream = state->stream;
    char * const uploaded = state->uploaded.emplace_back(stream).grow(column.bytes.size());
    checkCuda(
        cudaMemcpyAsync(uploaded, column.bytes.data(), column.bytes.size(), cudaMemcpyHostToDevice, stream.get()),
        "Cannot send compressed blocks to the device");

    /// The chunks expand one after another: nvcomp takes destinations of any alignment, see
    /// `nvcompBatchedZstdDecompressGetRequiredAlignments`.
    for (const auto & chunk : column.chunks)
    {
        state->batchOf(chunk.codec).add(uploaded + chunk.offset, chunk.compressed_bytes, destination, chunk.decompressed_bytes);
        destination += chunk.decompressed_bytes;
    }
}

GPUFuture<void> DecompressionBatch::expand()
{
    /// The buffers are freed in the order of the stream, after what is queued on it.
    SCOPE_EXIT({
        state->batches.clear();
        state->uploaded.clear();
    });

    const cuda::stream_ref stream = state->stream;
    for (const NvcompBatch & batch : state->batches)
    {
        state->launch(batch);
        GPUPromise<void>(stream).fulfill().get();
        state->check(batch);
    }
    return GPUPromise<void>(stream).fulfill();
}

void DecompressionBatch::State::launch(const NvcompBatch & batch)
{
    const size_t num_chunks = batch.size();
    const size_t vector_bytes = num_chunks * sizeof(void *);
    const size_t arguments_bytes = 4 * vector_bytes;
    static_assert(sizeof(void *) == sizeof(size_t));

    host_arguments.clear();
    char * const arguments = growBy(host_arguments, arguments_bytes);
    memcpy(arguments, batch.compressed.data(), vector_bytes);
    memcpy(arguments + vector_bytes, batch.destinations.data(), vector_bytes);
    memcpy(arguments + 2 * vector_bytes, batch.compressed_bytes.data(), vector_bytes);
    memcpy(arguments + 3 * vector_bytes, batch.decompressed_bytes.data(), vector_bytes);

    waitOnFailure(stream, [&]
    {
        device_arguments.clear();
        device_arguments.append(bytesOf(host_arguments));

        size_t offset = 0;
        const void * const * d_compressed = arrayAt<const void *>(device_arguments.data(), offset, num_chunks);
        void * const * d_destinations = arrayAt<void *>(device_arguments.data(), offset, num_chunks);
        const size_t * d_compressed_bytes = arrayAt<size_t>(device_arguments.data(), offset, num_chunks);
        const size_t * d_decompressed_bytes = arrayAt<size_t>(device_arguments.data(), offset, num_chunks);

        const size_t results_bytes = num_chunks * (sizeof(size_t) + sizeof(nvcompStatus_t));
        device_results.clear();
        device_results.grow(results_bytes);

        offset = 0;
        size_t * d_actual_bytes = arrayAt<size_t>(device_results.data(), offset, num_chunks);
        nvcompStatus_t * d_statuses = arrayAt<nvcompStatus_t>(device_results.data(), offset, num_chunks);

        size_t temp_bytes = 0;
        checkNvcomp(
            decompressTempBytes(batch.codec, num_chunks, batch.max_decompressed, batch.decompressed_total, &temp_bytes, stream),
            "Cannot size nvcomp's scratch space");
        device_temp.clear();
        device_temp.grow(temp_bytes);

        checkNvcomp(
            decompressAsync(
                batch.codec,
                d_compressed,
                d_compressed_bytes,
                d_decompressed_bytes,
                d_actual_bytes,
                num_chunks,
                device_temp.data(),
                temp_bytes,
                d_destinations,
                d_statuses,
                stream),
            "Cannot decompress on the device");

        host_results.clear();
        char * const results = growBy(host_results, results_bytes);
        checkCuda(
            cudaMemcpyAsync(results, device_results.data(), results_bytes, cudaMemcpyDeviceToHost, stream.get()),
            "Cannot copy the decompression statuses back");
    });
}

void DecompressionBatch::State::check(const NvcompBatch & batch) const
{
    const size_t num_chunks = batch.size();
    size_t offset = 0;
    char * const results = const_cast<char *>(host_results.data());
    const size_t * actual_bytes = arrayAt<size_t>(results, offset, num_chunks);
    const nvcompStatus_t * statuses = arrayAt<nvcompStatus_t>(results, offset, num_chunks);

    for (size_t i = 0; i < num_chunks; ++i)
    {
        if (statuses[i] != nvcompSuccess)
            throw Exception(ErrorCodes::GPU_ERROR, "Block {} did not decompress: nvcomp status {}", i, static_cast<int>(statuses[i]));
        if (actual_bytes[i] != batch.decompressed_bytes[i])
            throw Exception(ErrorCodes::GPU_ERROR, "Block {} expanded to {} bytes, expected {}", i, actual_bytes[i], batch.decompressed_bytes[i]);
    }
}

}

#endif
