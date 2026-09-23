#include <GPU/GPUDecompression.h>

#if USE_GPU

#include <GPU/GPUDevice.h>
#include <GPU/GPUMemory.h>

#include <Common/Exception.h>

#include <nvcomp/lz4.h>
#include <nvcomp/zstd.h>

#include <algorithm>
#include <utility>

namespace DB::ErrorCodes
{
    extern const int GPU_ERROR;
    extern const int LOGICAL_ERROR;
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

nvcompStatus_t decompressTempBytes(GPUCodec codec, size_t num_blocks, size_t max_decompressed, size_t decompressed_total, size_t * temp_bytes)
{
    switch (codec)
    {
        case GPUCodec::LZ4:
            return nvcompBatchedLZ4DecompressGetTempSizeAsync(
                num_blocks, max_decompressed, nvcompBatchedLZ4DecompressDefaultOpts, temp_bytes, decompressed_total);
        case GPUCodec::ZSTD:
            return nvcompBatchedZstdDecompressGetTempSizeAsync(
                num_blocks, max_decompressed, nvcompBatchedZstdDecompressDefaultOpts, temp_bytes, decompressed_total);
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
    cudaStream_t stream)
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
                stream);
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
                stream);
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

}

size_t decompressedBytesOf(std::span<const CompressedBlock> blocks)
{
    size_t total = 0;
    for (const CompressedBlock & block : blocks)
        total += block.decompressed_bytes;
    return total;
}

Decompressor::Decompressor()
    : device_compressed(StreamRegistry::get().decompression)
    , device_arguments(StreamRegistry::get().decompression)
    , device_results(StreamRegistry::get().decompression)
    , device_temp(StreamRegistry::get().decompression)
{
    for (Slot & slot : slots)
        slot.values = DeviceBuffer(StreamRegistry::get().decompression);
}

Decompressor::Decompressor(Decompressor && other) noexcept
    : in_flight(std::exchange(other.in_flight, false))
    , started_destination(std::exchange(other.started_destination, nullptr))
    , host_arguments(std::move(other.host_arguments))
    , host_results(std::move(other.host_results))
    , device_compressed(std::move(other.device_compressed))
    , device_arguments(std::move(other.device_arguments))
    , device_results(std::move(other.device_results))
    , device_temp(std::move(other.device_temp))
    , expanded(std::move(other.expanded))
    , expected_bytes(std::move(other.expected_bytes))
    , current_slot(other.current_slot)
{
    for (size_t i = 0; i < num_slots; ++i)
    {
        slots[i].values = std::move(other.slots[i].values);
        slots[i].copied_out = std::move(other.slots[i].copied_out);
        slots[i].in_use = std::exchange(other.slots[i].in_use, false);
    }
}

Decompressor::~Decompressor()
{
    try
    {
        if (in_flight)
            expanded.wait();
        for (Slot & slot : slots)
        {
            if (slot.in_use)
                slot.copied_out.wait();
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void Decompressor::decompress(
    GPUCodec codec, std::string_view host_compressed, std::span<const CompressedBlock> blocks, char * device_destination)
{
    size_t compressed_total = 0;
    for (const CompressedBlock & block : blocks)
        compressed_total = std::max(compressed_total, block.offset + block.compressed_bytes);

    if (compressed_total > host_compressed.size())
        throw Exception(
            ErrorCodes::GPU_ERROR,
            "The compressed blocks reach {} bytes into a staging buffer of {}",
            compressed_total,
            host_compressed.size());

    if (blocks.empty())
        return;

    if (in_flight)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A decompression was asked for while an expansion was in flight");

    DeviceEvent destination_ready;
    destination_ready.record(StreamRegistry::get().compute);
    destination_ready.waitOn(StreamRegistry::get().decompression);

    device_compressed.clear();
    device_compressed.append(host_compressed.substr(0, compressed_total));

    const Piece piece{
        .device_compressed = device_compressed.data(),
        .compressed_bytes = compressed_total,
        .blocks = blocks,
    };
    queue(codec, std::span<const Piece>(&piece, 1), device_destination);
    waitAndCheck();
}

void Decompressor::start(GPUCodec codec, std::span<const Piece> pieces)
{
    if (in_flight)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "An expansion was started while the last one was not finished");

    size_t total = 0;
    for (const Piece & piece : pieces)
        total += decompressedBytesOf(piece.blocks);

    Slot & slot = slots[current_slot];
    if (slot.in_use)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A decompression buffer was taken again before being released");

    slot.copied_out.waitOn(StreamRegistry::get().decompression);
    slot.values.clear();
    started_destination = slot.values.grow(total);

    queue(codec, pieces, started_destination);
    in_flight = true;
}

const char * Decompressor::finish()
{
    if (!in_flight)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "An expansion was finished without being started");

    waitAndCheck();
    in_flight = false;
    slots[current_slot].in_use = true;
    return started_destination;
}

void Decompressor::release()
{
    Slot & slot = slots[current_slot];
    if (!slot.in_use)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A decompression buffer was released without being taken");

    slot.copied_out.record(StreamRegistry::get().compute);
    slot.in_use = false;
    current_slot = (current_slot + 1) % num_slots;
}

void Decompressor::queue(GPUCodec codec, std::span<const Piece> pieces, char * destination)
{
    const cudaStream_t stream = StreamRegistry::get().decompression;

    size_t num_blocks = 0;
    size_t decompressed_total = 0;
    size_t max_decompressed = 0;

    for (const Piece & piece : pieces)
    {
        size_t piece_compressed = 0;
        for (const CompressedBlock & block : piece.blocks)
        {
            piece_compressed = std::max(piece_compressed, block.offset + block.compressed_bytes);
            decompressed_total += block.decompressed_bytes;
            max_decompressed = std::max(max_decompressed, block.decompressed_bytes);
        }

        if (piece_compressed > piece.compressed_bytes)
            throw Exception(
                ErrorCodes::GPU_ERROR,
                "The compressed blocks reach {} bytes into an upload of {}",
                piece_compressed,
                piece.compressed_bytes);

        num_blocks += piece.blocks.size();

        if (piece.uploaded)
            piece.uploaded->waitOn(stream);
    }

    expected_bytes.clear();
    expected_bytes.reserve(num_blocks);

    if (num_blocks == 0)
    {
        expanded.record(stream);
        return;
    }

    const size_t arguments_bytes = num_blocks * (2 * sizeof(void *) + 2 * sizeof(size_t));
    host_arguments.clear();
    char * const arguments = host_arguments.grow(arguments_bytes);

    size_t offset = 0;
    const void ** compressed_ptrs = arrayAt<const void *>(arguments, offset, num_blocks);
    void ** value_ptrs = arrayAt<void *>(arguments, offset, num_blocks);
    size_t * compressed_bytes = arrayAt<size_t>(arguments, offset, num_blocks);
    size_t * decompressed_bytes = arrayAt<size_t>(arguments, offset, num_blocks);

    size_t block_index = 0;
    size_t at = 0;
    for (const Piece & piece : pieces)
    {
        for (const CompressedBlock & block : piece.blocks)
        {
            compressed_ptrs[block_index] = piece.device_compressed + block.offset;
            value_ptrs[block_index] = destination + at;
            compressed_bytes[block_index] = block.compressed_bytes;
            decompressed_bytes[block_index] = block.decompressed_bytes;
            expected_bytes.push_back(block.decompressed_bytes);
            at += block.decompressed_bytes;
            ++block_index;
        }
    }

    device_arguments.clear();
    device_arguments.append(host_arguments.bytes());

    offset = 0;
    const void * const * d_compressed_ptrs = arrayAt<const void *>(device_arguments.data(), offset, num_blocks);
    void * const * d_value_ptrs = arrayAt<void *>(device_arguments.data(), offset, num_blocks);
    const size_t * d_compressed_bytes = arrayAt<size_t>(device_arguments.data(), offset, num_blocks);
    const size_t * d_decompressed_bytes = arrayAt<size_t>(device_arguments.data(), offset, num_blocks);

    const size_t results_bytes = num_blocks * (sizeof(size_t) + sizeof(nvcompStatus_t));
    device_results.clear();
    device_results.grow(results_bytes);

    offset = 0;
    size_t * d_actual_bytes = arrayAt<size_t>(device_results.data(), offset, num_blocks);
    nvcompStatus_t * d_statuses = arrayAt<nvcompStatus_t>(device_results.data(), offset, num_blocks);

    size_t temp_bytes = 0;
    checkNvcomp(
        decompressTempBytes(codec, num_blocks, max_decompressed, decompressed_total, &temp_bytes),
        "Cannot size nvcomp's scratch space");

    device_temp.clear();
    device_temp.grow(temp_bytes);

    checkNvcomp(
        decompressAsync(
            codec,
            d_compressed_ptrs,
            d_compressed_bytes,
            d_decompressed_bytes,
            d_actual_bytes,
            num_blocks,
            device_temp.data(),
            temp_bytes,
            d_value_ptrs,
            d_statuses,
            stream),
        "Cannot decompress on the device");

    host_results.clear();
    char * const results = host_results.grow(results_bytes);

    checkCuda(
        cudaMemcpyAsync(results, device_results.data(), results_bytes, cudaMemcpyDeviceToHost, stream),
        "Cannot copy the decompression statuses back");

    expanded.record(stream);
}

void Decompressor::waitAndCheck()
{
    expanded.wait();

    const size_t num_blocks = expected_bytes.size();
    if (num_blocks == 0)
        return;

    size_t offset = 0;
    const size_t * actual_bytes = arrayAt<size_t>(host_results.data(), offset, num_blocks);
    const nvcompStatus_t * statuses = arrayAt<nvcompStatus_t>(host_results.data(), offset, num_blocks);

    for (size_t block_index = 0; block_index < num_blocks; ++block_index)
    {
        if (statuses[block_index] != nvcompSuccess)
            throw Exception(
                ErrorCodes::GPU_ERROR, "Block {} did not decompress: nvcomp status {}", block_index, static_cast<int>(statuses[block_index]));
        if (actual_bytes[block_index] != expected_bytes[block_index])
            throw Exception(
                ErrorCodes::GPU_ERROR,
                "Block {} expanded to {} bytes, expected {}",
                block_index,
                actual_bytes[block_index],
                expected_bytes[block_index]);
    }
}

}

#endif
