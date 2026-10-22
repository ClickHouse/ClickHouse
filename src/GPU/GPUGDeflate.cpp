#include <GPU/GPUGDeflate.h>

#if USE_GPU

#include <GPU/BitUnpack.cuh>
#include <GPU/GPUDecompression.h>
#include <GPU/GPUDevice.h>
#include <GPU/GPUMemory.h>

#include <Common/Exception.h>
#include <base/types.h>

#include <nvcomp/gdeflate.h>

#include <algorithm>
#include <bit>
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

/// Set in the number of chunks of a block whose chunks have descriptors of `packed_descriptor_bytes`.
constexpr UInt32 packed_flag = UInt32{1} << 31;
constexpr size_t plain_descriptor_bytes = sizeof(UInt32);
constexpr size_t packed_descriptor_bytes = 16;

size_t alignUp(size_t bytes, size_t alignment)
{
    return (bytes + alignment - 1) / alignment * alignment;
}

size_t numChunks(size_t uncompressed_bytes)
{
    return (uncompressed_bytes + gdeflate_chunk_bytes - 1) / gdeflate_chunk_bytes;
}

size_t headerBytes(size_t num_chunks, bool packed)
{
    return sizeof(UInt32) + num_chunks * (packed ? packed_descriptor_bytes : plain_descriptor_bytes);
}

template <typename T>
T readAt(const char * at)
{
    T value;
    memcpy(&value, at, sizeof(value));
    return value;
}

template <typename T>
void writeAt(char * at, T value)
{
    memcpy(at, &value, sizeof(value));
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

/// An integer of `element_size` bytes, widened to 64 bits as what it is, signed or not, so that the integers of a chunk
/// order as their type does.
uint64_t integerAt(const char * at, uint32_t element_size, bool is_signed)
{
    switch (element_size)
    {
        case 1: return is_signed ? static_cast<uint64_t>(static_cast<int64_t>(readAt<Int8>(at))) : readAt<UInt8>(at);
        case 2: return is_signed ? static_cast<uint64_t>(static_cast<int64_t>(readAt<Int16>(at))) : readAt<UInt16>(at);
        case 4: return is_signed ? static_cast<uint64_t>(static_cast<int64_t>(readAt<Int32>(at))) : readAt<UInt32>(at);
        default: return readAt<UInt64>(at);
    }
}

struct PackedInput
{
    std::vector<char> bytes;
    uint32_t bits = 0;
    uint64_t reference = 0;
};

/// The chunk packed, if its integers take fewer bits so than they are wide; otherwise nothing.
PackedInput pack(const char * chunk, size_t chunk_bytes, uint32_t element_size, bool is_signed)
{
    const size_t count = chunk_bytes / element_size;
    uint64_t least = integerAt(chunk, element_size, is_signed);
    uint64_t most = least;
    for (size_t i = 1; i < count; ++i)
    {
        const uint64_t value = integerAt(chunk + i * element_size, element_size, is_signed);
        if (is_signed ? static_cast<int64_t>(value) < static_cast<int64_t>(least) : value < least)
            least = value;
        if (is_signed ? static_cast<int64_t>(value) > static_cast<int64_t>(most) : value > most)
            most = value;
    }

    const auto bits = std::max<uint32_t>(1, static_cast<uint32_t>(std::bit_width(most - least)));
    if (bits > max_packed_bits || bits >= 8 * element_size)
        return {};

    PackedInput packed{.bytes = std::vector<char>((count * bits + 7) / 8 + 8, 0), .bits = bits, .reference = least};
    auto * out = reinterpret_cast<unsigned char *>(packed.bytes.data());
    for (size_t i = 0; i < count; ++i)
    {
        const uint64_t delta = integerAt(chunk + i * element_size, element_size, is_signed) - least;
        const size_t first_bit = i * bits;
        for (uint32_t b = 0; b < bits; b += 8)
        {
            const size_t bit = first_bit + b;
            const uint64_t part = delta >> b;
            out[bit / 8] |= static_cast<unsigned char>(part << (bit % 8));
            if (bit % 8 != 0)
                out[bit / 8 + 1] |= static_cast<unsigned char>(part >> (8 - bit % 8));
        }
    }
    packed.bytes.resize((count * bits + 7) / 8);
    return packed;
}

}

size_t gdeflateMaxCompressedBytes(size_t uncompressed_bytes, int algorithm)
{
    size_t max_chunk = 0;
    checkNvcomp(
        nvcompBatchedGdeflateCompressGetMaxOutputChunkSize(gdeflate_chunk_bytes, compressOpts(algorithm), &max_chunk),
        "Cannot bound the size of a GDeflate chunk");
    const size_t num_chunks = numChunks(uncompressed_bytes);
    return headerBytes(num_chunks, true) + num_chunks * alignUp(max_chunk, chunk_alignment);
}

size_t gdeflateCompress(const char * source, size_t source_bytes, char * dest, int algorithm, uint32_t element_size, bool is_signed)
{
    const size_t num_chunks = numChunks(source_bytes);
    /// A block that ends in the middle of an integer is not packed: its chunks do not hold whole integers.
    const bool packing = (element_size == 1 || element_size == 2 || element_size == 4 || element_size == 8) && source_bytes % element_size == 0;
    writeAt<UInt32>(dest, static_cast<UInt32>(num_chunks) | (packing ? packed_flag : 0));
    if (num_chunks == 0)
        return sizeof(UInt32);

    std::vector<PackedInput> packed(num_chunks);
    std::vector<size_t> packed_input(num_chunks, SIZE_MAX);
    size_t num_inputs = num_chunks;
    size_t packed_bytes = 0;
    if (packing)
    {
        for (size_t i = 0; i < num_chunks; ++i)
        {
            const size_t chunk_bytes = std::min(gdeflate_chunk_bytes, source_bytes - i * gdeflate_chunk_bytes);
            packed[i] = pack(source + i * gdeflate_chunk_bytes, chunk_bytes, element_size, is_signed);
            if (packed[i].bits)
            {
                packed_input[i] = num_inputs++;
                packed_bytes += packed[i].bytes.size();
            }
        }
    }

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

    /// Both the plain and the packed form of each chunk that has one are compressed, in one batch, and the smaller kept.
    std::vector<size_t> compressed_sizes(num_inputs);
    waitOnFailure(stream, [&]
    {
        input.reserve(source_bytes + packed_bytes);
        input.append(std::string_view(source, source_bytes));
        std::vector<const void *> in_ptrs(num_inputs);
        std::vector<size_t> in_bytes(num_inputs);
        for (size_t i = 0; i < num_chunks; ++i)
        {
            in_ptrs[i] = input.data() + i * gdeflate_chunk_bytes;
            in_bytes[i] = std::min(gdeflate_chunk_bytes, source_bytes - i * gdeflate_chunk_bytes);
        }
        for (size_t i = 0; i < num_chunks; ++i)
        {
            if (packed_input[i] == SIZE_MAX)
                continue;
            const size_t at = input.size();
            input.append(std::string_view(packed[i].bytes.data(), packed[i].bytes.size()));
            in_ptrs[packed_input[i]] = input.data() + at;
            in_bytes[packed_input[i]] = packed[i].bytes.size();
        }

        output.grow(num_inputs * max_chunk);
        std::vector<void *> out_ptrs(num_inputs);
        for (size_t i = 0; i < num_inputs; ++i)
            out_ptrs[i] = output.data() + i * max_chunk;

        size_t temp_bytes = 0;
        checkNvcomp(
            nvcompBatchedGdeflateCompressGetTempSize(num_inputs, gdeflate_chunk_bytes, opts, &temp_bytes, source_bytes + packed_bytes, stream.get()),
            "Cannot size the scratch space of a GDeflate compression");
        temp.grow(std::max<size_t>(temp_bytes, 1));
        out_sizes.grow(num_inputs * sizeof(size_t));

        checkNvcomp(
            nvcompBatchedGdeflateCompressAsync(
                toDevice(in_pointers, in_ptrs),
                toDevice(in_sizes, in_bytes),
                gdeflate_chunk_bytes,
                num_inputs,
                temp.data(),
                temp_bytes,
                const_cast<void * const *>(toDevice(out_pointers, out_ptrs)),
                reinterpret_cast<size_t *>(out_sizes.data()),
                opts,
                nullptr,
                stream.get()),
            "Cannot compress with GDeflate on the device");

        checkCuda(
            cudaMemcpyAsync(compressed_sizes.data(), out_sizes.data(), num_inputs * sizeof(size_t), cudaMemcpyDeviceToHost, stream.get()),
            "Cannot copy the sizes of GDeflate chunks back");
        synchronizeStream(stream);

        size_t at = headerBytes(num_chunks, packing);
        for (size_t i = 0; i < num_chunks; ++i)
        {
            const bool use_packed = packed_input[i] != SIZE_MAX && compressed_sizes[packed_input[i]] < compressed_sizes[i];
            const size_t chosen = use_packed ? packed_input[i] : i;
            if (compressed_sizes[chosen] > max_chunk)
                throw Exception(
                    ErrorCodes::CANNOT_COMPRESS, "GDeflate compressed a chunk to {} bytes, more than {}", compressed_sizes[chosen], max_chunk);

            char * const descriptor = dest + headerBytes(i, packing);
            writeAt<UInt32>(descriptor, static_cast<UInt32>(compressed_sizes[chosen]));
            if (packing)
            {
                descriptor[4] = static_cast<char>(use_packed ? packed[i].bits : 0);
                descriptor[5] = static_cast<char>(element_size);
                writeAt<UInt16>(descriptor + 6, 0);
                writeAt<UInt64>(descriptor + 8, use_packed ? packed[i].reference : 0);
            }

            checkCuda(
                cudaMemcpyAsync(dest + at, output.data() + chosen * max_chunk, compressed_sizes[chosen], cudaMemcpyDeviceToHost, stream.get()),
                "Cannot copy a GDeflate chunk back");
            const size_t next = alignUp(at + compressed_sizes[chosen], chunk_alignment);
            memset(dest + at + compressed_sizes[chosen], 0, next - at - compressed_sizes[chosen]);
            at = next;
        }
        synchronizeStream(stream);
    });

    size_t at = headerBytes(num_chunks, packing);
    for (size_t i = 0; i < num_chunks; ++i)
        at = alignUp(at + readAt<UInt32>(dest + headerBytes(i, packing)), chunk_alignment);
    return at;
}

void forEachGDeflateChunk(std::string_view block, size_t uncompressed_bytes, const std::function<void(const GDeflateChunk &)> & on_chunk)
{
    if (block.size() < sizeof(UInt32))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "A GDeflate block of {} bytes", block.size());

    const UInt32 header = readAt<UInt32>(block.data());
    const bool packed = header & packed_flag;
    const size_t num_chunks = header & ~packed_flag;
    if (num_chunks != numChunks(uncompressed_bytes) || headerBytes(num_chunks, packed) > block.size())
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "A GDeflate block of {} bytes holds {} chunks, where {} expanded bytes make {}",
            block.size(),
            num_chunks,
            uncompressed_bytes,
            numChunks(uncompressed_bytes));

    size_t at = headerBytes(num_chunks, packed);
    for (size_t i = 0; i < num_chunks; ++i)
    {
        const char * const descriptor = block.data() + headerBytes(i, packed);
        GDeflateChunk chunk{
            .offset = at,
            .compressed_bytes = readAt<UInt32>(descriptor),
            .decompressed_bytes = std::min(gdeflate_chunk_bytes, uncompressed_bytes - i * gdeflate_chunk_bytes),
        };
        chunk.expanded_bytes = chunk.decompressed_bytes;
        if (packed && descriptor[4] != 0)
        {
            chunk.packed_bits = static_cast<unsigned char>(descriptor[4]);
            chunk.element_size = static_cast<unsigned char>(descriptor[5]);
            chunk.reference = readAt<UInt64>(descriptor + 8);
            if (chunk.packed_bits > max_packed_bits
                || (chunk.element_size != 1 && chunk.element_size != 2 && chunk.element_size != 4 && chunk.element_size != 8)
                || chunk.decompressed_bytes % chunk.element_size != 0)
                throw Exception(
                    ErrorCodes::CANNOT_DECOMPRESS,
                    "Chunk {} of a GDeflate block packs integers of {} bytes in {} bits",
                    i,
                    chunk.element_size,
                    chunk.packed_bits);
            chunk.expanded_bytes = (chunk.decompressed_bytes / chunk.element_size * chunk.packed_bits + 7) / 8;
        }

        if (at + chunk.compressed_bytes > block.size())
            throw Exception(
                ErrorCodes::CANNOT_DECOMPRESS, "Chunk {} of a GDeflate block of {} bytes ends at {}", i, block.size(), at + chunk.compressed_bytes);
        on_chunk(chunk);
        at = alignUp(at + chunk.compressed_bytes, chunk_alignment);
    }
}

void gdeflateDecompress(const char * source, size_t source_bytes, char * dest, size_t uncompressed_bytes)
{
    std::vector<CompressedBlock> blocks;
    forEachGDeflateChunk({source, source_bytes}, uncompressed_bytes, [&](const GDeflateChunk & chunk)
    {
        blocks.push_back({
            .offset = chunk.offset,
            .compressed_bytes = chunk.compressed_bytes,
            .decompressed_bytes = chunk.decompressed_bytes,
            .packed_bytes = chunk.expanded_bytes,
            .packed_bits = chunk.packed_bits,
            .element_size = chunk.element_size,
            .reference = chunk.reference,
        });
    });
    if (blocks.empty())
        return;

    DeviceBuffer output(GPUResources::get().decompression);
    output.grow(uncompressed_bytes);
    SyncDecompressor decompressor;
    decompressor.decompress(GPUCodec::GDEFLATE, {source, source_bytes}, blocks, output.data());
    checkCuda(cudaMemcpy(dest, output.data(), uncompressed_bytes, cudaMemcpyDeviceToHost), "Cannot copy expanded bytes back");
}

}

#endif
