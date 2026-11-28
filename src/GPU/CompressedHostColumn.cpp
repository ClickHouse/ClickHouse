#include <GPU/CompressedHostColumn.h>

#if USE_GPU

#include <GPU/GPUTypeMapping.h>

#include <Common/Exception.h>
#include <Compression/CompressionCodecGDeflate.h>
#include <base/unaligned.h>

#include <algorithm>

namespace DB::ErrorCodes
{
    extern const int CANNOT_DECOMPRESS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace DB::GPU
{

namespace
{

/// Where each compressed chunk starts on the device, see `nvcompZstdRequiredDecompressionAlignment`.
constexpr size_t compressed_alignment = 8;

size_t alignedUp(size_t bytes)
{
    return (bytes + compressed_alignment - 1) / compressed_alignment * compressed_alignment;
}

}

void CompressedHostColumn::append(
    UInt8 method, std::string_view payload, size_t decompressed_bytes, size_t skipped, size_t kept, size_t reserve_bytes)
{
    /// The plan knows the codecs of the table, not those of the blocks of a part: a part written before an `ALTER` of a
    /// codec, or by the adaptive codec, may hold blocks of others.
    const auto codec = codecOf(method);
    if (!codec)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "A block of compression method {:#x} read for the GPU, which the device does not expand; such a part is aggregated "
            "on the CPU with `allow_experimental_gpu_aggregation = 0`",
            static_cast<UInt16>(method));

    if (skipped + kept > decompressed_bytes)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} bytes kept after {} skipped of a block of {} for the GPU", kept, skipped, decompressed_bytes);
    if (isCut() || (!empty() && skipped != 0))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A block for the GPU that skips {} bytes after the first of a run, or after one cut short", skipped);

    if (empty())
    {
        skipped_bytes = skipped;
        bytes.reserve(std::max(reserve_bytes, payload.size()));
    }
    kept_bytes += kept;
    expanded_bytes += decompressed_bytes;

    bytes.resize_fill(alignedUp(bytes.size()), 0);
    const size_t offset = bytes.size();
    appendHostBytes(bytes, payload);

    if (*codec != GPUCodec::GDEFLATE)
    {
        chunks.push_back({.codec = *codec, .offset = offset, .compressed_bytes = payload.size(), .decompressed_bytes = decompressed_bytes});
        return;
    }

    /// The number of chunks, the compressed size of each, then the chunks, each from a multiple of 4 bytes.
    constexpr size_t chunk_bytes = CompressionCodecGDeflate::chunk_bytes;
    constexpr size_t chunk_alignment = 4;
    if (payload.size() < sizeof(UInt32))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "A GDeflate block of {} bytes", payload.size());
    const size_t num_chunks = unalignedLoadLittleEndian<UInt32>(payload.data());
    const size_t header_bytes = sizeof(UInt32) * (1 + num_chunks);
    if (num_chunks != (decompressed_bytes + chunk_bytes - 1) / chunk_bytes || header_bytes > payload.size())
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "A GDeflate block of {} bytes holds {} chunks, where {} expanded bytes make {}",
            payload.size(),
            num_chunks,
            decompressed_bytes,
            (decompressed_bytes + chunk_bytes - 1) / chunk_bytes);

    size_t at = header_bytes;
    for (size_t i = 0; i < num_chunks; ++i)
    {
        const size_t compressed_bytes = unalignedLoadLittleEndian<UInt32>(payload.data() + sizeof(UInt32) * (1 + i));
        if (at + compressed_bytes > payload.size())
            throw Exception(
                ErrorCodes::CANNOT_DECOMPRESS, "Chunk {} of a GDeflate block of {} bytes ends at {}", i, payload.size(), at + compressed_bytes);
        chunks.push_back({
            .codec = *codec,
            .offset = offset + at,
            .compressed_bytes = compressed_bytes,
            .decompressed_bytes = std::min(chunk_bytes, decompressed_bytes - i * chunk_bytes),
        });
        at = (at + compressed_bytes + chunk_alignment - 1) / chunk_alignment * chunk_alignment;
    }
}

void CompressedHostColumn::clear()
{
    bytes.clear();
    chunks.clear();
    expanded_bytes = 0;
    skipped_bytes = 0;
    kept_bytes = 0;
}

}

#endif
