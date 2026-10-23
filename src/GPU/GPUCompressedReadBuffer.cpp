#include <GPU/GPUCompressedReadBuffer.h>

#if USE_GPU

#include <GPU/GPUTypes.cuh>

#include <Compression/ICompressionCodec.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

namespace ProfileEvents
{
    extern const Event GPUDecompressionBytes;
    extern const Event GPUDecompressionMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}

namespace
{

/// What the blocks of a batch expand to, at most, but for the block that crosses it: enough for the device to expand them
/// at a good part of its speed, and little enough for every stream of every reading thread to hold one in pinned memory.
constexpr size_t batch_bytes = 4UL << 20;

}

GPUCompressedReadBuffer::GPUCompressedReadBuffer(std::unique_ptr<ReadBufferFromFileBase> file_in_, bool allow_different_codecs_)
    : ReadBuffer(nullptr, 0)
    , file_in(std::move(file_in_))
    , stream(GPU::createStream())
{
    compressed_in = file_in.get();
    allow_different_codecs = allow_different_codecs_;
    decompressor.emplace(cuda::stream_ref{stream.get()});
    expanded.reserve(batch_bytes + batch_bytes / 4);
}

bool GPUCompressedReadBuffer::readBatch()
{
    batch.clear();
    next_block = 0;
    staged.clear();
    device_blocks.clear();
    device_packed.clear();

    constexpr UInt8 header_size = ICompressionCodec::getHeaderSize();
    std::optional<GPU::GPUCodec> device_codec;
    size_t expanded_bytes = 0;
    size_t device_compressed_bytes = 0;

    while (expanded_bytes < batch_bytes)
    {
        const size_t offset_in_file = file_in->getPosition();
        size_t decompressed_bytes = 0;
        size_t compressed_with_header = 0;
        if (readCompressedData(decompressed_bytes, compressed_with_header, /*always_copy=*/false) == 0)
            break;

        const auto method = static_cast<UInt8>(compressed_buffer[0]);
        const std::string_view payload(compressed_buffer + header_size, compressed_with_header - header_size);

        /// Every block of a codec the device expands goes to it. Those of the device come first, of one codec, as one call
        /// of nvcomp expands them.
        const auto codec_on_device = GPU::deviceCodecOf(method, payload);
        const bool on_device = codec_on_device && (!device_codec || *device_codec == *codec_on_device)
            && expanded_bytes == device_blocks_expanded;
        if (on_device)
        {
            GPU::stageCompressedBlock(staged, device_blocks, device_packed, method, payload, decompressed_bytes);
            device_codec = codec_on_device;
            batch.push_back({.offset_in_file = offset_in_file, .offset_in_expanded = expanded_bytes, .decompressed_bytes = decompressed_bytes});
            expanded_bytes += decompressed_bytes;
            device_blocks_expanded = expanded_bytes;
            device_compressed_bytes += payload.size();
            continue;
        }

        /// A block for the CPU, after those of the device: the batch ends with it.
        expanded.resize(expanded_bytes + decompressed_bytes + codec->getAdditionalSizeAtTheEndOfBuffer());
        decompressTo(expanded.data() + expanded_bytes, decompressed_bytes, compressed_with_header);
        batch.push_back({.offset_in_file = offset_in_file, .offset_in_expanded = expanded_bytes, .decompressed_bytes = decompressed_bytes});
        expanded_bytes += decompressed_bytes;
        break;
    }

    if (!device_blocks.empty())
    {
        if (expanded.size() < device_blocks_expanded)
            expanded.resize(device_blocks_expanded);
        Stopwatch watch;
        decompressor->decompressToHost(*device_codec, GPU::bytesOf(staged), device_blocks, device_packed, expanded.data());
        ProfileEvents::increment(ProfileEvents::GPUDecompressionBytes, device_compressed_bytes);
        ProfileEvents::increment(ProfileEvents::GPUDecompressionMicroseconds, watch.elapsedMicroseconds());
    }
    device_blocks_expanded = 0;

    return !batch.empty();
}

bool GPUCompressedReadBuffer::nextImpl()
{
    try
    {
        if (next_block >= batch.size() && !readBatch())
            return false;

        const Block & block = batch[next_block++];
        working_buffer = Buffer(expanded.data() + block.offset_in_expanded, expanded.data() + block.offset_in_expanded + block.decompressed_bytes);

        /// Set by a lazy seek, see `seek`.
        if (nextimpl_working_buffer_offset > working_buffer.size())
            throw Exception(
                ErrorCodes::SEEK_POSITION_OUT_OF_BOUND,
                "Required to move position beyond the decompressed block (pos: {}, block size: {})",
                nextimpl_working_buffer_offset,
                working_buffer.size());
        return true;
    }
    catch (Exception & e)
    {
        addDiagnostics(e);
        throw;
    }
}

void GPUCompressedReadBuffer::seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block)
{
    for (size_t i = 0; i < batch.size(); ++i)
    {
        if (batch[i].offset_in_file != offset_in_compressed_file)
            continue;

        /// The block the buffer is in.
        if (i + 1 == next_block && !working_buffer.empty() && offset_in_decompressed_block <= working_buffer.size())
        {
            pos = working_buffer.begin() + offset_in_decompressed_block;
            return;
        }

        next_block = i;
        resetWorkingBuffer();
        nextimpl_working_buffer_offset = offset_in_decompressed_block;
        return;
    }

    batch.clear();
    next_block = 0;
    file_in->seek(offset_in_compressed_file, SEEK_SET);
    resetWorkingBuffer();
    nextimpl_working_buffer_offset = offset_in_decompressed_block;
}

off_t GPUCompressedReadBuffer::getPosition() const
{
    return file_in->getPosition();
}

}

#endif
