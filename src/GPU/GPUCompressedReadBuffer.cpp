#include <GPU/GPUCompressedReadBuffer.h>

#if USE_GPU

#include <GPU/GPUDevice.h>
#include <GPU/GPUTypes.cuh>

#include <Compression/CompressionInfo.h>
#include <Compression/ICompressionCodec.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <mutex>

namespace ProfileEvents
{
    extern const Event GPUDecompressionBytes;
    extern const Event GPUDecompressionMicroseconds;
    extern const Event GPUDecompressionWaitMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int GPU_ERROR;
}

namespace
{

/// What the blocks of a batch expand to, at most, but for the block that crosses it: enough for the device to expand them
/// at a good part of its speed, and little enough for every stream of every reading thread to hold two in pinned memory.
constexpr size_t max_batch_bytes = 4UL << 20;

}

GPUCompressedReadBuffer::Slot::Slot()
    : stream(GPU::createStream())
    , device_compressed(cuda::stream_ref{stream.get()})
    , device_expanded(cuda::stream_ref{stream.get()})
    , decompression(cuda::stream_ref{stream.get()})
{
    expanded.reserve(max_batch_bytes + max_batch_bytes / 4);
}

/// The memory of the slots belongs to the server, not to the query that happens to grow or free it: a slot outlives
/// the queries it serves. So it is kept out of the memory trackers of the queries, whose accounts would otherwise be
/// charged in one query and credited in another. The pinned pool bounds it by its release threshold.
class GPUCompressedReadBuffer::SlotPool
{
public:
    static SlotPool & instance()
    {
        static SlotPool pool;
        return pool;
    }

    std::unique_ptr<Slot> acquire()
    {
        {
            std::lock_guard lock(mutex);
            if (!idle.empty())
            {
                auto slot = std::move(idle.back());
                idle.pop_back();
                return slot;
            }
        }
        MemoryTrackerBlockerInThread blocker;
        return std::make_unique<Slot>();
    }

    void release(std::unique_ptr<Slot> slot)
    {
        slot->batch.clear();
        slot->full = false;
        std::unique_lock lock(mutex);
        if (idle.size() < max_idle)
        {
            idle.push_back(std::move(slot));
            return;
        }
        lock.unlock();
        MemoryTrackerBlockerInThread blocker;
        slot.reset();
    }

private:
    /// Each holds about two batches of pinned memory and as much on the device.
    static constexpr size_t max_idle = 128;

    std::mutex mutex;
    std::vector<std::unique_ptr<Slot>> idle;
};

GPUCompressedReadBuffer::GPUCompressedReadBuffer(
    std::unique_ptr<ReadBufferFromFileBase> file_in_, bool allow_different_codecs_, bool zstd_on_device_, bool lz4_on_device_)
    : ReadBuffer(nullptr, 0)
    , file_in(std::move(file_in_))
    , zstd_on_device(zstd_on_device_)
    , lz4_on_device(lz4_on_device_)
{
    compressed_in = file_in.get();
    allow_different_codecs = allow_different_codecs_;
}

GPUCompressedReadBuffer::~GPUCompressedReadBuffer()
{
    /// The device must be done with the buffers before they are given back.
    for (auto & slot : slots)
    {
        if (!slot)
            continue;
        if (slot->in_flight)
        {
            try
            {
                finish(*slot);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
                /// Not given back: the device may still be writing to it.
                MemoryTrackerBlockerInThread blocker;
                slot.reset();
                continue;
            }
        }
        SlotPool::instance().release(std::move(slot));
    }
}

bool GPUCompressedReadBuffer::startBatch(Slot & slot)
{
    slot.batch.clear();
    slot.staged.clear();
    slot.device_blocks.clear();
    slot.device_packed.clear();
    slot.device_codec.reset();
    slot.device_compressed_bytes = 0;
    slot.device_blocks_expanded = 0;
    slot.full = false;

    constexpr UInt8 header_size = ICompressionCodec::getHeaderSize();
    size_t expanded_bytes = 0;

    while (expanded_bytes < max_batch_bytes)
    {
        /// The first block is where the file is, as a seek or the read in order left it; the blocks read ahead after it
        /// are only those of the ranges to be read.
        if (!slot.batch.empty() && !skipToRequestedRange())
            break;

        const size_t offset_in_file = file_in->getPosition();
        size_t decompressed_bytes = 0;
        size_t compressed_with_header = 0;
        if (readCompressedData(decompressed_bytes, compressed_with_header, /*always_copy=*/false) == 0)
            break;

        const auto method = static_cast<UInt8>(compressed_buffer[0]);
        const std::string_view payload(compressed_buffer + header_size, compressed_with_header - header_size);

        /// Every block of a codec the device expands goes to it. Those of the device come first, of one codec, as one call
        /// of nvcomp expands them.
        /// A plain `ZSTD` or `LZ4` block, one not after `BitPacking`, is for the CPU unless told otherwise.
        const auto codec_on_device = GPU::deviceCodecOf(method, payload);
        const bool plain = method != static_cast<UInt8>(CompressionMethodByte::Multiple);
        const bool codec_for_device = codec_on_device
            && (*codec_on_device == GPU::GPUCodec::GDEFLATE || !plain || (*codec_on_device == GPU::GPUCodec::ZSTD ? zstd_on_device : lz4_on_device));
        const bool on_device = codec_for_device && (!slot.device_codec || *slot.device_codec == *codec_on_device)
            && expanded_bytes == slot.device_blocks_expanded;
        if (on_device)
        {
            {
                MemoryTrackerBlockerInThread blocker;
                GPU::stageCompressedBlock(slot.staged, slot.device_blocks, slot.device_packed, method, payload, decompressed_bytes);
            }
            slot.device_codec = codec_on_device;
            slot.batch.push_back({.offset_in_file = offset_in_file, .offset_in_expanded = expanded_bytes, .decompressed_bytes = decompressed_bytes});
            expanded_bytes += decompressed_bytes;
            slot.device_blocks_expanded = expanded_bytes;
            slot.device_compressed_bytes += payload.size();
            continue;
        }

        /// A block for the CPU, after those of the device: the batch ends with it.
        {
            MemoryTrackerBlockerInThread blocker;
            slot.expanded.resize(expanded_bytes + decompressed_bytes + codec->getAdditionalSizeAtTheEndOfBuffer());
        }
        decompressTo(slot.expanded.data() + expanded_bytes, decompressed_bytes, compressed_with_header);
        slot.batch.push_back({.offset_in_file = offset_in_file, .offset_in_expanded = expanded_bytes, .decompressed_bytes = decompressed_bytes});
        expanded_bytes += decompressed_bytes;
        break;
    }
    slot.full = expanded_bytes >= max_batch_bytes;

    if (slot.device_blocks.empty())
        return !slot.batch.empty();

    if (slot.expanded.size() < slot.device_blocks_expanded)
    {
        MemoryTrackerBlockerInThread blocker;
        slot.expanded.resize(slot.device_blocks_expanded);
    }

    const size_t bytes = GPU::valueBytesOf(slot.device_blocks, slot.device_packed);
    if (bytes != slot.device_blocks_expanded)
        throw Exception(ErrorCodes::GPU_ERROR, "The blocks for the device expand to {} bytes, {} expected", bytes, slot.device_blocks_expanded);

    Stopwatch watch;
    const cuda::stream_ref on{slot.stream.get()};
    MemoryTrackerBlockerInThread blocker;
    GPU::waitOnFailure(on, [&]
    {
        /// The expanded bytes are allocated in the order of this stream, and the expansion waits for that alone.
        slot.device_expanded.clear();
        char * const destination = slot.device_expanded.grow(bytes);
        GPU::checkCuda(cudaEventRecord(slot.landed.get(), on.get()), "Cannot mark a point in a decompression stream");

        slot.device_compressed.clear();
        slot.device_compressed.append(GPU::bytesOf(slot.staged));

        const GPU::CompressedPiece piece{
            .device_compressed = slot.device_compressed.data(),
            .compressed_bytes = slot.staged.size(),
            .blocks = slot.device_blocks,
            .packed = slot.device_packed,
        };
        slot.decompression.launch(*slot.device_codec, std::span<const GPU::CompressedPiece>(&piece, 1), destination, slot.landed.get());
        GPU::checkCuda(
            cudaMemcpyAsync(slot.expanded.data(), destination, bytes, cudaMemcpyDeviceToHost, on.get()),
            "Cannot copy expanded bytes back from the device");
        GPU::checkCuda(cudaEventRecord(slot.landed.get(), on.get()), "Cannot mark a point in a decompression stream");
    });
    slot.in_flight = true;
    ProfileEvents::increment(ProfileEvents::GPUDecompressionBytes, slot.device_compressed_bytes);
    ProfileEvents::increment(ProfileEvents::GPUDecompressionMicroseconds, watch.elapsedMicroseconds());
    return true;
}

void GPUCompressedReadBuffer::finish(Slot & slot)
{
    if (!slot.in_flight)
        return;
    slot.in_flight = false;

    Stopwatch watch;
    slot.decompression.waitAndCheck();
    GPU::checkCuda(cudaEventSynchronize(slot.landed.get()), "Cannot wait for expanded bytes to come back from the device");
    ProfileEvents::increment(ProfileEvents::GPUDecompressionWaitMicroseconds, watch.elapsedMicroseconds());
}

bool GPUCompressedReadBuffer::advance()
{
    next_block = 0;
    if (ahead)
    {
        finish(*ahead);
        std::swap(current, ahead);
        ahead = nullptr;
    }
    else
    {
        if (!slots[0])
            slots[0] = SlotPool::instance().acquire();
        if (!current)
            current = slots[0].get();
        if (!startBatch(*current))
        {
            current->batch.clear();
            return false;
        }
        finish(*current);
    }

    /// The next batch, meanwhile, unless this one ended the read.
    if (current->full)
    {
        if (!slots[1])
            slots[1] = SlotPool::instance().acquire();
        Slot & other = current == slots[0].get() ? *slots[1] : *slots[0];
        if (startBatch(other))
            ahead = &other;
        else
            other.batch.clear();
    }
    return true;
}

bool GPUCompressedReadBuffer::nextImpl()
{
    try
    {
        if ((!current || next_block >= current->batch.size()) && !advance())
            return false;

        const Block & block = current->batch[next_block++];
        working_buffer = Buffer(
            current->expanded.data() + block.offset_in_expanded, current->expanded.data() + block.offset_in_expanded + block.decompressed_bytes);

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
    if (current)
    {
        for (size_t i = 0; i < current->batch.size(); ++i)
        {
            if (current->batch[i].offset_in_file != offset_in_compressed_file)
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
    }

    /// In the batch read ahead: it becomes the current one, and the one after it is read ahead.
    if (ahead)
    {
        for (size_t i = 0; i < ahead->batch.size(); ++i)
        {
            if (ahead->batch[i].offset_in_file != offset_in_compressed_file)
                continue;
            advance();
            next_block = i;
            resetWorkingBuffer();
            nextimpl_working_buffer_offset = offset_in_decompressed_block;
            return;
        }
        finish(*ahead);
        ahead->batch.clear();
        ahead = nullptr;
    }

    if (current)
        current->batch.clear();
    next_block = 0;
    file_in->seek(offset_in_compressed_file, SEEK_SET);
    resetWorkingBuffer();
    nextimpl_working_buffer_offset = offset_in_decompressed_block;
}

void GPUCompressedReadBuffer::setRequestMap(ByteRangeSet ranges)
{
    request_map = ranges;
    file_in->setRequestMap(std::move(ranges));
}

bool GPUCompressedReadBuffer::skipToRequestedRange()
{
    if (request_map.empty())
        return true;

    const auto position = static_cast<size_t>(file_in->getPosition());
    const auto & ranges = request_map.ranges();
    /// The first range that ends after the position.
    const auto range = std::upper_bound(
        ranges.begin(), ranges.end(), position, [](size_t offset, const ByteRange & r) { return offset < r.end(); });
    if (range == ranges.end())
        return false;
    if (range->offset > position)
        file_in->seek(range->offset, SEEK_SET);
    return true;
}

off_t GPUCompressedReadBuffer::getPosition() const
{
    return file_in->getPosition();
}

}

#endif
