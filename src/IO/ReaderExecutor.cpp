#include <IO/ReaderExecutor.h>
#include <IO/PrefetchThreadPool.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>

#include "config.h"

namespace ProfileEvents
{
    extern const Event LiveSourceBufferCreated;
    extern const Event LiveSourceBufferHits;
    extern const Event LiveSourceBufferFallbacks;
    extern const Event LiveSourceBufferBytes;
}

namespace DB::ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
}

#if USE_SSL
#include <IO/FileEncryptionCommon.h>
#include <IO/ReadBufferFromMemory.h>
#endif

#include <Common/logger_useful.h>
#include <algorithm>
#include <cstring>

namespace DB
{

ReaderExecutor::ReaderExecutor(
    std::shared_ptr<ISourceReader> source_,
    const StoredObjects & objects,
    std::vector<std::shared_ptr<ICacheProvider>> caches_,
    size_t window_size_,
    size_t min_bytes_for_seek_,
    CacheKey cache_key_)
    : source(std::move(source_))
    , caches(std::move(caches_))
    , cache_key(std::move(cache_key_))
    , window_size(window_size_)
    , min_bytes_for_seek(min_bytes_for_seek_)
{
    offset_map.build(objects);
    LOG_DEBUG(log, "Created: {} objects, total_size={}, window_size={}, min_bytes_for_seek={}, {} caches",
        objects.size(), offset_map.totalSize(), window_size, min_bytes_for_seek, caches.size());
}

ReaderExecutor::~ReaderExecutor()
{
    discardPrefetch();
}

std::vector<ByteRange> ReaderExecutor::mergeRanges(const std::vector<ByteRange> & ranges, size_t min_gap)
{
    if (ranges.empty() || min_gap == 0)
        return ranges;

    std::vector<ByteRange> sorted = ranges;
    std::sort(sorted.begin(), sorted.end(),
        [](const ByteRange & a, const ByteRange & b) { return a.offset < b.offset; });

    std::vector<ByteRange> merged;
    merged.push_back(sorted[0]);

    for (size_t i = 1; i < sorted.size(); ++i)
    {
        auto & prev = merged.back();
        /// Saturating subtraction: overlapping ranges (sorted[i].offset < prev.end())
        /// collapse to gap = 0 and merge via the same branch as adjacent ranges.
        size_t gap = sorted[i].offset > prev.end() ? sorted[i].offset - prev.end() : 0;

        if (gap <= min_gap)
        {
            size_t new_end = std::max(prev.end(), sorted[i].end());
            prev.size = new_end - prev.offset;
        }
        else
        {
            merged.push_back(sorted[i]);
        }
    }

    return merged;
}

void ReaderExecutor::setPrefetchPool(std::shared_ptr<PrefetchThreadPool> pool)
{
    prefetch_pool = std::move(pool);
}

void ReaderExecutor::setBufferLimit(std::shared_ptr<SourceBufferLimit> limit)
{
    buffer_limit = std::move(limit);
}

void ReaderExecutor::maybeTriggerPrefetch()
{
    if (!prefetch_pool || prefetch_valid)
        return;

    size_t logical_size = totalSize();
    if (position >= logical_size)
        return;

    size_t next_size = std::min(window_size, logical_size - position);
    ByteRange next_physical_window{position + data_start_offset, next_size};
    size_t next_logical_offset = position;

    LOG_TRACE(log, "Prefetch: submitting physical [{}, {})", next_physical_window.offset, next_physical_window.end());

    prefetch_future = prefetch_pool->submit([this, next_physical_window, next_logical_offset]()
    {
        return decryptRope(readPhysicalWindow(next_physical_window), next_logical_offset);
    });
    /// Track prefetch_range in logical coordinates — same space as `position`
    /// and as the decrypted rope returned by the future.
    prefetch_range = ByteRange{next_logical_offset, next_size};
    prefetch_valid = true;
}

void ReaderExecutor::discardPrefetch()
{
    if (prefetch_valid)
    {
        LOG_TRACE(log, "Prefetch: discarding [{}, {})", prefetch_range.offset, prefetch_range.end());
        if (prefetch_future.valid())
            std::ignore = prefetch_future.get();
        prefetch_valid = false;
    }
}

void ReaderExecutor::addDecryptionLayer(
    [[maybe_unused]] String path,
    [[maybe_unused]] size_t buffer_size,
    [[maybe_unused]] KeyFinderFunc key_finder)
{
#if USE_SSL
    decryption_layers.push_back(DecryptionLayer{
        .path = std::move(path),
        .buffer_size = buffer_size,
        .key_finder = std::move(key_finder),
        .key = {},
    });
    data_start_offset = decryption_layers.size() * FileEncryption::Header::kSize;
    LOG_DEBUG(log, "Added decryption layer, data_start_offset={}", data_start_offset);
#endif
}

void ReaderExecutor::initDecryption()
{
#if USE_SSL
    if (decryption_initialized || decryption_layers.empty())
        return;

    LOG_DEBUG(log, "initDecryption: reading {} headers ({} bytes)",
        decryption_layers.size(), data_start_offset);

    /// Read all headers in one call through the cache chain.
    Rope header_rope = readPhysicalWindow(ByteRange{0, data_start_offset});
    chassert(header_rope.totalBytes() == data_start_offset);

    /// Parse each header sequentially from the rope.
    size_t offset = 0;
    for (auto & layer : decryption_layers)
    {
        Rope one_header = header_rope.slice(ByteRange{offset, FileEncryption::Header::kSize});
        chassert(one_header.totalBytes() == FileEncryption::Header::kSize);

        /// Header is 64 bytes — fits in a single node after slice.
        const auto & nodes = one_header.getNodes();
        chassert(nodes.size() == 1);
        ReadBufferFromMemory rb(nodes[0].data(), nodes[0].size);

        FileEncryption::Header header;
        header.read(rb);
        layer.key = layer.key_finder(header.key_fingerprint, layer.path);
        decryption_headers.push_back(std::move(header));
        offset += FileEncryption::Header::kSize;

        LOG_DEBUG(log, "initDecryption: parsed header for {}, algorithm={}",
            layer.path, static_cast<int>(decryption_headers.back().algorithm));
    }

    decryption_initialized = true;
#endif
}

Rope ReaderExecutor::decryptRope(Rope rope, [[maybe_unused]] size_t logical_offset)
{
#if USE_SSL
    if (decryption_layers.empty())
        return rope;

    size_t total = rope.totalBytes();
    if (total == 0)
        return {};

    /// Allocate destination blocks (≤ ROPE_BLOCK_SIZE each) and copy the
    /// (still-encrypted) input rope into them. Input nodes may reference
    /// shared memory (e.g. page cache cells), so in-place decryption is not
    /// safe in general — but the destination blocks are private, so decrypting
    /// them in place is fine.
    auto blocks = allocateBlocks(total);

    {
        size_t out_idx = 0;
        size_t out_pos = 0;
        for (const auto & in_node : rope.getNodes())
        {
            size_t in_pos = 0;
            while (in_pos < in_node.size)
            {
                auto & out_block = *blocks[out_idx];
                size_t space = out_block.size() - out_pos;
                size_t chunk = std::min(in_node.size - in_pos, space);
                std::memcpy(out_block.data() + out_pos, in_node.data() + in_pos, chunk);
                in_pos += chunk;
                out_pos += chunk;
                if (out_pos == out_block.size())
                {
                    ++out_idx;
                    out_pos = 0;
                }
            }
        }
    }

    /// Apply each decryption layer to each block. CTR mode is fully position-
    /// addressable, so setOffset(logical_offset + block_start) gives the same
    /// keystream as decrypting the whole range in one call.
    for (size_t i = 0; i < decryption_layers.size(); ++i)
    {
        FileEncryption::Encryptor encryptor(
            decryption_headers[i].algorithm,
            decryption_layers[i].key,
            decryption_headers[i].init_vector);

        size_t block_start = 0;
        for (auto & block : blocks)
        {
            encryptor.setOffset(logical_offset + block_start);
            encryptor.decrypt(block->data(), block->size(), block->data());
            block_start += block->size();
        }
    }

    Rope result;
    size_t pos = 0;
    for (auto & block : blocks)
    {
        size_t sz = block->size();
        result.append(RopeNode{block, 0, sz, logical_offset + pos});
        pos += sz;
    }
    return result;
#else
    return rope;
#endif
}

Rope ReaderExecutor::readNextWindow()
{
    size_t logical_size = totalSize();
    if (position >= logical_size)
    {
        LOG_TRACE(log, "readNextWindow: EOF at position {}", position);
        return {};
    }

    Rope rope;

    if (prefetch_valid && prefetch_future.valid())
    {
        LOG_TRACE(log, "readNextWindow: using prefetched [{}, {})", prefetch_range.offset, prefetch_range.end());
        rope = prefetch_future.get();
        prefetch_valid = false;

        /// If seek landed us in the middle of the prefetched window, drop the
        /// prefix bytes that come before our current logical position so
        /// rope.range().offset matches `position`.
        if (!rope.empty() && position > rope.range().offset)
        {
            size_t end = rope.range().end();
            rope = rope.slice(ByteRange{position, end - position});
        }
    }
    else
    {
        size_t win_size = std::min(window_size, logical_size - position);
        ByteRange physical_window{position + data_start_offset, win_size};
        LOG_TRACE(log, "readNextWindow: synchronous read physical [{}, {}), logical [{}, {})",
            physical_window.offset, physical_window.end(), position, position + win_size);
        rope = decryptRope(readPhysicalWindow(physical_window), position);
    }

    position += rope.range().size;
    LOG_TRACE(log, "readNextWindow: got {} bytes, {} nodes, position advanced to {}",
        rope.range().size, rope.getNodes().size(), position);

    maybeTriggerPrefetch();

    return rope;
}

void ReaderExecutor::seek(size_t new_position)
{
    LOG_DEBUG(log, "seek to {}, current position={}", new_position, position);

    if (prefetch_valid
        && new_position >= prefetch_range.offset
        && new_position < prefetch_range.end())
    {
        LOG_TRACE(log, "seek: target within prefetch [{}, {}), keeping prefetch",
            prefetch_range.offset, prefetch_range.end());
        position = new_position;
        return;
    }

    discardPrefetch();
    position = new_position;
}

std::vector<std::shared_ptr<OwnedRopeBuffer>> ReaderExecutor::allocateBlocks(size_t size)
{
    std::vector<std::shared_ptr<OwnedRopeBuffer>> blocks;
    blocks.reserve((size + ROPE_BLOCK_SIZE - 1) / ROPE_BLOCK_SIZE);
    size_t remaining = size;
    while (remaining > 0)
    {
        size_t chunk = std::min(ROPE_BLOCK_SIZE, remaining);
        blocks.push_back(std::make_shared<OwnedRopeBuffer>(chunk));
        remaining -= chunk;
    }
    return blocks;
}

Rope ReaderExecutor::readFromLiveBufferIntoRope(
    std::vector<std::shared_ptr<OwnedRopeBuffer>> blocks, size_t logical_offset)
{
    chassert(live_buffer);
    auto & buf = *live_buffer->buffer;

    Rope rope;
    size_t total_read = 0;

    for (auto & block : blocks)
    {
        size_t chunk = block->size();

        /// set() + next(): data goes directly from network into block memory.
        buf.set(block->data(), chunk);
        if (!buf.next())
            break;

        size_t got = buf.available();
        buf.position() = buf.buffer().end();

        LOG_DEBUG(log, "readFromLiveBufferIntoRope: block {}, chunk={}, got={}, first_byte=0x{:02x}",
            rope.getNodes().size(), chunk, got,
            got > 0 ? static_cast<unsigned char>(block->data()[0]) : 0);

        if (got == 0)
            break;

        rope.append(RopeNode{block, 0, got, logical_offset + total_read});
        total_read += got;
    }

    /// Blocks not referenced from rope (file ended before consuming all of them)
    /// drop their refcount to 0 when `blocks` goes out of scope here.
    return rope;
}

Rope ReaderExecutor::readFromSource(
    const StoredObject & object, size_t offset,
    std::vector<std::shared_ptr<OwnedRopeBuffer>> blocks, size_t logical_offset)
{
    /// Try live buffer: reuse open connection for sequential reads.
    if (live_buffer
        && live_buffer->object_path == object.remote_path
        && live_buffer->current_position == offset)
    {
        LOG_TRACE(log, "readFromSource: live buffer hit for {}, position={}", object.remote_path, offset);
        ProfileEvents::increment(ProfileEvents::LiveSourceBufferHits);

        Rope rope = readFromLiveBufferIntoRope(std::move(blocks), logical_offset);
        size_t total_read = rope.totalBytes();

        ProfileEvents::increment(ProfileEvents::LiveSourceBufferBytes, total_read);
        live_buffer->current_position += total_read;
        live_buffer->slot.updatePosition(live_buffer->current_position);
        return rope;
    }

    /// Live buffer doesn't match — close it if present.
    if (live_buffer)
    {
        LOG_TRACE(log, "readFromSource: closing live buffer for {} (was at {}), need {}:{}",
            live_buffer->object_path, live_buffer->current_position, object.remote_path, offset);
        live_buffer.reset();
    }

    /// Try to acquire a slot for a new live buffer.
    if (buffer_limit)
    {
        auto slot = buffer_limit->tryAcquire(buffer_limit, object.remote_path, String(CurrentThread::getQueryId()));
        if (slot)
        {
            auto opened = source->open(object, /*use_external_buffer=*/true);

            if (opened)
            {
                if (offset > 0)
                    opened->seek(offset, SEEK_SET);

                live_buffer.emplace(LiveBuffer{
                    .object_path = object.remote_path,
                    .current_position = offset,
                    .buffer = std::move(opened),
                    .slot = std::move(*slot),
                });

                Rope rope = readFromLiveBufferIntoRope(std::move(blocks), logical_offset);
                size_t total_read = rope.totalBytes();

                live_buffer->current_position += total_read;
                live_buffer->slot.updatePosition(live_buffer->current_position);

                ProfileEvents::increment(ProfileEvents::LiveSourceBufferCreated);
                ProfileEvents::increment(ProfileEvents::LiveSourceBufferBytes, total_read);
                LOG_TRACE(log, "readFromSource: opened live buffer for {}, read {} bytes, position={}",
                    object.remote_path, total_read, live_buffer->current_position);
                return rope;
            }
        }
    }

    /// Fallback: open a fresh connection without storing it as live_buffer
    /// (slot was unavailable). Read into the pre-allocated blocks via set()+next(),
    /// same pattern as the live-buffer path. The opened buffer is dropped when
    /// this function returns.
    ProfileEvents::increment(ProfileEvents::LiveSourceBufferFallbacks);

    auto opened = source->open(object, /*use_external_buffer=*/true);
    if (offset > 0)
        opened->seek(offset, SEEK_SET);
    auto & buf = *opened;

    Rope rope;
    size_t total_read = 0;

    for (auto & block : blocks)
    {
        size_t chunk = block->size();
        buf.set(block->data(), chunk);
        if (!buf.next())
            break;

        size_t got = buf.available();
        buf.position() = buf.buffer().end();

        LOG_DEBUG(log, "readFromSource: stateless block offset={}, chunk={}, got={}, first_byte=0x{:02x}",
            offset + total_read, chunk, got,
            got > 0 ? static_cast<unsigned char>(block->data()[0]) : 0);

        if (got == 0)
            break;

        rope.append(RopeNode{block, 0, got, logical_offset + total_read});
        total_read += got;
    }

    /// Blocks not referenced from rope drop their refcount to 0 when this function returns.
    return rope;
}

Rope ReaderExecutor::readPhysicalWindow(ByteRange physical_window)
{
    LOG_TRACE(log, "readPhysicalWindow [{}, {})", physical_window.offset, physical_window.end());

    Rope result;
    std::vector<ByteRange> remaining = {physical_window};

    /// Handles with misses — kept alive for put after source fetch.
    std::vector<std::unique_ptr<ICacheHandle>> miss_handles;

    /// Walk the cache chain
    for (auto & cache : caches)
    {
        std::vector<ByteRange> still_missing;

        for (const auto & r : remaining)
        {
            auto handle = cache->lookup(cache_key, r);
            auto status = handle->status();

            for (const auto & hit : status.hit_ranges)
            {
                LOG_TRACE(log, "readPhysicalWindow: cache {} hit [{}, {})", cache->name(), hit.offset, hit.end());
                auto slice = handle->get(hit);
                for (const auto & node : slice.getNodes())
                    result.append(RopeNode{node.buffer, node.buffer_offset, node.size, node.logical_offset});
            }

            for (const auto & miss : status.miss_ranges)
            {
                LOG_TRACE(log, "readPhysicalWindow: cache {} miss [{}, {})", cache->name(), miss.offset, miss.end());
                still_missing.push_back(miss);
            }

            if (!status.miss_ranges.empty())
                miss_handles.push_back(std::move(handle));
        }

        remaining = std::move(still_missing);
        if (remaining.empty())
            break;
    }

    /// Merge close-together ranges to reduce source request count.
    auto fetch_ranges = mergeRanges(remaining, min_bytes_for_seek);

    if (fetch_ranges.size() < remaining.size())
        LOG_TRACE(log, "readPhysicalWindow: merged {} miss ranges into {} fetch ranges (min_gap={})",
            remaining.size(), fetch_ranges.size(), min_bytes_for_seek);

    /// If merging extended a miss range across a cached hit, the source will
    /// fetch the whole merged range — including the bytes already in `result`
    /// from the cache hit. Drop those cached nodes so we don't end up with
    /// duplicate coverage for the same logical offsets.
    if (fetch_ranges.size() < remaining.size())
    {
        auto & nodes = result.getNodes();
        auto inside_fetch_range = [&fetch_ranges](const RopeNode & node)
        {
            for (const auto & fr : fetch_ranges)
                if (node.logical_offset >= fr.offset
                    && node.logical_offset + node.size <= fr.end())
                    return true;
            return false;
        };
        nodes.erase(std::remove_if(nodes.begin(), nodes.end(), inside_fetch_range), nodes.end());
    }

    /// Fetch from source — allocate destination blocks (≤ ROPE_BLOCK_SIZE each) before
    /// opening connections, then try live buffer for sequential reads, fall back to stateless.
    for (const auto & miss_range : fetch_ranges)
    {
        auto physical_ranges = offset_map.map(miss_range);
        size_t logical_pos = miss_range.offset;

        for (const auto & pr : physical_ranges)
        {
            LOG_TRACE(log, "readPhysicalWindow: source read object={}, offset={}, size={}",
                pr.object.remote_path, pr.object_offset, pr.size);

            auto blocks = allocateBlocks(pr.size);
            Rope source_rope = readFromSource(pr.object, pr.object_offset, std::move(blocks), logical_pos);
            size_t actual = source_rope.totalBytes();
            /// offset_map's pr.size is authoritative — any short read indicates the
            /// source can't deliver what offset_map (and therefore StoredObject.bytes_size)
            /// promised. Failing fast is the only correct choice: a short read on a
            /// non-terminal range shifts subsequent ranges' logical placement, and on a
            /// terminal range, returning an empty rope from the next readNextWindow would
            /// produce an infinite loop because position can't advance past the missing
            /// bytes.
            if (actual != pr.size)
                throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                    "ReaderExecutor: short read from {} at offset {}: requested {} bytes, got {}",
                    pr.object.remote_path, pr.object_offset, pr.size, actual);
            logical_pos += pr.size;
            result.append(std::move(source_rope));
        }
    }

    /// Fill caches with fetched data using the saved handles.
    for (auto & handle : miss_handles)
    {
        auto status = handle->status();
        for (const auto & miss : status.miss_ranges)
        {
            auto slice = result.slice(miss);
            if (!slice.empty())
                handle->put(miss, std::move(slice));
        }
    }

    /// Sort nodes by logical offset
    auto & nodes = result.getNodes();
    std::sort(nodes.begin(), nodes.end(),
        [](const RopeNode & a, const RopeNode & b)
        { return a.logical_offset < b.logical_offset; });

    /// Trim to the originally requested physical window.
    return result.slice(physical_window);
}

}
