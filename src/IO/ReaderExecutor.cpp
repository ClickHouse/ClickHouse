#include <IO/ReaderExecutor.h>
#include <IO/PrefetchThreadPool.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>

#include "config.h"

namespace ProfileEvents
{
    extern const Event LiveSourceBufferCreated;
    extern const Event LiveSourceBufferHits;
    extern const Event LiveSourceBufferFallbacks;
    extern const Event LiveSourceBufferBytes;
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
        size_t gap = sorted[i].offset - prev.end();

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

    size_t effective_window = live_buffer ? ROPE_BLOCK_SIZE : window_size;
    size_t next_size = std::min(effective_window, logical_size - position);
    ByteRange next_physical_window{position + data_start_offset, next_size};

    LOG_TRACE(log, "Prefetch: submitting physical [{}, {})", next_physical_window.offset, next_physical_window.end());

    prefetch_future = prefetch_pool->submit([this, next_physical_window]()
    {
        return readPhysicalWindow(next_physical_window);
    });
    prefetch_range = next_physical_window;
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

#if USE_SSL
void ReaderExecutor::addDecryptionLayer(String path, size_t buffer_size, KeyFinderFunc key_finder)
{
    decryption_layers.push_back(DecryptionLayer{
        .path = std::move(path),
        .buffer_size = buffer_size,
        .key_finder = std::move(key_finder),
        .key = {},
    });
    data_start_offset = decryption_layers.size() * FileEncryption::Header::kSize;
    LOG_DEBUG(log, "Added decryption layer, data_start_offset={}", data_start_offset);
}

void ReaderExecutor::initDecryption()
{
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
}

Rope ReaderExecutor::decryptRope(Rope rope, size_t logical_offset)
{
    if (decryption_layers.empty())
        return rope;

    /// Copy into an owned buffer before decrypting.
    /// Rope nodes may reference shared memory (e.g. page cache cells),
    /// so in-place decryption is not safe in general.
    /// TODO: optimize — decrypt in-place for OwnedRopeBuffer nodes.
    size_t total = rope.totalBytes();
    auto buf = std::make_shared<OwnedRopeBuffer>(total);

    size_t pos = 0;
    for (const auto & node : rope.getNodes())
    {
        std::memcpy(buf->data() + pos, node.data(), node.size);
        pos += node.size;
    }

    /// Apply each decryption layer.
    for (size_t i = 0; i < decryption_layers.size(); ++i)
    {
        FileEncryption::Encryptor encryptor(
            decryption_headers[i].algorithm,
            decryption_layers[i].key,
            decryption_headers[i].init_vector);
        encryptor.setOffset(logical_offset);
        encryptor.decrypt(buf->data(), total, buf->data());
    }

    Rope result;
    result.append(RopeNode{std::move(buf), 0, total, logical_offset});
    return result;
}
#endif

Rope ReaderExecutor::readNextWindow()
{
#if USE_SSL
    /// Initialize decryption headers on first call.
    initDecryption();
#endif

    size_t logical_size = totalSize();
    if (position >= logical_size)
    {
        LOG_TRACE(log, "readNextWindow: EOF at position {}", position);
        return {};
    }

    Rope rope;
    [[maybe_unused]] size_t logical_pos = position;

    if (prefetch_valid && prefetch_future.valid())
    {
        LOG_TRACE(log, "readNextWindow: using prefetched [{}, {})", prefetch_range.offset, prefetch_range.end());
        rope = prefetch_future.get();
        prefetch_valid = false;
    }
    else
    {
        /// Use small window when a live connection is active — data streams
        /// continuously, no first-byte latency per window. Large window only
        /// for stateless remote reads where each window = one HTTP request.
        size_t effective_window = live_buffer ? ROPE_BLOCK_SIZE : window_size;
        size_t win_size = std::min(effective_window, logical_size - position);
        ByteRange physical_window{position + data_start_offset, win_size};
        LOG_TRACE(log, "readNextWindow: synchronous read physical [{}, {}), logical [{}, {})",
            physical_window.offset, physical_window.end(), position, position + win_size);
        rope = readPhysicalWindow(physical_window);
    }

    position += rope.range().size;
    LOG_TRACE(log, "readNextWindow: got {} bytes, {} nodes, position advanced to {}",
        rope.range().size, rope.getNodes().size(), position);

    maybeTriggerPrefetch();

#if USE_SSL
    /// Decrypt if needed.
    return decryptRope(std::move(rope), logical_pos);
#else
    return rope;
#endif
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
        position = prefetch_range.offset;
        return;
    }

    discardPrefetch();
    position = new_position;
}

Rope ReaderExecutor::allocateRope(size_t total_size, size_t logical_offset)
{
    Rope rope;
    size_t allocated = 0;
    while (allocated < total_size)
    {
        size_t chunk = std::min(ROPE_BLOCK_SIZE, total_size - allocated);
        auto buf = std::make_shared<OwnedRopeBuffer>(chunk);
        rope.append(RopeNode{std::move(buf), 0, chunk, logical_offset + allocated});
        allocated += chunk;
    }
    return rope;
}

/// Fill pre-allocated Rope nodes from a stream (live buffer or freshly opened).
/// Returns actual bytes read. Nodes may be partially filled at EOF.
static size_t fillRopeFromStream(ReadBufferFromFileBase & stream, Rope & rope, bool use_external_buffer)
{
    size_t total_read = 0;
    size_t filled_nodes = 0;
    for (auto & node : rope.getNodes())
    {
        size_t node_read = 0;
        if (use_external_buffer)
        {
            /// Zero-copy: point the stream at our Rope block memory.
            stream.set(node.data(), node.size);
            if (!stream.next())
                break;
            node_read = stream.available();
            stream.position() = stream.buffer().end();
        }
        else
        {
            /// Copy path: read from stream's internal buffer into our block.
            while (node_read < node.size)
            {
                size_t bytes = stream.read(node.data() + node_read, node.size - node_read);
                if (bytes == 0)
                    break;
                node_read += bytes;
            }
            if (node_read == 0)
                break;
        }
        node.size = node_read;
        total_read += node_read;
        ++filled_nodes;
    }
    /// Remove unfilled nodes — they contain uninitialized memory.
    rope.getNodes().resize(filled_nodes);
    return total_read;
}

/// Fill pre-allocated Rope nodes from stateless source->read calls.
static size_t fillRopeFromStateless(ISourceReader & source, const StoredObject & object, size_t offset, Rope & rope)
{
    size_t total_read = 0;
    size_t filled_nodes = 0;
    for (auto & node : rope.getNodes())
    {
        size_t got = source.read(object, offset + total_read, node.size, node.data());
        if (got == 0)
            break;
        node.size = got;
        total_read += got;
        ++filled_nodes;
    }
    rope.getNodes().resize(filled_nodes);
    return total_read;
}

size_t ReaderExecutor::readFromSource(const StoredObject & object, size_t offset, Rope & rope)
{
    /// Try live buffer: reuse open connection for sequential reads.
    if (live_buffer
        && live_buffer->object_path == object.remote_path
        && live_buffer->current_position == offset)
    {
        LOG_TRACE(log, "readFromSource: live buffer hit for {}, position={}", object.remote_path, offset);
        ProfileEvents::increment(ProfileEvents::LiveSourceBufferHits);

        size_t total_read = fillRopeFromStream(*live_buffer->buffer, rope, live_buffer->use_external_buffer);

        ProfileEvents::increment(ProfileEvents::LiveSourceBufferBytes, total_read);
        live_buffer->current_position += total_read;
        live_buffer->slot.updatePosition(live_buffer->current_position);
        return total_read;
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
            auto opened = source->open(object, /*use_external_buffer=*/false);

            if (opened)
            {
                if (offset > 0)
                    opened->seek(offset, SEEK_SET);

                live_buffer.emplace(LiveBuffer{
                    .object_path = object.remote_path,
                    .current_position = offset,
                    .use_external_buffer = false,
                    .buffer = std::move(opened),
                    .slot = std::move(*slot),
                });

                size_t total_read = fillRopeFromStream(*live_buffer->buffer, rope, true);

                live_buffer->current_position += total_read;
                live_buffer->slot.updatePosition(live_buffer->current_position);

                ProfileEvents::increment(ProfileEvents::LiveSourceBufferCreated);
                ProfileEvents::increment(ProfileEvents::LiveSourceBufferBytes, total_read);
                LOG_TRACE(log, "readFromSource: opened live buffer for {}, read {} bytes, position={}",
                    object.remote_path, total_read, live_buffer->current_position);
                return total_read;
            }
        }
    }

    /// Fallback: stateless read, fill blocks one by one.
    ProfileEvents::increment(ProfileEvents::LiveSourceBufferFallbacks);
    return fillRopeFromStateless(*source, object, offset, rope);
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

    /// Fetch from source — try live buffer for sequential reads, fall back to stateless.
    for (const auto & miss_range : fetch_ranges)
    {
        auto physical_ranges = offset_map.map(miss_range);
        size_t logical_pos = miss_range.offset;

        for (const auto & pr : physical_ranges)
        {
            LOG_TRACE(log, "readPhysicalWindow: source read object={}, offset={}, size={}",
                pr.object.remote_path, pr.object_offset, pr.size);

            Rope source_rope = allocateRope(pr.size, logical_pos);
            readFromSource(pr.object, pr.object_offset, source_rope);

            for (const auto & node : source_rope.getNodes())
                logical_pos += node.size;
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
    auto & nodes = const_cast<std::vector<RopeNode> &>(result.getNodes());
    std::sort(nodes.begin(), nodes.end(),
        [](const RopeNode & a, const RopeNode & b)
        { return a.logical_offset < b.logical_offset; });

    /// Trim to the originally requested physical window.
    return result.slice(physical_window);
}

}
