#include <IO/ReaderExecutor.h>
#include <IO/PrefetchThreadPool.h>

#include "config.h"

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
    size_t min_bytes_for_seek_)
    : source(std::move(source_))
    , caches(std::move(caches_))
    , window_size(window_size_)
    , min_bytes_for_seek(min_bytes_for_seek_)
{
    offset_map.build(objects);
    LOG_DEBUG(log, "Created: {} objects, total_size={}, window_size={}, min_bytes_for_seek={}, {} caches",
        objects.size(), offset_map.totalSize(), window_size, min_bytes_for_seek, caches.size());
}

std::vector<Range> ReaderExecutor::mergeRanges(const std::vector<Range> & ranges, size_t min_gap)
{
    if (ranges.empty() || min_gap == 0)
        return ranges;

    std::vector<Range> sorted = ranges;
    std::sort(sorted.begin(), sorted.end(),
        [](const Range & a, const Range & b) { return a.offset < b.offset; });

    std::vector<Range> merged;
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

void ReaderExecutor::maybeTriggerPrefetch()
{
    if (!prefetch_pool || prefetch_valid)
        return;

    size_t logical_size = totalSize();
    if (position >= logical_size)
        return;

    size_t next_size = std::min(window_size, logical_size - position);
    Range next_window{position + data_start_offset, next_size};

    LOG_TRACE(log, "Prefetch: submitting [{}, {})", next_window.offset, next_window.end());

    prefetch_future = prefetch_pool->submit([this, next_window]()
    {
        return readWindow(next_window);
    });
    prefetch_range = next_window;
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
    Rope header_rope = readWindow(Range{0, data_start_offset});
    chassert(header_rope.totalBytes() == data_start_offset);

    /// Parse each header sequentially from the rope.
    size_t offset = 0;
    for (auto & layer : decryption_layers)
    {
        Rope one_header = header_rope.slice(Range{offset, FileEncryption::Header::kSize});
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

    /// Collect all bytes into a contiguous buffer for decryption.
    size_t total = rope.totalBytes();
    auto decrypted_buf = std::make_shared<OwnedRopeBuffer>(total);

    size_t pos = 0;
    for (const auto & node : rope.getNodes())
    {
        std::memcpy(decrypted_buf->data() + pos, node.data(), node.size);
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
        encryptor.decrypt(decrypted_buf->data(), total, decrypted_buf->data());
    }

    Rope result;
    result.append(RopeNode{std::move(decrypted_buf), 0, total, logical_offset});
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
        size_t win_size = std::min(window_size, logical_size - position);
        Range physical_window{position + data_start_offset, win_size};
        LOG_TRACE(log, "readNextWindow: synchronous read physical [{}, {}), logical [{}, {})",
            physical_window.offset, physical_window.end(), position, position + win_size);
        rope = readWindow(physical_window);
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

Rope ReaderExecutor::readWindow(Range window)
{
    LOG_TRACE(log, "readWindow [{}, {})", window.offset, window.end());

    Rope result;
    std::vector<Range> remaining = {window};

    /// Handles with misses — kept alive for put after source fetch.
    std::vector<std::unique_ptr<ICacheHandle>> miss_handles;

    /// Walk the cache chain
    for (auto & cache : caches)
    {
        std::vector<Range> still_missing;

        for (const auto & r : remaining)
        {
            auto handle = cache->lookup(CacheKey{}, r);
            auto status = handle->status();

            for (const auto & hit : status.hit_ranges)
            {
                LOG_TRACE(log, "readWindow: cache {} hit [{}, {})", cache->name(), hit.offset, hit.end());
                auto slice = handle->get(hit);
                for (const auto & node : slice.getNodes())
                    result.append(RopeNode{node.buffer, node.buffer_offset, node.size, node.logical_offset});
            }

            for (const auto & miss : status.miss_ranges)
            {
                LOG_TRACE(log, "readWindow: cache {} miss [{}, {})", cache->name(), miss.offset, miss.end());
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
        LOG_TRACE(log, "readWindow: merged {} miss ranges into {} fetch ranges (min_gap={})",
            remaining.size(), fetch_ranges.size(), min_bytes_for_seek);

    /// Fetch from source
    for (const auto & miss_range : fetch_ranges)
    {
        auto physical_ranges = offset_map.map(miss_range);
        size_t logical_pos = miss_range.offset;

        for (const auto & pr : physical_ranges)
        {
            LOG_TRACE(log, "readWindow: source read object={}, offset={}, size={}",
                pr.object.remote_path, pr.object_offset, pr.size);
            auto buf = std::make_shared<OwnedRopeBuffer>(pr.size);
            size_t bytes_read = source->read(pr.object, pr.object_offset, pr.size, buf->data());

            result.append(RopeNode{std::move(buf), 0, bytes_read, logical_pos});
            logical_pos += bytes_read;
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

    /// Trim to the originally requested window.
    return result.slice(window);
}

}
