#include <IO/CachedInMemoryReadBufferFromFile.h>
#include <base/scope_guard.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event PageCacheReadBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_END_OF_FILE;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int LOGICAL_ERROR;
}

CachedInMemoryReadBufferFromFile::CachedInMemoryReadBufferFromFile(
    PageCacheFile cache_file_, PageCachePtr cache_, std::unique_ptr<ReadBufferFromFileBase> in_, const ReadSettings & settings_)
    : ReadBufferFromFileBase(0, nullptr, 0, in_->getFileSize())
    , cache_file(std::move(cache_file_))
    , cache_key_base_hash(cache_file.baseHash())
    , cache(cache_)
    , settings(settings_)
    , in(std::move(in_)), read_until_position(file_size.value())
    , inner_read_until_position(read_until_position)
{
}

bool CachedInMemoryReadBufferFromFile::innerSupportsReadAt() const
{
    std::call_once(inner_supports_read_at_init, [this]()
    {
        inner_supports_read_at = in->supportsReadAt();
    });
    return inner_supports_read_at;
}

String CachedInMemoryReadBufferFromFile::getFileName() const
{
    return cache_file.path;
}

String CachedInMemoryReadBufferFromFile::getInfoForLog()
{
    return "CachedInMemoryReadBufferFromFile(" + in->getInfoForLog() + ")";
}

bool CachedInMemoryReadBufferFromFile::isSeekCheap()
{
    /// Seek is cheap in the sense that seek()+nextImpl() is never much slower than ignore()+nextImpl()
    /// (which is what the caller cares about).
    return true;
}

off_t CachedInMemoryReadBufferFromFile::seek(off_t off, int whence)
{
    if (whence != SEEK_SET)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET mode is allowed.");

    size_t offset = static_cast<size_t>(off);
    if (offset > file_size.value())
        throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bounds. Offset: {}", off);

    if (offset >= file_offset_of_buffer_end - working_buffer.size() && offset <= file_offset_of_buffer_end)
    {
        pos = working_buffer.end() - (file_offset_of_buffer_end - offset);
        chassert(getPosition() == off);
        return off;
    }

    resetWorkingBuffer();

    file_offset_of_buffer_end = offset;
    chunk.reset();

    chassert(getPosition() == off);
    return off;
}

off_t CachedInMemoryReadBufferFromFile::getPosition()
{
    return file_offset_of_buffer_end - available();
}

size_t CachedInMemoryReadBufferFromFile::getFileOffsetOfBufferEnd() const
{
    return file_offset_of_buffer_end;
}

void CachedInMemoryReadBufferFromFile::setReadUntilPosition(size_t position)
{
    read_until_position = std::min(position, file_size.value());
    if (position < static_cast<size_t>(getPosition()))
    {
        resetWorkingBuffer();
        chunk.reset();
    }
    else if (position < file_offset_of_buffer_end)
    {
        size_t diff = file_offset_of_buffer_end - position;
        working_buffer.resize(working_buffer.size() - diff);
        file_offset_of_buffer_end -= diff;
    }
}

void CachedInMemoryReadBufferFromFile::setReadUntilEnd()
{
    setReadUntilPosition(file_size.value());
}

bool CachedInMemoryReadBufferFromFile::nextImpl()
{
    chassert(read_until_position <= file_size.value());
    if (file_offset_of_buffer_end >= read_until_position)
        return false;

    size_t block_size = settings.page_cache_block_size;

    if (chunk != nullptr)
    {
        chassert(chunk->range.hash(cache_key_base_hash) == cache_range.hash(cache_key_base_hash));
        if (file_offset_of_buffer_end < cache_range.offset || file_offset_of_buffer_end >= cache_range.offset + block_size)
            chunk.reset();
    }

    if (chunk == nullptr)
    {
        cache_range.offset = file_offset_of_buffer_end / block_size * block_size;
        cache_range.size = std::min(block_size, file_size.value() - cache_range.offset);

        chunk = cache->getOrSet(cache_file, cache_range, settings.read_from_page_cache_if_exists_otherwise_bypass_cache, settings.page_cache_inject_eviction, [&](auto cell)
        {
            Buffer prev_in_buffer = in->internalBuffer();
            SCOPE_EXIT({ in->set(prev_in_buffer.begin(), prev_in_buffer.size()); });

            size_t pos = 0;
            while (pos < cache_range.size)
            {
                char * piece_start = cell->data() + pos;
                size_t piece_size = cache_range.size - pos;
                in->set(piece_start, piece_size);
                if (pos == 0)
                {
                    /// Do in->setReadUntilPosition if needed.
                    /// If the next few blocks are likely cache misses, include them too, to reduce
                    /// the number of requests (usually `in` makes a new HTTP request after each
                    /// nontrivial seek or setReadUntilPosition call).
                    /// Use aligned groups of blocks (rather than sliding window) to work better
                    /// with distributed cache.
                    size_t lookahead_bytes = block_size * std::max<size_t>(1, settings.page_cache_lookahead_blocks);
                    size_t lookahead_block_end = std::min({
                        file_size.value(),
                        (cache_range.offset / lookahead_bytes + 1) * lookahead_bytes,
                        (read_until_position + block_size - 1) / block_size * block_size});

                    if (inner_read_until_position < cache_range.offset + cache_range.size ||
                        inner_read_until_position > lookahead_block_end)
                    {
                        PageCacheByteRange probe = cache_range;
                        do
                        {
                            probe.offset += probe.size;
                            probe.size = std::min(block_size, file_size.value() - probe.offset);
                            chassert(probe.offset <= lookahead_block_end);
                        }
                        while (probe.offset < lookahead_block_end
                            && !cache->contains(
                                probe.hash(cache_key_base_hash),
                                settings.page_cache_inject_eviction));
                        inner_read_until_position = probe.offset;
                        in->setReadUntilPosition(inner_read_until_position);
                    }

                    in->seek(cache_range.offset, SEEK_SET);
                }
                else
                    chassert(!in->available());

                if (in->eof())
                    throw Exception(ErrorCodes::UNEXPECTED_END_OF_FILE, "File {} ended after {} bytes, but we expected {}",
                        getFileName(), cache_range.offset + pos, file_size.value());

                chassert(in->position() >= piece_start && in->buffer().end() <= piece_start + piece_size);
                chassert(in->getPosition() == static_cast<off_t>(cache_range.offset + pos));

                size_t n = in->available();
                chassert(n);
                if (in->position() != piece_start)
                    memmove(piece_start, in->position(), n);
                in->position() += n;
                pos += n;
            }

            return cell;
        });
    }

    nextimpl_working_buffer_offset = file_offset_of_buffer_end - cache_range.offset;
    working_buffer = Buffer(
        chunk->data(),
        chunk->data() + std::min(chunk->size(), read_until_position - cache_range.offset));
    pos = working_buffer.begin() + nextimpl_working_buffer_offset;

    if (!internal_buffer.empty())
    {
        /// We were given an external buffer to read into. We currently don't allow this as it would
        /// require unnecessary memcpy.
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CachedInMemoryReadBufferFromFile doesn't support using external buffer");
    }

    size_t size = available();
    file_offset_of_buffer_end += size;
    ProfileEvents::increment(ProfileEvents::PageCacheReadBytes, size);

    return true;
}

std::vector<PageCache::MappedPtr> CachedInMemoryReadBufferFromFile::populateBlockRange(size_t offset, size_t n, const std::function<bool(PageCache::MappedPtr &)> & block_callback) const
{
    if (n == 0 || offset >= file_size.value())
        return {};

    size_t block_size = settings.page_cache_block_size;
    /// Compute end_offset without overflow: clamp n so that offset + n <= file_size.
    size_t end_offset = offset + std::min(n, file_size.value() - offset);

    size_t first_block_start = offset / block_size * block_size;
    size_t num_blocks = (end_offset - first_block_start + block_size - 1) / block_size;

    bool detached_if_missing = settings.read_from_page_cache_if_exists_otherwise_bypass_cache;
    bool inject_eviction = settings.page_cache_inject_eviction;

    /// Phase 1: probe cache for all blocks, record hits.
    std::vector<PageCache::MappedPtr> cells(num_blocks);
    PageCacheByteRange block_range;
    for (size_t i = 0; i < num_blocks; ++i)
    {
        block_range.offset = first_block_start + i * block_size;
        block_range.size = std::min(block_size, file_size.value() - block_range.offset);
        cells[i] = cache->get(block_range.hash(cache_key_base_hash), inject_eviction);
    }

    /// Phase 2: fill each missing block by reading directly into its cache cell,
    /// avoiding a large temporary buffer and the extra memcpy.
    for (size_t i = 0; i < num_blocks; ++i)
    {
        if (!cells[i])
        {
            block_range.offset = first_block_start + i * block_size;
            block_range.size = std::min(block_size, file_size.value() - block_range.offset);
            UInt128 key_hash = block_range.hash(cache_key_base_hash);

            cells[i] = cache->getOrSet(
                cache_file, block_range, detached_if_missing, inject_eviction,
                [&](const auto & c)
                {
                    size_t bytes_read = in->readBigAt(c->data(), block_range.size, block_range.offset, nullptr);
                    if (bytes_read < block_range.size)
                        throw Exception(ErrorCodes::UNEXPECTED_END_OF_FILE, "File {} ended after {} bytes, but we expected {}",
                            cache_file.path, block_range.offset + bytes_read, file_size.value());
                },
                key_hash);
        }
        if (block_callback && block_callback(cells[i]))
            break;
    }

    return cells;
}

size_t CachedInMemoryReadBufferFromFile::readBigAt(char * to, size_t n, size_t offset, const std::function<bool(size_t m)> & progress_callback) const
{
    if (n == 0 || offset >= file_size.value())
        return 0;

    size_t end_offset = offset + std::min(n, file_size.value() - offset);

    size_t bytes_copied = 0;
    auto cells = populateBlockRange(
        offset, n,
        [&](PageCache::MappedPtr & cell)
        {
            size_t block_start = cell->range.offset;
            size_t block_data_size = cell->range.size;
            size_t offset_in_block = (offset > block_start) ? offset - block_start : 0;
            size_t to_copy = std::min(block_data_size - offset_in_block, end_offset - (offset + bytes_copied));

            memcpy(to + bytes_copied, cell->data() + offset_in_block, to_copy);
            bytes_copied += to_copy;

            ProfileEvents::increment(ProfileEvents::PageCacheReadBytes, to_copy);

            if (progress_callback)
                return progress_callback(bytes_copied);
            return false;
        });

    return bytes_copied;
}

std::vector<SeekableReadBuffer::CachedRegion> CachedInMemoryReadBufferFromFile::readBigAtRetainCells(size_t n, size_t offset) const
{
    if (n == 0 || offset >= file_size.value())
        return {};

    size_t block_size = settings.page_cache_block_size;
    size_t end_offset = offset + std::min(n, file_size.value() - offset);
    size_t first_block_start = offset / block_size * block_size;

    auto cells = populateBlockRange(offset, n);

    std::vector<CachedRegion> regions;
    size_t current_offset = offset;
    for (size_t i = 0; i < cells.size() && current_offset < end_offset; ++i)
    {
        size_t block_start = first_block_start + i * block_size;
        size_t block_data_size = std::min(block_size, file_size.value() - block_start);
        size_t offset_in_block = (current_offset > block_start) ? current_offset - block_start : 0;
        size_t usable = std::min(block_data_size - offset_in_block, end_offset - current_offset);

        const char * data_ptr = cells[i]->data() + offset_in_block;
        regions.push_back(CachedRegion{
            .handle = std::move(cells[i]),
            .data = data_ptr,
            .size = usable,
            .file_offset = current_offset,
        });

        current_offset += usable;
        ProfileEvents::increment(ProfileEvents::PageCacheReadBytes, usable);
    }

    return regions;
}

bool CachedInMemoryReadBufferFromFile::isContentCached(size_t offset, size_t /*size*/)
{
    /// Usually this is called immediately after seek()ing to `offset`.

    if (!working_buffer.empty())
    {
        chassert(chunk);
        return chunk->range.offset <= offset && chunk->range.offset + chunk->range.size > offset;
    }

    size_t block_size = settings.page_cache_block_size;
    cache_range.offset = offset / block_size * block_size;
    cache_range.size = std::min(block_size, file_size.value() - cache_range.offset);

    /// Use get() instead of contains() to populate `chunk`, so the subsequent nextImpl() call
    /// can reuse it without a second cache lookup.
    UInt128 key_hash = cache_range.hash(cache_key_base_hash);
    chunk = cache->get(key_hash, settings.page_cache_inject_eviction);

    return chunk != nullptr;
}

}
