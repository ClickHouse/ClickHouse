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
    PageCacheKey cache_key_, PageCachePtr cache_, std::unique_ptr<ReadBufferFromFileBase> in_, const ReadSettings & settings_)
    : ReadBufferFromFileBase(0, nullptr, 0, in_->getFileSize()), cache_key(cache_key_), cache(cache_)
    , settings(settings_)
    , in(std::move(in_)), read_until_position(file_size.value())
    , inner_read_until_position(read_until_position)
{
    cache_key.offset = 0;
}

String CachedInMemoryReadBufferFromFile::getFileName() const
{
    return cache_key.path;
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
        chassert(chunk->key.hash() == cache_key.hash());
        if (file_offset_of_buffer_end < cache_key.offset || file_offset_of_buffer_end >= cache_key.offset + block_size)
            chunk.reset();
    }

    if (chunk == nullptr)
    {
        cache_key.offset = file_offset_of_buffer_end / block_size * block_size;
        cache_key.size = std::min(block_size, file_size.value() - cache_key.offset);

        chunk = cache->getOrSet(cache_key, settings.read_from_page_cache_if_exists_otherwise_bypass_cache, settings.page_cache_inject_eviction, [&](auto cell)
        {
            Buffer prev_in_buffer = in->internalBuffer();
            SCOPE_EXIT({ in->set(prev_in_buffer.begin(), prev_in_buffer.size()); });

            size_t pos = 0;
            while (pos < cache_key.size)
            {
                char * piece_start = cell->data() + pos;
                size_t piece_size = cache_key.size - pos;
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
                        (cache_key.offset / lookahead_bytes + 1) * lookahead_bytes,
                        (read_until_position + block_size - 1) / block_size * block_size});

                    if (inner_read_until_position < cache_key.offset + cache_key.size ||
                        inner_read_until_position > lookahead_block_end)
                    {
                        PageCacheKey temp_key = cache_key;
                        do
                        {
                            temp_key.offset += temp_key.size;
                            temp_key.size = std::min(block_size, file_size.value() - temp_key.offset);
                            chassert(temp_key.offset <= lookahead_block_end);
                        }
                        while (temp_key.offset < lookahead_block_end
                            && !cache->contains(temp_key, settings.page_cache_inject_eviction));
                        inner_read_until_position = temp_key.offset;
                        in->setReadUntilPosition(inner_read_until_position);
                    }

                    in->seek(cache_key.offset, SEEK_SET);
                }
                else
                    chassert(!in->available());

                if (in->eof())
                    throw Exception(ErrorCodes::UNEXPECTED_END_OF_FILE, "File {} ended after {} bytes, but we expected {}",
                        getFileName(), cache_key.offset + pos, file_size.value());

                chassert(in->position() >= piece_start && in->buffer().end() <= piece_start + piece_size);
                chassert(in->getPosition() == static_cast<off_t>(cache_key.offset + pos));

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

    nextimpl_working_buffer_offset = file_offset_of_buffer_end - cache_key.offset;
    working_buffer = Buffer(
        chunk->data(),
        chunk->data() + std::min(chunk->size(), read_until_position - cache_key.offset));
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

bool CachedInMemoryReadBufferFromFile::supportsReadAt()
{
    return in->supportsReadAt();
}

size_t CachedInMemoryReadBufferFromFile::readBigAt(char * to, size_t n, size_t offset, const std::function<bool(size_t m)> & progress_callback) const
{
    /// This method is called from multiple threads in parallel (e.g. by the Parquet reader's
    /// prefetcher via Arrow's ReadAsync). Each call creates independent S3 requests through
    /// in->readBigAt(), enabling parallel downloads. The page cache is thread-safe.

    size_t block_size = settings.page_cache_block_size;
    size_t end_offset = std::min(offset + n, file_size.value());
    size_t bytes_copied = 0;

    while (offset + bytes_copied < end_offset)
    {
        size_t current_offset = offset + bytes_copied;
        size_t block_start = current_offset / block_size * block_size;
        size_t block_data_size = std::min(block_size, file_size.value() - block_start);

        size_t offset_in_block = current_offset - block_start;
        size_t to_copy = std::min(block_data_size - offset_in_block, end_offset - current_offset);

        PageCacheKey key;
        key.path = cache_key.path;
        key.file_version = cache_key.file_version;
        key.offset = block_start;
        key.size = block_data_size;

        auto cell = cache->getOrSet(key, settings.read_from_page_cache_if_exists_otherwise_bypass_cache, settings.page_cache_inject_eviction, [&](const auto & c)
        {
            /// Download the whole block using positional read (thread-safe, creates independent HTTP request).
            size_t bytes_read = in->readBigAt(c->data(), c->size(), block_start, nullptr);
            if (bytes_read < c->size())
                throw Exception(ErrorCodes::UNEXPECTED_END_OF_FILE, "File {} ended after {} bytes, but we expected {}",
                    cache_key.path, block_start + bytes_read, file_size.value());
        });

        memcpy(to + bytes_copied, cell->data() + offset_in_block, to_copy);
        bytes_copied += to_copy;

        ProfileEvents::increment(ProfileEvents::PageCacheReadBytes, to_copy);

        if (progress_callback && progress_callback(bytes_copied))
            break;
    }

    return bytes_copied;
}

bool CachedInMemoryReadBufferFromFile::isContentCached(size_t offset, size_t /*size*/)
{
    /// Usually this is called immediately after seek()ing to `offset`.

    if (!working_buffer.empty())
    {
        chassert(chunk);
        return chunk->key.offset <= offset && chunk->key.offset + chunk->key.size > offset;
    }

    size_t block_size = settings.page_cache_block_size;
    cache_key.offset = offset / block_size * block_size;
    cache_key.size = std::min(block_size, file_size.value() - cache_key.offset);

    /// Use get() instead of contains() to populate `chunk`, so the subsequent nextImpl() call
    /// can reuse it without a second cache lookup.
    chunk = cache->get(cache_key, settings.page_cache_inject_eviction);

    return chunk != nullptr;
}

}
