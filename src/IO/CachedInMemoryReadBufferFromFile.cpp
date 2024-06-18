#include "CachedInMemoryReadBufferFromFile.h"
#include <IO/SwapHelper.h>
#include <base/scope_guard.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_END_OF_FILE;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}

CachedInMemoryReadBufferFromFile::CachedInMemoryReadBufferFromFile(
    FileChunkAddress cache_key_, PageCachePtr cache_, std::unique_ptr<ReadBufferFromFileBase> in_, const ReadSettings & settings_)
    : ReadBufferFromFileBase(0, nullptr, 0, in_->getFileSize()), cache_key(cache_key_), cache(cache_), settings(settings_), in(std::move(in_))
    , read_until_position(file_size.value())
{
    cache_key.offset = 0;
}

String CachedInMemoryReadBufferFromFile::getFileName() const
{
    return in->getFileName();
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
    read_until_position = position;
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

    if (chunk.has_value() && file_offset_of_buffer_end >= cache_key.offset + cache->chunkSize())
    {
        chassert(file_offset_of_buffer_end == cache_key.offset + cache->chunkSize());
        chunk.reset();
    }

    if (!chunk.has_value())
    {
        cache_key.offset = file_offset_of_buffer_end / cache->chunkSize() * cache->chunkSize();
        chunk = cache->getOrSet(cache_key.hash(), settings.read_from_page_cache_if_exists_otherwise_bypass_cache, settings.page_cache_inject_eviction);

        size_t chunk_size = std::min(cache->chunkSize(), file_size.value() - cache_key.offset);

        std::unique_lock download_lock(chunk->getChunk()->state.download_mutex);

        if (!chunk->isPrefixPopulated(chunk_size))
        {
            /// A few things could be improved here, which may or may not be worth the added complexity:
            ///  * If the next file chunk is in cache, use in->setReadUntilPosition() to limit the read to
            ///    just one chunk. More generally, look ahead in the cache to count how many next chunks
            ///    need to be downloaded. (Up to some limit? And avoid changing `in`'s until-position if
            ///    it's already reasonable; otherwise we'd increase it by one chunk every chunk, discarding
            ///    a half-completed HTTP request every time.)
            ///  * If only a subset of pages are missing from this chunk, download only them,
            ///    with some threshold for avoiding short seeks.
            ///    In particular, if a previous download failed in the middle of the chunk, we could
            ///    resume from that position instead of from the beginning of the chunk.
            ///    (It's also possible in principle that a proper subset of chunk's pages was reclaimed
            ///    by the OS. But, for performance purposes, we should completely ignore that, because
            ///    (a) PageCache normally uses 2 MiB transparent huge pages and has just one such page
            ///    per chunk, and (b) even with 4 KiB pages partial chunk eviction is extremely rare.)
            ///  * If our [position, read_until_position) covers only part of the chunk, we could download
            ///    just that part. (Which would be bad if someone else needs the rest of the chunk and has
            ///    to do a whole new HTTP request to get it. Unclear what the policy should be.)
            ///  * Instead of doing in->next() in a loop until we get the whole chunk, we could return the
            ///    results as soon as in->next() produces them.
            ///    (But this would make the download_mutex situation much more complex, similar to the
            ///    FileSegment::State::PARTIALLY_DOWNLOADED and FileSegment::setRemoteFileReader() stuff.)

            Buffer prev_in_buffer = in->internalBuffer();
            SCOPE_EXIT({ in->set(prev_in_buffer.begin(), prev_in_buffer.size()); });

            size_t pos = 0;
            while (pos < chunk_size)
            {
                char * piece_start = chunk->getChunk()->data + pos;
                size_t piece_size = chunk_size - pos;
                in->set(piece_start, piece_size);
                if (pos == 0)
                    in->seek(cache_key.offset, SEEK_SET);
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

            chunk->markPrefixPopulated(chunk_size);
        }
    }

    nextimpl_working_buffer_offset = file_offset_of_buffer_end - cache_key.offset;
    working_buffer = Buffer(
        chunk->getChunk()->data,
        chunk->getChunk()->data + std::min(chunk->getChunk()->size, read_until_position - cache_key.offset));
    pos = working_buffer.begin() + nextimpl_working_buffer_offset;

    if (!internal_buffer.empty())
    {
        /// We were given an external buffer to read into. Copy the data into it.
        /// Would be nice to avoid this copy, somehow, maybe by making ReadBufferFromRemoteFSGather
        /// and AsynchronousBoundedReadBuffer explicitly aware of the page cache.
        size_t n = std::min(available(), internal_buffer.size());
        memcpy(internal_buffer.begin(), pos, n);
        working_buffer = Buffer(internal_buffer.begin(), internal_buffer.begin() + n);
        pos = working_buffer.begin();
        nextimpl_working_buffer_offset = 0;
    }

    file_offset_of_buffer_end += available();

    return true;
}

}
