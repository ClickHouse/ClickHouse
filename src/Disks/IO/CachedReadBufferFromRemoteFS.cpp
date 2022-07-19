#include "CachedReadBufferFromRemoteFS.h"

#include <Common/assert_cast.h>
#include <Common/hex.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/ReadBufferFromFile.h>
#include <base/scope_guard.h>


namespace ProfileEvents
{
    extern const Event RemoteFSReadBytes;
    extern const Event RemoteFSCacheReadBytes;
    extern const Event RemoteFSCacheDownloadBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int LOGICAL_ERROR;
}

CachedReadBufferFromRemoteFS::CachedReadBufferFromRemoteFS(
    const String & remote_fs_object_path_,
    FileCachePtr cache_,
    RemoteFSFileReaderCreator remote_file_reader_creator_,
    const ReadSettings & settings_,
    size_t read_until_position_)
    : SeekableReadBuffer(nullptr, 0)
#ifndef NDEBUG
    , log(&Poco::Logger::get("CachedReadBufferFromRemoteFS(" + remote_fs_object_path_ + ")"))
#else
    , log(&Poco::Logger::get("CachedReadBufferFromRemoteFS"))
#endif
    , cache_key(cache_->hash(remote_fs_object_path_))
    , remote_fs_object_path(remote_fs_object_path_)
    , cache(cache_)
    , settings(settings_)
    , read_until_position(read_until_position_)
    , remote_file_reader_creator(remote_file_reader_creator_)
{
}

void CachedReadBufferFromRemoteFS::initialize(size_t offset, size_t size)
{
    file_segments_holder.emplace(cache->getOrSet(cache_key, offset, size));

    /**
     * Segments in returned list are ordered in ascending order and represent a full contiguous
     * interval (no holes). Each segment in returned list has state: DOWNLOADED, DOWNLOADING or EMPTY.
     */
    if (file_segments_holder->file_segments.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "List of file segments cannot be empty");

    LOG_TEST(log, "Having {} file segments to read", file_segments_holder->file_segments.size());
    current_file_segment_it = file_segments_holder->file_segments.begin();

    initialized = true;
}

SeekableReadBufferPtr CachedReadBufferFromRemoteFS::getCacheReadBuffer(size_t offset) const
{
    return std::make_shared<ReadBufferFromFile>(cache->getPathInLocalCache(cache_key, offset), settings.local_fs_buffer_size);
}

SeekableReadBufferPtr CachedReadBufferFromRemoteFS::getRemoteFSReadBuffer(FileSegmentPtr & file_segment, ReadType read_type_)
{
    switch (read_type_)
    {
        case ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE:
        {
            /**
            * Each downloader is elected to download at most buffer_size bytes and then any other can
            * continue. The one who continues download should reuse download buffer.
            *
            * TODO: Also implementation (s3, hdfs, web) buffer might be passed through file segments.
            * E.g. consider for query1 and query2 we need intersecting ranges like this:
            *
            *     [___________]         -- read_range_1 for query1
            *        [_______________]  -- read_range_2 for query2
            *     ^___________^______^
            *     | segment1 | segment2
            *
            * So query2 can reuse implementation buffer, which downloaded segment1.
            * Implementation buffer from segment1 is passed to segment2 once segment1 is loaded.
            */

            auto remote_fs_segment_reader = file_segment->getRemoteFileReader();

            if (remote_fs_segment_reader)
                return remote_fs_segment_reader;

            remote_fs_segment_reader = remote_file_reader_creator();
            file_segment->setRemoteFileReader(remote_fs_segment_reader);

            ///TODO: add check for pending data
            return remote_fs_segment_reader;
        }
        case ReadType::REMOTE_FS_READ_BYPASS_CACHE:
        {
            /// Result buffer is owned only by current buffer -- not shareable like in the case above.

            if (remote_file_reader && remote_file_reader->getFileOffsetOfBufferEnd() == file_offset_of_buffer_end)
                return remote_file_reader;

            remote_file_reader = remote_file_reader_creator();
            return remote_file_reader;
        }
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Cannot use remote filesystem reader with read type: {}", toString(read_type));
    }
}

SeekableReadBufferPtr CachedReadBufferFromRemoteFS::getReadBufferForFileSegment(FileSegmentPtr & file_segment)
{
    auto range = file_segment->range();

    /// Each wait() call has a timeout of 1 second.
    size_t wait_download_max_tries = settings.remote_fs_cache_max_wait_sec;
    size_t wait_download_tries = 0;

    auto download_state = file_segment->state();
    while (true)
    {
        switch (download_state)
        {
            case FileSegment::State::SKIP_CACHE:
            {
                read_type = ReadType::REMOTE_FS_READ_BYPASS_CACHE;
                return getRemoteFSReadBuffer(file_segment, read_type);
            }
            case FileSegment::State::EMPTY:
            {
                auto downloader_id = file_segment->getOrSetDownloader();
                if (downloader_id == file_segment->getCallerId())
                {
                    if (file_offset_of_buffer_end == file_segment->getDownloadOffset())
                    {
                        read_type = ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE;
                        return getRemoteFSReadBuffer(file_segment, read_type);
                    }
                    else
                    {
                        ///                      segment{k}
                        /// cache:           [______|___________
                        ///                         ^
                        ///                         download_offset
                        /// requested_range:            [__________]
                        ///                             ^
                        ///                             file_offset_of_buffer_end
                        assert(file_offset_of_buffer_end > file_segment->getDownloadOffset());
                        bytes_to_predownload = file_offset_of_buffer_end - file_segment->getDownloadOffset();

                        read_type = ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE;
                        return getRemoteFSReadBuffer(file_segment, read_type);
                    }
                }
                else
                {
                    download_state = file_segment->state();
                    continue;
                }
            }
            case FileSegment::State::DOWNLOADING:
            {
                size_t download_offset = file_segment->getDownloadOffset();
                bool can_start_from_cache = download_offset > file_offset_of_buffer_end;

                /// If file segment is being downloaded but we can already read from already downloaded part, do that.
                if (can_start_from_cache)
                {
                    ///                      segment{k} state: DOWNLOADING
                    /// cache:           [______|___________
                    ///                         ^
                    ///                         download_offset (in progress)
                    /// requested_range:    [__________]
                    ///                     ^
                    ///                     file_offset_of_buffer_end

                    read_type = ReadType::CACHED;
                    return getCacheReadBuffer(range.left);
                }

                if (wait_download_tries++ < wait_download_max_tries)
                {
                    download_state = file_segment->wait();
                }
                else
                {
                    download_state = FileSegment::State::SKIP_CACHE;
                }

                continue;
            }
            case FileSegment::State::DOWNLOADED:
            {
                read_type = ReadType::CACHED;
                return getCacheReadBuffer(range.left);
            }
            case FileSegment::State::PARTIALLY_DOWNLOADED:
            {
                auto downloader_id = file_segment->getOrSetDownloader();
                if (downloader_id == file_segment->getCallerId())
                {
                    size_t download_offset = file_segment->getDownloadOffset();
                    bool can_start_from_cache = download_offset > file_offset_of_buffer_end;

                    LOG_TEST(log, "Current download offset: {}, file offset of buffer end: {}", download_offset, file_offset_of_buffer_end);

                    if (can_start_from_cache)
                    {
                        ///                      segment{k}
                        /// cache:           [______|___________
                        ///                         ^
                        ///                         download_offset
                        /// requested_range:    [__________]
                        ///                     ^
                        ///                     file_offset_of_buffer_end

                        read_type = ReadType::CACHED;
                        file_segment->resetDownloader();
                        return getCacheReadBuffer(range.left);
                    }

                    if (download_offset < file_offset_of_buffer_end)
                    {
                        ///                   segment{1}
                        /// cache:         [_____|___________
                        ///                      ^
                        ///                      download_offset
                        /// requested_range:          [__________]
                        ///                           ^
                        ///                           file_offset_of_buffer_end

                        assert(file_offset_of_buffer_end > file_segment->getDownloadOffset());
                        bytes_to_predownload = file_offset_of_buffer_end - file_segment->getDownloadOffset();
                        assert(bytes_to_predownload < range.size());
                    }

                    download_offset = file_segment->getDownloadOffset();
                    can_start_from_cache = download_offset > file_offset_of_buffer_end;
                    assert(!can_start_from_cache);

                    read_type = ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE;
                    return getRemoteFSReadBuffer(file_segment, read_type);
                }

                download_state = file_segment->state();
                continue;
            }
            case FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION:
            {
                size_t download_offset = file_segment->getDownloadOffset();
                bool can_start_from_cache = download_offset > file_offset_of_buffer_end;

                if (can_start_from_cache)
                {
                    read_type = ReadType::CACHED;
                    return getCacheReadBuffer(range.left);
                }
                else
                {
                    read_type = ReadType::REMOTE_FS_READ_BYPASS_CACHE;
                    return getRemoteFSReadBuffer(file_segment, read_type);
                }
            }
        }
    }
}

SeekableReadBufferPtr CachedReadBufferFromRemoteFS::getImplementationBuffer(FileSegmentPtr & file_segment)
{
    assert(!file_segment->isDownloader());
    assert(file_offset_of_buffer_end >= file_segment->range().left);

    auto range = file_segment->range();
    bytes_to_predownload = 0;

    auto read_buffer_for_file_segment = getReadBufferForFileSegment(file_segment);

    [[maybe_unused]] auto download_current_segment = read_type == ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE;
    assert(download_current_segment == file_segment->isDownloader());

    assert(file_segment->range() == range);
    assert(file_offset_of_buffer_end >= range.left && file_offset_of_buffer_end <= range.right);

    LOG_TEST(log, "Current file segment: {}, read type: {}, current file offset: {}",
             range.toString(), toString(read_type), file_offset_of_buffer_end);

    read_buffer_for_file_segment->setReadUntilPosition(range.right + 1); /// [..., range.right]

    switch (read_type)
    {
        case ReadType::CACHED:
        {
            size_t seek_offset = file_offset_of_buffer_end - range.left;
            read_buffer_for_file_segment->seek(seek_offset, SEEK_SET);

            auto * file_reader = assert_cast<ReadBufferFromFile *>(read_buffer_for_file_segment.get());
            size_t file_size = file_reader->size();
            auto state = file_segment->state();

            LOG_TEST(log, "Cache file: {}. Cached seek to: {}, file size: {}, file segment state: {}, download offset: {}",
                    file_reader->getFileName(), seek_offset, file_size, state, file_segment->getDownloadOffset());

            assert(file_size > 0);
            break;
        }
        case ReadType::REMOTE_FS_READ_BYPASS_CACHE:
        {
            read_buffer_for_file_segment->seek(file_offset_of_buffer_end, SEEK_SET);
            break;
        }
        case ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE:
        {
            assert(file_segment->isDownloader());

            if (bytes_to_predownload)
            {
                size_t download_offset = file_segment->getDownloadOffset();
                read_buffer_for_file_segment->seek(download_offset, SEEK_SET);
            }
            else
            {
                read_buffer_for_file_segment->seek(file_offset_of_buffer_end, SEEK_SET);
            }

            auto impl_range = read_buffer_for_file_segment->getRemainingReadRange();
            auto download_offset = file_segment->getDownloadOffset();
            if (download_offset != static_cast<size_t>(read_buffer_for_file_segment->getPosition()))
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Buffer's offsets mismatch; cached buffer offset: {}, download_offset: {}, position: {}, implementation buffer offset: {}, "
                    "implementation buffer reading until: {}, file segment info: {}",
                    file_offset_of_buffer_end, download_offset, read_buffer_for_file_segment->getPosition(),
                    impl_range.left, *impl_range.right, file_segment->getInfoForLog());

            break;
        }
    }

    return read_buffer_for_file_segment;
}

bool CachedReadBufferFromRemoteFS::completeFileSegmentAndGetNext()
{
    LOG_TEST(log, "Completed segment: {}", (*current_file_segment_it)->range().toString());

    auto file_segment_it = current_file_segment_it++;
    auto & file_segment = *file_segment_it;

    [[maybe_unused]] const auto & range = file_segment->range();
    assert(file_offset_of_buffer_end > range.right);

    LOG_TEST(log, "Removing file segment: {}, downloader: {}, state: {}",
             file_segment->range().toString(), file_segment->getDownloader(), file_segment->state());

    /// Do not hold pointer to file segment if it is not needed anymore
    /// so can become releasable and can be evicted from cache.
    file_segments_holder->file_segments.erase(file_segment_it);

    if (current_file_segment_it == file_segments_holder->file_segments.end())
        return false;

    implementation_buffer = getImplementationBuffer(*current_file_segment_it);

    LOG_TEST(log, "New segment: {}", (*current_file_segment_it)->range().toString());
    return true;
}

void CachedReadBufferFromRemoteFS::predownload(FileSegmentPtr & file_segment)
{
    if (bytes_to_predownload)
    {
        /// Consider this case. Some user needed segment [a, b] and downloaded it partially.
        /// But before he called complete(state) or his holder called complete(),
        /// some other user, who needed segment [a', b'], a < a' < b', started waiting on [a, b] to be
        /// downloaded because it intersects with the range he needs.
        /// But then first downloader fails and second must continue. In this case we need to
        /// download from offset a'' < a', but return buffer from offset a'.
        LOG_TEST(log, "Bytes to predownload: {}, caller_id: {}", bytes_to_predownload, FileSegment::getCallerId());

        assert(implementation_buffer->getFileOffsetOfBufferEnd() == file_segment->getDownloadOffset());
        const auto & current_range = file_segment->range();

        while (true)
        {
            if (!bytes_to_predownload || implementation_buffer->eof())
            {
                if (bytes_to_predownload)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Failed to predownload remaining {} bytes. Current file segment: {}, current download offset: {}, expected: {}, eof: {}",
                        file_segment->range().toString(), file_segment->getDownloadOffset(), file_offset_of_buffer_end, implementation_buffer->eof());

                auto result = implementation_buffer->hasPendingData();

                if (result)
                {
                    nextimpl_working_buffer_offset = implementation_buffer->offset();

                    auto download_offset = file_segment->getDownloadOffset();
                    if (download_offset != static_cast<size_t>(implementation_buffer->getPosition()) || download_offset != file_offset_of_buffer_end)
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR,
                            "Buffer's offsets mismatch after predownloading; download offset: {}, cached buffer offset: {}, implementation buffer offset: {}, "
                            "file segment info: {}", download_offset, file_offset_of_buffer_end, implementation_buffer->getPosition(), file_segment->getInfoForLog());
                }

                break;
            }

            size_t current_predownload_size = std::min(implementation_buffer->buffer().size(), bytes_to_predownload);

            if (file_segment->reserve(current_predownload_size))
            {
                LOG_TEST(log, "Left to predownload: {}, buffer size: {}", bytes_to_predownload, implementation_buffer->buffer().size());

                file_segment->write(implementation_buffer->buffer().begin(), current_predownload_size);

                bytes_to_predownload -= current_predownload_size;
                implementation_buffer->position() += current_predownload_size;
            }
            else
            {
                /// We were predownloading:
                ///                   segment{1}
                /// cache:         [_____|___________
                ///                      ^
                ///                      download_offset
                /// requested_range:          [__________]
                ///                           ^
                ///                           file_offset_of_buffer_end
                /// But space reservation failed.
                /// So get working and internal buffer from predownload buffer, get new download buffer,
                /// return buffer back, seek to actual position.
                /// We could reuse predownload buffer and just seek to needed position, but for now
                /// seek is only allowed once for ReadBufferForS3 - before call to nextImpl.
                /// TODO: allow seek more than once with seek avoiding.

                bytes_to_predownload = 0;
                file_segment->complete(FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);

                read_type = ReadType::REMOTE_FS_READ_BYPASS_CACHE;

                swap(*implementation_buffer);
                resetWorkingBuffer();

                implementation_buffer = getRemoteFSReadBuffer(file_segment, read_type);

                swap(*implementation_buffer);

                implementation_buffer->setReadUntilPosition(current_range.right + 1); /// [..., range.right]
                implementation_buffer->seek(file_offset_of_buffer_end, SEEK_SET);

                LOG_TEST(
                    log, "Predownload failed because of space limit. Will read from remote filesystem starting from offset: {}",
                    file_offset_of_buffer_end);

                break;
            }
        }
    }
}

bool CachedReadBufferFromRemoteFS::updateImplementationBufferIfNeeded()
{
    auto & file_segment = *current_file_segment_it;
    auto current_read_range = file_segment->range();
    auto current_state = file_segment->state();

    assert(current_read_range.left <= file_offset_of_buffer_end);
    assert(!file_segment->isDownloader());

    if (file_offset_of_buffer_end > current_read_range.right)
    {
        return completeFileSegmentAndGetNext();
    }

    if (read_type == ReadType::CACHED && current_state != FileSegment::State::DOWNLOADED)
    {
        /// If current read_type is ReadType::CACHED and file segment is not DOWNLOADED,
        /// it means the following case, e.g. we started from CacheReadBuffer and continue with RemoteFSReadBuffer.
        ///                      segment{k}
        /// cache:           [______|___________
        ///                         ^
        ///                         download_offset
        /// requested_range:    [__________]
        ///                     ^
        ///                     file_offset_of_buffer_end

        auto download_offset = file_segment->getDownloadOffset();
        if (download_offset == file_offset_of_buffer_end)
        {
            /// TODO: makes sense to reuse local file reader if we return here with CACHED read type again?
            implementation_buffer = getImplementationBuffer(*current_file_segment_it);

            return true;
        }
        else if (download_offset < file_offset_of_buffer_end)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected {} >= {} ({})", download_offset, file_offset_of_buffer_end, getInfoForLog());
    }

    if (read_type == ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE)
    {
        /**
        * ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE means that on previous getImplementationBuffer() call
        * current buffer successfully called file_segment->getOrSetDownloader() and became a downloader
        * for this file segment. However, the downloader's term has a lifespan of 1 nextImpl() call,
        * e.g. downloader reads buffer_size byte and calls completeBatchAndResetDownloader() and some other
        * thread can become a downloader if it calls getOrSetDownloader() faster.
        *
        * So downloader is committed to download only buffer_size bytes and then is not a downloader anymore,
        * because there is no guarantee on a higher level, that current buffer will not disappear without
        * being destructed till the end of query or without finishing the read range, which he was supposed
        * to read by marks range given to him. Therefore, each nextImpl() call, in case of
        * READ_AND_PUT_IN_CACHE, starts with getOrSetDownloader().
        */
        implementation_buffer = getImplementationBuffer(*current_file_segment_it);
    }

    return true;
}

bool CachedReadBufferFromRemoteFS::nextImpl()
{
    try
    {
        return nextImplStep();
    }
    catch (Exception & e)
    {
        e.addMessage("Cache info: {}", getInfoForLog());
        throw;
    }
}

bool CachedReadBufferFromRemoteFS::nextImplStep()
{
    if (IFileCache::shouldBypassCache())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Using cache when not allowed");

    if (!initialized)
        initialize(file_offset_of_buffer_end, getTotalSizeToRead());

    if (current_file_segment_it == file_segments_holder->file_segments.end())
        return false;

    SCOPE_EXIT({
        if (current_file_segment_it == file_segments_holder->file_segments.end())
            return;

        auto & file_segment = *current_file_segment_it;

        bool download_current_segment = read_type == ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE;
        if (download_current_segment)
        {
            try
            {
                bool file_segment_already_completed = !file_segment->isDownloader();
                if (!file_segment_already_completed)
                    file_segment->completeBatchAndResetDownloader();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }

        assert(!file_segment->isDownloader());
    });

    bytes_to_predownload = 0;

    if (implementation_buffer)
    {
        bool can_read_further = updateImplementationBufferIfNeeded();
        if (!can_read_further)
            return false;
    }
    else
    {
        implementation_buffer = getImplementationBuffer(*current_file_segment_it);
    }

    assert(!internal_buffer.empty());
    swap(*implementation_buffer);

    auto & file_segment = *current_file_segment_it;
    auto current_read_range = file_segment->range();

    LOG_TEST(log, "Current segment: {}, downloader: {}, current count: {}, position: {}",
             current_read_range.toString(), file_segment->getDownloader(), implementation_buffer->count(), implementation_buffer->getPosition());

    assert(current_read_range.left <= file_offset_of_buffer_end);
    assert(current_read_range.right >= file_offset_of_buffer_end);

    bool result = false;
    size_t size = 0;

    size_t needed_to_predownload = bytes_to_predownload;
    if (needed_to_predownload)
    {
        predownload(file_segment);

        result = implementation_buffer->hasPendingData();
        size = implementation_buffer->available();
    }

    auto download_current_segment = read_type == ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE;
    if (download_current_segment != file_segment->isDownloader())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Incorrect segment state. Having read type: {}, Caller id: {}, downloader id: {}, file segment state: {}",
            toString(read_type), file_segment->getCallerId(), file_segment->getDownloader(), file_segment->state());

    if (!result)
    {
        result = implementation_buffer->next();
        size = implementation_buffer->buffer().size();
    }

    if (result)
    {
        if (download_current_segment)
        {
            assert(file_offset_of_buffer_end + size - 1 <= file_segment->range().right);

            if (file_segment->reserve(size))
            {
                file_segment->write(needed_to_predownload ? implementation_buffer->position() : implementation_buffer->buffer().begin(), size);
            }
            else
            {
                download_current_segment = false;
                file_segment->complete(FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);
                LOG_DEBUG(log, "No space left in cache, will continue without cache download");
            }
        }

        switch (read_type)
        {
            case ReadType::CACHED:
            {
                ProfileEvents::increment(ProfileEvents::RemoteFSCacheReadBytes, size);
                break;
            }
            case ReadType::REMOTE_FS_READ_BYPASS_CACHE:
            {
                ProfileEvents::increment(ProfileEvents::RemoteFSReadBytes, size);
                break;
            }
            case ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE:
            {
                ProfileEvents::increment(ProfileEvents::RemoteFSReadBytes, size);
                ProfileEvents::increment(ProfileEvents::RemoteFSCacheDownloadBytes, size);
                break;
            }
        }

        if (std::next(current_file_segment_it) == file_segments_holder->file_segments.end())
        {
            size_t remaining_size_to_read = std::min(current_read_range.right, read_until_position - 1) - file_offset_of_buffer_end + 1;
            size = std::min(size, remaining_size_to_read);
            implementation_buffer->buffer().resize(nextimpl_working_buffer_offset + size);
        }

        file_offset_of_buffer_end += size;
    }

    swap(*implementation_buffer);

    if (download_current_segment)
        file_segment->completeBatchAndResetDownloader();

    assert(!file_segment->isDownloader());

    LOG_TEST(log,
             "Key: {}. Returning with {} bytes, buffer position: {} (offset: {}, predownloaded: {}), "
             "buffer available: {}, current range: {}, current offset: {}, file segment state: {}, download offset: {}, read_type: {}, "
             "reading until position: {}, started with offset: {}, remaining ranges: {}",
             getHexUIntLowercase(cache_key), working_buffer.size(), getPosition(), offset(), needed_to_predownload,
             available(), current_read_range.toString(),
             file_offset_of_buffer_end, FileSegment::stateToString(file_segment->state()), file_segment->getDownloadOffset(), toString(read_type),
             read_until_position, first_offset, file_segments_holder->toString());

    if (size == 0 && file_offset_of_buffer_end < read_until_position)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Having zero bytes, but range is not finished: file offset: {}, reading until: {}",
                        file_offset_of_buffer_end, read_until_position);
    return result;
}

off_t CachedReadBufferFromRemoteFS::seek(off_t offset, int whence)
{
    if (initialized)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE,
                        "Seek is allowed only before first read attempt from the buffer");

    if (whence != SEEK_SET)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET allowed");

    first_offset = offset;
    file_offset_of_buffer_end = offset;
    size_t size = getTotalSizeToRead();
    initialize(offset, size);

    return offset;
}

size_t CachedReadBufferFromRemoteFS::getTotalSizeToRead()
{
    /// Last position should be guaranteed to be set, as at least we always know file size.
    if (!read_until_position)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Last position was not set");

    /// On this level should be guaranteed that read size is non-zero.
    if (file_offset_of_buffer_end >= read_until_position)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Read boundaries mismatch. Expected {} < {}",
                        file_offset_of_buffer_end, read_until_position);

    return read_until_position - file_offset_of_buffer_end;
}

void CachedReadBufferFromRemoteFS::setReadUntilPosition(size_t)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Method `setReadUntilPosition()` not allowed");
}

off_t CachedReadBufferFromRemoteFS::getPosition()
{
    return file_offset_of_buffer_end - available();
}

std::optional<size_t> CachedReadBufferFromRemoteFS::getLastNonDownloadedOffset() const
{
    if (!file_segments_holder)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File segments holder not initialized");

    const auto & file_segments = file_segments_holder->file_segments;
    for (auto it = file_segments.rbegin(); it != file_segments.rend(); ++it)
    {
        const auto & file_segment = *it;
        if (file_segment->state() != FileSegment::State::DOWNLOADED)
            return file_segment->range().right;
    }

    return std::nullopt;
}

String CachedReadBufferFromRemoteFS::getInfoForLog()
{
    return fmt::format("Buffer path: {}, hash key: {}, file_offset_of_buffer_end: {}, internal buffer remaining read range: {}, file segment info: {}",
                        remote_fs_object_path, getHexUIntLowercase(cache_key), file_offset_of_buffer_end,
                       (implementation_buffer ?
                        std::to_string(implementation_buffer->getRemainingReadRange().left) + '-' + (implementation_buffer->getRemainingReadRange().right ? std::to_string(*implementation_buffer->getRemainingReadRange().right) : "None")
                        : "None"),
                        (current_file_segment_it == file_segments_holder->file_segments.end() ? "None" : (*current_file_segment_it)->getInfoForLog()));
}

}
