#include "CachedReadBufferFromRemoteFS.h"

#include <Common/hex.h>
#include <IO/createReadBufferFromFileBase.h>
#include <IO/ReadBufferFromFile.h>


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
    const String & path_,
    FileCachePtr cache_,
    RemoteFSFileReaderCreator remote_file_reader_creator_,
    const ReadSettings & settings_,
    size_t read_until_position_)
    : SeekableReadBuffer(nullptr, 0)
    , log(&Poco::Logger::get("CachedReadBufferFromRemoteFS(" + path_ + ")"))
    , key(cache_->hash(path_))
    , path(path_)
    , cache(cache_)
    , settings(settings_)
    , read_until_position(read_until_position_)
    , remote_file_reader_creator(remote_file_reader_creator_)
    , use_external_buffer(settings_.remote_fs_method == RemoteFSReadMethod::threadpool)
{
}

void CachedReadBufferFromRemoteFS::initialize(size_t offset, size_t size)
{
    file_segments_holder.emplace(cache->getOrSet(key, offset, size));

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
    return std::make_shared<ReadBufferFromFile>(cache->path(key, offset), settings.local_fs_buffer_size);
}

SeekableReadBufferPtr CachedReadBufferFromRemoteFS::getRemoteFSReadBuffer([[maybe_unused]] FileSegmentPtr file_segment, ReadType read_type_)
{
    switch (read_type_)
    {
        case ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE:
        {
            /**
            * Implementation (s3, hdfs, web) buffer might be passed through file segments.
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

            if (remote_file_reader)
                return remote_file_reader;

            remote_file_reader = remote_file_reader_creator();
            return remote_file_reader;
        }
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Cannot use remote filesystem reader with read type: {}", toString(read_type));
    }
}

SeekableReadBufferPtr CachedReadBufferFromRemoteFS::getReadBufferForFileSegment(FileSegmentPtr file_segment)
{
    assert(!file_segment->isDownloader());
    assert(file_offset_of_buffer_end >= file_segment->range().left);

    auto range = file_segment->range();
    [[maybe_unused]] bool first_segment_read_in_range = impl == nullptr;
    bytes_to_predownload = 0;

    SeekableReadBufferPtr implementation_buffer;

    size_t wait_download_max_tries = 5; /// TODO: Make configurable by setting.
    size_t wait_download_tries = 0;

    auto download_state = file_segment->state();
    while (true)
    {
        switch (download_state)
        {
            case FileSegment::State::SKIP_CACHE:
            {
                read_type = ReadType::REMOTE_FS_READ_BYPASS_CACHE;
                implementation_buffer = getRemoteFSReadBuffer(file_segment, read_type);

                break;
            }
            case FileSegment::State::EMPTY:
            {
                auto downloader_id = file_segment->getOrSetDownloader();
                if (downloader_id == file_segment->getCallerId())
                {
                    if (file_offset_of_buffer_end == file_segment->downloadOffset())
                    {
                        read_type = ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE;
                        implementation_buffer = getRemoteFSReadBuffer(file_segment, read_type);
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

                        read_type = ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE;
                        implementation_buffer = getRemoteFSReadBuffer(file_segment, read_type);

                        assert(file_offset_of_buffer_end > file_segment->downloadOffset());
                        bytes_to_predownload = file_offset_of_buffer_end - file_segment->downloadOffset();
                    }

                    break;
                }
                else
                {
                    download_state = file_segment->state();
                    continue;
                }
            }
            case FileSegment::State::DOWNLOADING:
            {
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
                implementation_buffer = getCacheReadBuffer(range.left);

                break;
            }
            case FileSegment::State::PARTIALLY_DOWNLOADED:
            {
                size_t download_offset = file_segment->downloadOffset();
                bool can_start_from_cache = download_offset > file_offset_of_buffer_end;

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
                    implementation_buffer = getCacheReadBuffer(range.left);

                    break;
                }

                LOG_TEST(log, "Current download offset: {}, file offset of buffer end: {}", download_offset, file_offset_of_buffer_end);

                auto downloader_id = file_segment->getOrSetDownloader();
                if (downloader_id == file_segment->getCallerId())
                {
                    read_type = ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE;
                    implementation_buffer = getRemoteFSReadBuffer(file_segment, read_type);

                    if (download_offset < file_offset_of_buffer_end)
                    {
                        ///                   segment{1}
                        /// cache:         [_____|___________
                        ///                      ^
                        ///                      download_offset
                        /// requested_range:          [__________]
                        ///                           ^
                        ///                           file_offset_of_buffer_end

                        assert(file_offset_of_buffer_end > file_segment->downloadOffset());
                        bytes_to_predownload = file_offset_of_buffer_end - file_segment->downloadOffset();
                    }

                    break;
                }

                download_state = file_segment->state();
                continue;
            }
            case FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION:
            {
                size_t download_offset = file_segment->downloadOffset();
                bool can_start_from_cache = download_offset > file_offset_of_buffer_end;

                if (can_start_from_cache)
                {
                    read_type = ReadType::CACHED;
                    implementation_buffer = getCacheReadBuffer(range.left);
                }
                else
                {
                    read_type = ReadType::REMOTE_FS_READ_BYPASS_CACHE;
                    implementation_buffer = getRemoteFSReadBuffer(file_segment, read_type);
                }

                break;
            }
        }

        break;
    }

    [[maybe_unused]] auto download_current_segment = read_type == ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE;
    assert(download_current_segment == file_segment->isDownloader());

    assert(file_segment->range() == range);
    assert(file_offset_of_buffer_end >= range.left && file_offset_of_buffer_end <= range.right);

    LOG_TEST(log, "Current file segment: {}, read type: {}, current file offset: {}",
             range.toString(), toString(read_type), file_offset_of_buffer_end);

    /// TODO: For remote FS read need to set maximum possible right offset -- of
    /// the last empty segment and in s3 buffer check > instead of !=.

    // auto last_non_downloaded_offset = getLastNonDownloadedOffset();
    // implementation_buffer->setReadUntilPosition(last_non_downloaded_offset ? *last_non_downloaded_offset : range.right + 1); /// [..., range.right]
    implementation_buffer->setReadUntilPosition(range.right + 1); /// [..., range.right]

    switch (read_type)
    {
        case ReadType::CACHED:
        {
            size_t seek_offset = file_offset_of_buffer_end - range.left;
            implementation_buffer->seek(seek_offset, SEEK_SET);

            auto * file_reader = dynamic_cast<ReadBufferFromFile *>(implementation_buffer.get());
            size_t file_size = file_reader->size();
            auto state = file_segment->state();

            LOG_TEST(log, "Cache file: {}. Cached seek to: {}, file size: {}, file segment state: {}, download offset: {}",
                     file_reader->getFileName(), seek_offset, file_size, state, file_segment->downloadOffset());

            assert(file_size > 0);
            break;
        }
        case ReadType::REMOTE_FS_READ_BYPASS_CACHE:
        {
            implementation_buffer->seek(file_offset_of_buffer_end, SEEK_SET);
            break;
        }
        case ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE:
        {
            assert(file_segment->isDownloader());

            if (bytes_to_predownload)
            {
                size_t download_offset = file_segment->downloadOffset();
                implementation_buffer->seek(download_offset, SEEK_SET);
            }
            else
            {
                implementation_buffer->seek(file_offset_of_buffer_end, SEEK_SET);
            }

            auto impl_range = implementation_buffer->getRemainingReadRange();
            auto download_offset = file_segment->downloadOffset();
            if (download_offset != impl_range.left)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "BUFFER INIT. Buffer's offsets mismatch; cached buffer offset: {}, implementation buffer offset: {}, "
                    "implementation buffer reading until: {}, file segment info: {}",
                    file_offset_of_buffer_end, impl_range.left, *impl_range.right, file_segment->getInfoForLog());

            break;
        }
    }

    return implementation_buffer;
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

    impl = getReadBufferForFileSegment(*current_file_segment_it);

    LOG_TEST(log, "New segment: {}", (*current_file_segment_it)->range().toString());
    return true;
}

bool CachedReadBufferFromRemoteFS::nextImpl()
{
    if (!initialized)
        initialize(file_offset_of_buffer_end, getTotalSizeToRead());

    if (current_file_segment_it == file_segments_holder->file_segments.end())
        return false;

    bytes_to_predownload = 0;

    if (impl)
    {
        if (!use_external_buffer)
        {
            impl->position() = position();
            assert(!impl->hasPendingData());
        }

        auto file_segment = *current_file_segment_it;
        auto current_read_range = file_segment->range();
        auto current_state = file_segment->state();

        assert(current_read_range.left <= file_offset_of_buffer_end);
        assert(!file_segment->isDownloader());

        bool new_buf = false;
        if (file_offset_of_buffer_end > current_read_range.right)
        {
            new_buf = completeFileSegmentAndGetNext();
            if (!new_buf)
                return false;
        }

        if (!new_buf
            && read_type == ReadType::CACHED && current_state != FileSegment::State::DOWNLOADED)
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

            auto download_offset = file_segment->downloadOffset();
            if (download_offset == file_offset_of_buffer_end)
            {
                impl = getReadBufferForFileSegment(*current_file_segment_it);
                new_buf = true;
            }
            else if (download_offset < file_offset_of_buffer_end)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected {} >= {}", download_offset, file_offset_of_buffer_end);
        }

        if (!new_buf && read_type == ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE)
        {
            /**
            * ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE means that on previous getReadBufferForFileSegment() call
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
            impl = getReadBufferForFileSegment(*current_file_segment_it);
        }
    }
    else
    {
        impl = getReadBufferForFileSegment(*current_file_segment_it);
    }

    if (use_external_buffer)
    {
        assert(!internal_buffer.empty());
        swap(*impl);
    }

    auto & file_segment = *current_file_segment_it;
    auto current_read_range = file_segment->range();

    LOG_TEST(log, "Current segment: {}, downloader: {}, current count: {}, position: {}",
             current_read_range.toString(), file_segment->getDownloader(), impl->count(), impl->getPosition());

    assert(current_read_range.left <= file_offset_of_buffer_end);
    assert(current_read_range.right >= file_offset_of_buffer_end);

    bool result = false;
    size_t size = 0;

    size_t predownload = bytes_to_predownload;
    if (bytes_to_predownload)
    {
        /// Consider this case. Some user needed segment [a, b] and downloaded it partially.
        /// But before he called complete(state) or his holder called complete(),
        /// some other user, who needed segment [a', b'], a < a' < b', started waiting on [a, b] to be
        /// downloaded because it intersects with the range he needs.
        /// But then first downloader fails and second must continue. In this case we need to
        /// download from offset a'' < a', but return buffer from offset a'.
        LOG_TEST(log, "Bytes to predownload: {}, caller_id: {}", bytes_to_predownload, FileSegment::getCallerId());

        assert(impl->getFileOffsetOfBufferEnd() == file_segment->downloadOffset());

        while (true)
        {
            if (!bytes_to_predownload || impl->eof())
            {
                if (bytes_to_predownload)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Failed to predownload remaing {} bytes. Current file segment: {}, current download offset: {}, expected: {}, eof: {}",
                        file_segment->range().toString(), file_segment->downloadOffset(), file_offset_of_buffer_end, impl->eof());

                result = impl->hasPendingData();

                if (result)
                {
                    size = impl->available();
                    nextimpl_working_buffer_offset = impl->offset();

                    auto download_offset = file_segment->downloadOffset();
                    if (download_offset != static_cast<size_t>(impl->getPosition()) || download_offset != file_offset_of_buffer_end)
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR,
                            "Buffer's offsets mismatch after predownloading; download offset: {}, cached buffer offset: {}, implementation buffer offset: {}, "
                            "file segment info: {}", download_offset, file_offset_of_buffer_end, impl->getPosition(), file_segment->getInfoForLog());
                }

                break;
            }

            size_t current_predownload_size = std::min(impl->buffer().size(), bytes_to_predownload);

            if (file_segment->reserve(current_predownload_size))
            {
                LOG_TEST(log, "Left to predownload: {}, buffer size: {}", bytes_to_predownload, impl->buffer().size());

                file_segment->write(impl->buffer().begin(), current_predownload_size);

                bytes_to_predownload -= current_predownload_size;
                impl->position() += current_predownload_size;
            }
            else
            {
                file_segment->complete(FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);
                bytes_to_predownload = 0;

                read_type = ReadType::REMOTE_FS_READ_BYPASS_CACHE;
                file_segment->completeBatchAndResetDownloader();
                impl = getRemoteFSReadBuffer(file_segment, read_type);
                impl->seek(file_offset_of_buffer_end, SEEK_SET);

                LOG_TEST(
                    log, "Predownload failed because of space limit. Will read from remote filesystem starting from offset: {}",
                    file_offset_of_buffer_end);

                break;
            }
        }
    }

    auto download_current_segment = read_type == ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE;
    if (download_current_segment != file_segment->isDownloader())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Incorrect segment state. Having read type: {}, Caller id: {}, downloader id: {}, file segment state: {}",
            toString(read_type), file_segment->getCallerId(), file_segment->getDownloader(), file_segment->state());

    try
    {
        if (!result)
        {
            result = impl->next();
            size = impl->buffer().size();

            if (read_type == ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE)
                LOG_TEST(log, "\n\n\nread from s3 size: {}, current offset: {}, s3 offset: {}, s3 until position: {}, current file segment info: {}\n\n\n",
                        size, file_offset_of_buffer_end, impl->getRemainingReadRange().left,
                        *impl->getRemainingReadRange().right, file_segment->getInfoForLog());
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);

        if (download_current_segment)
            file_segment->complete(FileSegment::State::PARTIALLY_DOWNLOADED, true);

        /// Note: If exception happens in another place -- out of scope of this buffer, then
        /// downloader's FileSegmentsHolder is responsible to call file_segment->complete().

        /// (download_path (if exists) is removed from inside cache)
        throw;
    }

    if (result)
    {
        if (download_current_segment)
        {
            assert(file_offset_of_buffer_end + size - 1 <= file_segment->range().right);

            if (file_segment->reserve(size))
            {
                file_segment->write(predownload ? impl->position() : impl->buffer().begin(), size);
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

        /// Local filesystem does not support bounded reads.
        if (read_type == ReadType::CACHED && std::next(current_file_segment_it) == file_segments_holder->file_segments.end())
        {
            size_t remaining_size_to_read = std::min(current_read_range.right, read_until_position - 1) - file_offset_of_buffer_end + 1;
            size = std::min(size, remaining_size_to_read);
            impl->buffer().resize(size);
        }

        file_offset_of_buffer_end += size;

        assert(read_type == ReadType::CACHED || file_offset_of_buffer_end == impl->getFileOffsetOfBufferEnd());
    }

    if (use_external_buffer)
        swap(*impl);
    else if (result)
        BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->offset());

    if (download_current_segment)
        file_segment->completeBatchAndResetDownloader();

    assert(!file_segment->isDownloader());

    LOG_TEST(log,
             "Key: {}. Returning with {} bytes, buffer position: {} (offset: {}, predownloaded: {}), "
             "buffer available: {}, current range: {}, current offset: {}, file segment state: {}, download offset: {}, read_type: {}, "
             "reading until position: {}, started with offset: {}, remaining ranges: {}",
             getHexUIntLowercase(key), working_buffer.size(), getPosition(), offset(), predownload,
             available(), current_read_range.toString(),
             file_offset_of_buffer_end, FileSegment::stateToString(file_segment->state()), file_segment->downloadOffset(), toString(read_type),
             read_until_position, first_offset, file_segments_holder->toString());

    if (size == 0 && file_offset_of_buffer_end < read_until_position)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Having zero bytes, but range is not finished: file offset: {}, reading until: {}",
                        file_offset_of_buffer_end, read_until_position);
    return result;
}

off_t CachedReadBufferFromRemoteFS::seek(off_t offset, int whence)
{
    first_offset = offset;
    if (initialized)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE,
                        "Seek is allowed only before first read attempt from the buffer");

    if (whence != SEEK_SET)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET allowed");

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
                        path, getHexUIntLowercase(key), file_offset_of_buffer_end,
                        (impl ? std::to_string(impl->getRemainingReadRange().left) + '-' + (impl->getRemainingReadRange().right ? std::to_string(*impl->getRemainingReadRange().right) : "None") : "None"),
                        (current_file_segment_it == file_segments_holder->file_segments.end() ? "None" : (*current_file_segment_it)->getInfoForLog()));
}

}
