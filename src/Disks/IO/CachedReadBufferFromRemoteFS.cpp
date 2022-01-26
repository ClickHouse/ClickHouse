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
    SeekableReadBufferPtr downloader_,
    const ReadSettings & settings_,
    size_t read_until_position_)
    : SeekableReadBuffer(nullptr, 0)
    , log(&Poco::Logger::get("CachedReadBufferFromRemoteFS(" + path_ + ")"))
    , key(cache_->hash(path_))
    , cache(cache_)
    , downloader(downloader_)
    , settings(settings_)
    , read_until_position(read_until_position_)
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

SeekableReadBufferPtr CachedReadBufferFromRemoteFS::createCacheReadBuffer(size_t offset) const
{
    return std::make_shared<ReadBufferFromFile>(cache->path(key, offset), settings.local_fs_buffer_size);
}

SeekableReadBufferPtr CachedReadBufferFromRemoteFS::createReadBuffer(FileSegmentPtr file_segment)
{
    auto range = file_segment->range();
    bool first_segment_read_in_range = impl == nullptr;
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
                read_type = ReadType::REMOTE_FS_READ;
                implementation_buffer = downloader;

                break;
            }
            case FileSegment::State::EMPTY:
            {
                auto downloader_id = file_segment->getOrSetDownloader();
                if (downloader_id == file_segment->getCallerId())
                {
                    read_type = ReadType::REMOTE_FS_READ_AND_DOWNLOAD;
                    implementation_buffer = downloader;

                    break;
                }
                else
                {
                    download_state = FileSegment::State::DOWNLOADING;
                    continue;
                }
            }
            case FileSegment::State::DOWNLOADING:
            {
                download_state = file_segment->wait();

                if (++wait_download_tries >= wait_download_max_tries)
                    download_state = FileSegment::State::SKIP_CACHE;

                continue;
            }
            case FileSegment::State::DOWNLOADED:
            {
                read_type = ReadType::CACHED;
                implementation_buffer = createCacheReadBuffer(range.left);

                break;
            }
            case FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION:
            {
                /// If downloader failed before downloading anything, it is determined
                /// whether continuation is possible. In case of no continuation and
                /// downloaded_size == 0 - cache cell is removed and state is switched to SKIP_CACHE.
                assert(file_segment->downloadOffset() > 0);

                read_type = ReadType::CACHED;
                implementation_buffer = createCacheReadBuffer(range.left);

                break;
            }
            case FileSegment::State::PARTIALLY_DOWNLOADED:
            {
                auto downloader_id = file_segment->getOrSetDownloader();
                if (downloader_id == file_segment->getCallerId())
                {
                    size_t download_offset = file_segment->downloadOffset();
                    bool can_start_from_cache = download_offset && download_offset >= file_offset_of_buffer_end;

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
                        implementation_buffer = createCacheReadBuffer(range.left);
                    }
                    else
                    {
                        read_type = ReadType::REMOTE_FS_READ_AND_DOWNLOAD;
                        implementation_buffer = downloader;

                        if (download_offset && download_offset < file_offset_of_buffer_end)
                        {
                            ///                   segment{1}
                            /// cache:         [_____|___________
                            ///                      ^
                            ///                      download_offset
                            /// requested_range:          [__________]
                            ///                           ^
                            ///                           file_offset_of_buffer_end

                            assert(first_segment_read_in_range);
                            bytes_to_predownload = file_offset_of_buffer_end - file_segment->downloadOffset() - 1;
                            LOG_TEST(log, "Bytes to predownload {} for {}", bytes_to_predownload, downloader_id);
                        }
                    }

                    break;
                }
                else
                {
                    download_state = FileSegment::State::DOWNLOADING;
                    continue;
                }
            }
        }

        break;
    }

    assert((!first_segment_read_in_range && range.left == file_offset_of_buffer_end)
           || (first_segment_read_in_range && range.left <= file_offset_of_buffer_end));
    assert(file_segment->range() == range);

    LOG_TEST(log, "Current file segment: {}, read type: {}, current file offset: {}", range.toString(), toString(read_type), file_offset_of_buffer_end);

    implementation_buffer->setReadUntilPosition(range.right + 1); /// [..., range.right]

    switch (read_type)
    {
        case ReadType::CACHED:
        {
            implementation_buffer->seek(file_offset_of_buffer_end - range.left, SEEK_SET);
            break;
        }
        case ReadType::REMOTE_FS_READ:
        {
            implementation_buffer->seek(file_offset_of_buffer_end, SEEK_SET);
            break;
        }
        case ReadType::REMOTE_FS_READ_AND_DOWNLOAD:
        {
            if (bytes_to_predownload)
            {
                size_t download_offset = file_segment->downloadOffset();
                assert(download_offset);
                implementation_buffer->seek(download_offset + 1, SEEK_SET);
            }
            else
            {
                assert(file_offset_of_buffer_end == range.left);
                implementation_buffer->seek(file_offset_of_buffer_end, SEEK_SET);
            }

            break;
        }
    }

    return implementation_buffer;
}

bool CachedReadBufferFromRemoteFS::completeFileSegmentAndGetNext()
{
    LOG_TEST(log, "Completed segment: {}", (*current_file_segment_it)->range().toString());

    auto file_segment_it = current_file_segment_it++;

    [[maybe_unused]] auto range = (*file_segment_it)->range();
    assert(file_offset_of_buffer_end > range.right);

    /// Only downloader completes file segment.
    if (read_type == ReadType::REMOTE_FS_READ_AND_DOWNLOAD)
        (*file_segment_it)->complete(DB::FileSegment::State::DOWNLOADED);

    LOG_TEST(log, "Removing file segment: {}, downloader: {}", (*file_segment_it)->range().toString(), (*file_segment_it)->downloader_id, (*file_segment_it)->state());
    /// Do not hold pointer to file segment if it is not needed anymore
    /// so can become releasable and can be evicted from cache.
    file_segments_holder->file_segments.erase(file_segment_it);

    if (current_file_segment_it == file_segments_holder->file_segments.end())
        return false;

    impl = createReadBuffer(*current_file_segment_it);

    LOG_TEST(log, "New segment: {}", (*current_file_segment_it)->range().toString());
    return true;
}

void CachedReadBufferFromRemoteFS::checkForPartialDownload()
{
    auto state = (*current_file_segment_it)->state();

    if (state != FileSegment::State::PARTIALLY_DOWNLOADED
        && state != FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION)
        return;

    auto current_read_range = (*current_file_segment_it)->range();
    auto last_downloaded_offset = (*current_file_segment_it)->downloadOffset();

    if (file_offset_of_buffer_end > last_downloaded_offset)
    {
        impl = downloader;

        if ((*current_file_segment_it)->state() == FileSegment::State::PARTIALLY_DOWNLOADED)
            read_type = ReadType::REMOTE_FS_READ_AND_DOWNLOAD;
        else
            read_type = ReadType::REMOTE_FS_READ;

        impl->setReadUntilPosition(current_read_range.right + 1); /// [..., range.right]
        impl->seek(file_offset_of_buffer_end, SEEK_SET);
    }
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

        auto current_read_range = (*current_file_segment_it)->range();
        auto current_state = (*current_file_segment_it)->state();

        assert(current_read_range.left <= file_offset_of_buffer_end);

        if (file_offset_of_buffer_end > current_read_range.right)
        {
            if (!completeFileSegmentAndGetNext())
                return false;
        }

        if (current_state == FileSegment::State::PARTIALLY_DOWNLOADED
                || current_state == FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION)
        {
            checkForPartialDownload();
        }
    }
    else
    {
        impl = createReadBuffer(*current_file_segment_it);
    }

    if (use_external_buffer)
    {
        assert(!internal_buffer.empty());
        swap(*impl);
    }

    auto & file_segment = *current_file_segment_it;
    auto current_read_range = file_segment->range();

    assert(current_read_range.left <= file_offset_of_buffer_end);
    assert(current_read_range.right >= file_offset_of_buffer_end);

    bool result = false;
    size_t size = 0;

    if (bytes_to_predownload)
    {
        /// Consider this case. Some user needed segment [a, b] and downloaded it partially
        /// or did not download it at all. But before he called complete(state) or his holder
        /// called complete(), some other user who needed segment [a', b'], a < a' < b' and
        /// started waiting on [a, b] to be downloaded as it intersects with the range he needs.
        /// But the first downloader fails and second must continue. In this case we need to
        /// download from offset a'' < a', but return buffer from offset a'.
        LOG_TEST(log, "Bytes to predownload: {}, caller_id: {}", bytes_to_predownload, FileSegment::getCallerId());

        while (bytes_to_predownload
               && file_segment->downloadOffset() + 1 != file_offset_of_buffer_end
               && downloader->next())
        {
            if (file_segment->reserve(downloader->buffer().size()))
            {
                size_t size_to_cache = std::min(bytes_to_predownload, downloader->buffer().size());

                file_segment->write(downloader->buffer().begin(), size_to_cache);

                bytes_to_predownload -= size_to_cache;
                downloader->position() += size_to_cache;
            }
            else
            {
                file_segment->complete(FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);

                read_type = ReadType::REMOTE_FS_READ;
                bytes_to_predownload = 0;
                break;
            }
        }

        if (file_segment->downloadOffset() + 1 != file_offset_of_buffer_end
            && read_type == ReadType::REMOTE_FS_READ_AND_DOWNLOAD)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Predownloading failed");

        result = downloader->hasPendingData();
        size = downloader->available();
    }

    auto download_current_segment = read_type == ReadType::REMOTE_FS_READ_AND_DOWNLOAD;

    try
    {
        if (!result)
        {
            result = impl->next();
            size = impl->buffer().size();
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);

        if (download_current_segment)
            file_segment->complete(FileSegment::State::PARTIALLY_DOWNLOADED);

        /// Note: If exception happens in another place -- out of scope of this buffer, then
        /// downloader's FileSegmentsHolder is responsible to call file_segment->complete().

        /// (download_path (if exists) is removed from inside cache)
        throw;
    }

    if (result)
    {
        if (download_current_segment)
        {
            if (file_segment->reserve(size))
            {
                file_segment->write(impl->buffer().begin(), size);
            }
            else
            {
                file_segment->complete(FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);
                LOG_DEBUG(log, "No space left in cache, will continue without cache download");
            }
        }

        /// just implement setReadUntilPosition() for local filesysteam read buffer?
        if (read_type == ReadType::CACHED && std::next(current_file_segment_it) == file_segments_holder->file_segments.end())
        {
            size_t remaining_size_to_read = std::min(current_read_range.right, read_until_position - 1) - file_offset_of_buffer_end + 1;
            impl->buffer().resize(std::min(impl->buffer().size(), remaining_size_to_read));
        }

        file_offset_of_buffer_end += impl->buffer().size();

        switch (read_type)
        {
            case ReadType::CACHED:
            {
                ProfileEvents::increment(ProfileEvents::RemoteFSCacheReadBytes, working_buffer.size());
                break;
            }
            case ReadType::REMOTE_FS_READ:
            {
                ProfileEvents::increment(ProfileEvents::RemoteFSReadBytes, working_buffer.size());
                break;
            }
            case ReadType::REMOTE_FS_READ_AND_DOWNLOAD:
            {
                ProfileEvents::increment(ProfileEvents::RemoteFSReadBytes, working_buffer.size());
                ProfileEvents::increment(ProfileEvents::RemoteFSCacheDownloadBytes, working_buffer.size());
                break;
            }
        }
    }

    if (use_external_buffer)
        swap(*impl);
    else
        BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->offset());

    LOG_TEST(log, "Key: {}. Returning with {} bytes, current range: {}, current offset: {}, file segment state: {}, download offset: {}",
             getHexUIntLowercase(key), working_buffer.size(), current_read_range.toString(),
             file_offset_of_buffer_end, FileSegment::toString(file_segment->state()), file_segment->downloadOffset());

    return result;
}

off_t CachedReadBufferFromRemoteFS::seek(off_t offset, int whence)
{
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

}
