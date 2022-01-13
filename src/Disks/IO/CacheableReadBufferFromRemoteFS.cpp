#include "CacheableReadBufferFromRemoteFS.h"
#include <IO/createReadBufferFromFileBase.h>
#include <filesystem>

namespace fs = std::filesystem;

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

CacheableReadBufferFromRemoteFS::CacheableReadBufferFromRemoteFS(
    const String & path_,
    FileCachePtr cache_,
    SeekableReadBufferPtr reader_,
    const ReadSettings & settings_,
    size_t read_until_position_)
    : SeekableReadBuffer(nullptr, 0)
    , log(&Poco::Logger::get("CacheableReadBufferFromRemoteFS"))
    , key(cache_->hash(path_))
    , cache(cache_)
    , reader(reader_)
    , settings(settings_)
    , read_until_position(read_until_position_)
{
}

void CacheableReadBufferFromRemoteFS::initialize(size_t offset, size_t size)
{
    file_segments_holder.emplace(cache->getOrSet(key, offset, size));

    /**
     * Segments in returned list are ordered in ascending order and represent a full contiguous
     * interval (no holes). Each segment in returned list has state: DOWNLOADED, DOWNLOADING or EMPTY.
     * DOWNLOADING means that either the segment is being downloaded by some other thread or that it
     * is going to be downloaded by the caller (just space reservation happened).
     * EMPTY means that the segment not in cache, not being downloaded and cannot be downloaded
     * by the caller (because of not enough space or max elements limit reached). E.g. returned list is never empty.
     */
    if (file_segments_holder->file_segments.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "List of file segments cannot be empty");

    LOG_TEST(log, "Having {} file segments to read", file_segments_holder->file_segments.size());
    current_file_segment_it = file_segments_holder->file_segments.begin();

    initialized = true;
}

SeekableReadBufferPtr CacheableReadBufferFromRemoteFS::createReadBuffer(FileSegmentPtr file_segment)
{
    auto range = file_segment->range();
    LOG_TEST(log, "Current file segment: {}", range.toString());

    assert(!impl || range.left == file_offset_of_buffer_end);

    SeekableReadBufferPtr implementation_buffer;
    switch (file_segment->state())
    {
        case FileSegment::State::DOWNLOADED:
        {
            read_type = ReadType::CACHE;
            implementation_buffer = createReadBufferFromFileBase(cache->path(key, file_offset_of_buffer_end), settings);
            break;
        }
        case FileSegment::State::DOWNLOADING:
        {
            if (file_segment->isDownloader())
            {
                download_path = cache->path(key, file_offset_of_buffer_end);
                download_buffer = std::make_unique<WriteBufferFromFile>(download_path);

                read_type = ReadType::REMOTE_FS_READ_AND_DOWNLOAD;
                implementation_buffer = reader;
            }
            else
            {
                auto download_state = file_segment->wait();

                if (download_state == FileSegment::State::DOWNLOADED)
                {
                    read_type = ReadType::CACHE;
                    implementation_buffer = createReadBufferFromFileBase(cache->path(key, file_offset_of_buffer_end), settings);
                }

                read_type = ReadType::REMOTE_FS_READ;
                implementation_buffer = reader;
            }
            break;
        }
        case FileSegment::State::ERROR:
        case FileSegment::State::EMPTY:
        {
            read_type = ReadType::REMOTE_FS_READ;
            implementation_buffer = reader;
            break;
        }
    }

    download_current_segment = read_type == ReadType::REMOTE_FS_READ_AND_DOWNLOAD;

    /// TODO: Add seek avoiding for s3 on the lowest level.
    implementation_buffer->setReadUntilPosition(range.right + 1); /// [..., range.right]
    implementation_buffer->seek(range.left, SEEK_SET);

    return implementation_buffer;
}

bool CacheableReadBufferFromRemoteFS::nextImpl()
{
    if (!initialized)
        initialize(file_offset_of_buffer_end, getTotalSizeToRead());

    if (current_file_segment_it == file_segments_holder->file_segments.end())
        return false;

    if (impl)
    {
        auto current_read_range = (*current_file_segment_it)->range();
        assert(current_read_range.left <= file_offset_of_buffer_end);

        /// Previous file segment was read till the end.
        if (file_offset_of_buffer_end > current_read_range.right)
        {
            if (download_current_segment)
            {
                (*current_file_segment_it)->complete(FileSegment::State::DOWNLOADED);

                download_buffer.reset();
                download_path.clear();
            }

            if (++current_file_segment_it == file_segments_holder->file_segments.end())
                return false;

            impl = createReadBuffer(*current_file_segment_it);
        }
    }
    else
    {
        impl = createReadBuffer(*current_file_segment_it);
    }

    auto current_read_range = (*current_file_segment_it)->range();
    size_t remaining_size_to_read = std::min(current_read_range.right, read_until_position - 1) - file_offset_of_buffer_end + 1;

    assert(current_read_range.left <= file_offset_of_buffer_end);
    assert(current_read_range.right >= file_offset_of_buffer_end);

    swap(*impl);
    bool result;
    try
    {
        result = impl->next();
        LOG_TEST(log, "Read {} bytes. Remaining bytes to read = {}", impl->buffer().size(), remaining_size_to_read);

        if (result)
        {
            if (download_current_segment)
                download_buffer->write(working_buffer.begin(), working_buffer.size());
        }
    }
    catch (...)
    {
        if (download_current_segment)
            (*current_file_segment_it)->complete(FileSegment::State::ERROR);

        /// Note: If exception happens in another place -- out of scope of this buffer, then
        /// downloader's FileSegmentsHolder is responsible to set ERROR state and call notify.

        /// (download_path (if exists) is removed from inside cache)
        throw;
    }

    if (result)
    {
        /// TODO: This resize() is needed only for local fs read, so it is better to
        /// just implement setReadUntilPosition() for local filesysteam read buffer?
        impl->buffer().resize(std::min(impl->buffer().size(), remaining_size_to_read));
        file_offset_of_buffer_end += working_buffer.size();

        switch (read_type)
        {
            case ReadType::CACHE:
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

    swap(*impl);

    LOG_TEST(log, "Returning with {} bytes", working_buffer.size());
    return result;
}

off_t CacheableReadBufferFromRemoteFS::seek(off_t offset, int whence)
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

size_t CacheableReadBufferFromRemoteFS::getTotalSizeToRead()
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

off_t CacheableReadBufferFromRemoteFS::getPosition()
{
    return file_offset_of_buffer_end - available();
}

}
