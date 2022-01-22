#include "FileSegment.h"
#include <Common/FileCache.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_CACHE_ERROR;
}

FileSegment::FileSegment(
        size_t offset_,
        size_t size_,
        const FileCacheKey & key_,
        FileCache * cache_,
        State download_state_)
    : segment_range(offset_, offset_ + size_ - 1)
    , download_state(download_state_)
    , file_key(key_)
    , cache(cache_)
{
    assert(download_state == State::DOWNLOADED || download_state == State::EMPTY);
}

size_t FileSegment::downloadedSize() const
{
    std::lock_guard segment_lock(mutex);
    return downloaded_size;
}

size_t FileSegment::reservedSize() const
{
    std::lock_guard segment_lock(mutex);
    return reserved_size;
}

String FileSegment::getCallerId()
{
    if (!CurrentThread::isInitialized() || CurrentThread::getQueryId().size == 0)
        throw Exception(ErrorCodes::FILE_CACHE_ERROR, "Cannot use cache without query id");

    return CurrentThread::getQueryId().toString();
}

String FileSegment::getOrSetDownloader()
{
    std::lock_guard segment_lock(mutex);

    if (downloader_id.empty())
    {
        downloader_id = getCallerId();
        download_state = State::DOWNLOADING;
    }

    return downloader_id;
}

bool FileSegment::isDownloader() const
{
    std::lock_guard segment_lock(mutex);
    return getCallerId() == downloader_id;
}

void FileSegment::write(const char * from, size_t size)
{
    if (available() < size)
        throw Exception(
            ErrorCodes::FILE_CACHE_ERROR,
            "Not enough space is reserved. Available: {}, expected: {}", available(), size);

    if (!download_buffer)
    {
        assert(!downloaded_size);
        auto download_path = cache->path(key(), range().left);
        download_buffer = std::make_unique<WriteBufferFromFile>(download_path);
    }

    download_buffer->write(from, size);
    downloaded_size += size;
}

FileSegment::State FileSegment::wait()
{
    std::unique_lock segment_lock(mutex);

    if (download_state == State::EMPTY)
        throw Exception(ErrorCodes::FILE_CACHE_ERROR, "Cannot wait on a file segment with empty state");

    if (download_state == State::DOWNLOADING)
    {
        LOG_TEST(&Poco::Logger::get("kssenii"), "Waiting on: {}", range().toString());
        cv.wait_for(segment_lock, std::chrono::seconds(60)); /// TODO: pass through settings
    }

    return download_state;
}

bool FileSegment::reserve(size_t size)
{
    if (!size)
        throw Exception(ErrorCodes::FILE_CACHE_ERROR, "Zero space reservation is not allowed");

    std::lock_guard segment_lock(mutex);

    if (downloaded_size == range().size())
        throw Exception(ErrorCodes::FILE_CACHE_ERROR,
                        "Attempt to reserve space for fully downloaded file segment");

    if (downloader_id != getCallerId())
        throw Exception(ErrorCodes::FILE_CACHE_ERROR, "Space can be reserved only by downloader");

    assert(reserved_size >= downloaded_size);

    std::lock_guard cache_lock(cache->mutex);

    /**
     * It is possible to have downloaded_size < reserved_size when reserve is called
     * in case previous downloader did not fully download current file_segment
     * and the caller is going to continue;
     */
    size_t free_space = reserved_size - downloaded_size;
    size_t size_to_reserve = size - free_space;

    bool reserved;
    if (downloaded_size)
        reserved = cache->tryReserve(size_to_reserve, cache_lock);
    else
        reserved = cache->set(key(), range().left, size_to_reserve, segment_lock, cache_lock);

    if (reserved)
        reserved_size += size;
    else
        download_state = downloaded_size
            ? State::PARTIALLY_DOWNLOADED_NO_CONTINUATION
            : State::SKIP_CACHE;

    return reserved;
}

void FileSegment::complete()
{
    {
        std::lock_guard segment_lock(mutex);

        /// TODO: There is a gap between next thread will call getOrSetDownlaoder and no one will remove the cell during this gap.

        if (downloader_id != getCallerId())
            throw Exception(ErrorCodes::FILE_CACHE_ERROR,
                            "File segment can be completed only by downloader or downloader's FileSegmentsHodler");

        downloader_id.clear();
        download_buffer.reset();
        reserved_size = downloaded_size;

        switch (download_state)
        {
            case State::EMPTY:
            case State::DOWNLOADED:
            {
                /// Download not even started or already completed successfully.
                break;
            }
            case State::SKIP_CACHE:
            {
                /// Nothing has been downloaded as first space reservation failed.
                assert(!downloaded_size);

                std::lock_guard cache_lock(cache->mutex);
                cache->remove(key(), range().left, cache_lock);

                break;
            }
            case State::DOWNLOADING:
            case State::PARTIALLY_DOWNLOADED:
            case State::PARTIALLY_DOWNLOADED_NO_CONTINUATION:
            {
                if (downloaded_size == range().size())
                {
                    std::lock_guard cache_lock(cache->mutex);
                    download_state = State::DOWNLOADED;
                }
                else
                {
                    /**
                    * If file segment's downloader did not fully download the range,
                    * check if there is some other thread holding the same file segment.
                    * If there is any - download can be continued.
                    */

                    std::lock_guard cache_lock(cache->mutex);

                    size_t users_count = cache->getUseCount(*this, cache_lock);
                    assert(users_count >= 1);

                    if (users_count == 1)
                    {
                        if (downloaded_size > 0)
                        {
                            segment_range.right = segment_range.left + downloaded_size - 1;
                            download_state = State::PARTIALLY_DOWNLOADED;
                        }
                        else
                        {
                            cache->remove(key(), range().left, cache_lock);
                            download_state = State::SKIP_CACHE;
                        }
                    }
                    else
                        download_state = State::PARTIALLY_DOWNLOADED;
                }
            }
        }
    }

    cv.notify_all();
}

String FileSegment::toString(FileSegment::State state)
{
    switch (state)
    {
        case FileSegment::State::DOWNLOADED:
            return "DOWNLOADED";
        case FileSegment::State::EMPTY:
            return "EMPTY";
        case FileSegment::State::DOWNLOADING:
            return "DOWNLOADING";
        case FileSegment::State::PARTIALLY_DOWNLOADED:
            return "PARTIALLY DOWNLOADED";
        case FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION:
            return "PARTIALLY DOWNLOADED NO CONTINUATION";
        case FileSegment::State::SKIP_CACHE:
            return "SKIP_CACHE";
    }
}

}
