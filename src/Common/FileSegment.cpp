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
        const Key & key_,
        FileCache * cache_,
        State download_state_)
    : segment_range(offset_, offset_ + size_ - 1)
    , download_state(download_state_)
    , file_key(key_)
    , cache(cache_)
{
    if (download_state == State::DOWNLOADED)
        reserved_size = downloaded_size = size_;
    else if (download_state != State::EMPTY)
        throw Exception(ErrorCodes::FILE_CACHE_ERROR, "Can create cell with either DOWNLOADED or EMPTY state");
}

FileSegment::State FileSegment::state() const
{
    std::lock_guard segment_lock(mutex);
    return download_state;
}

size_t FileSegment::downloadOffset() const
{
    std::lock_guard segment_lock(mutex);
    return range().left + downloaded_size - 1;
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
    if (!size)
        throw Exception(ErrorCodes::FILE_CACHE_ERROR, "Writing zero size is not allowed");

    if (available() < size)
        throw Exception(
            ErrorCodes::FILE_CACHE_ERROR,
            "Not enough space is reserved. Available: {}, expected: {}", available(), size);

    if (!isDownloader())
        throw Exception(ErrorCodes::FILE_CACHE_ERROR, "Only downloader can do the downloading");

    if (!download_buffer)
    {
        auto download_path = cache->path(key(), offset());
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

    if (downloaded_size + size > range().size())
        throw Exception(ErrorCodes::FILE_CACHE_ERROR,
                        "Attempt to reserve space too much space ({}) for file segment with range: {} (downloaded size: {})",
                        size, range().toString(), downloaded_size);

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

    bool reserved = cache->tryReserve(key(), offset(), size_to_reserve, cache_lock);

    if (reserved)
        reserved_size += size;

    return reserved;
}

void FileSegment::complete(std::optional<State> state)
{
    /**
     * Either downloader calls file_segment->complete(state) manually or
     * file_segment->complete() is called with no state from its FileSegmentsHolder desctructor.
     *
     * Downloader can call complete(state) with either DOWNLOADED or
     * PARTIALLY_DOWNLOADED_NO_CONTINUATION (in case space reservation failed).
     *
     * If complete() is called from FileSegmentsHolder desctructor -- actions are taken
     * according to current download_state and only in case `detached==false`, meaning than
     * this filesegment is present in cache cell. If file segment was removed from cache cell,
     * it has `detached=true`, so that other threads will know that no clean up is required from them.
     */

    {
        std::lock_guard segment_lock(mutex);

        bool download_can_continue = false;
        bool is_downloader = downloader_id == getCallerId();

        if (state)
        {
            if (!is_downloader)
                throw Exception(ErrorCodes::FILE_CACHE_ERROR,
                                "File segment can be completed only by downloader or downloader's FileSegmentsHodler");

            if (*state != State::DOWNLOADED && *state != State::PARTIALLY_DOWNLOADED_NO_CONTINUATION)
                throw Exception(ErrorCodes::FILE_CACHE_ERROR,
                                "Cannot complete file segment with state: {}", toString(*state));

            download_state = *state;
            if (download_state == State::PARTIALLY_DOWNLOADED_NO_CONTINUATION)
            {
                std::lock_guard cache_lock(cache->mutex);

                if (downloaded_size)
                    cache->reduceSizeToDownloaded(key(), offset(), cache_lock);
                else
                    cache->remove(key(), offset(), cache_lock);
            }
        }
        else if (!detached)
        {
            if (downloaded_size == range().size())
                download_state = State::DOWNLOADED;

            if (download_state == State::DOWNLOADING)
                download_state = State::PARTIALLY_DOWNLOADED;

            if (download_state == State::PARTIALLY_DOWNLOADED
                     || download_state == State::PARTIALLY_DOWNLOADED_NO_CONTINUATION)
            {
                std::lock_guard cache_lock(cache->mutex);

                bool is_last_holder = cache->isLastFileSegmentHolder(key(), offset(), cache_lock);
                download_can_continue = !is_last_holder && download_state == State::PARTIALLY_DOWNLOADED;

                if (!download_can_continue)
                {
                    bool is_responsible_for_cell = is_downloader || (downloader_id.empty() && is_last_holder);
                    if (is_responsible_for_cell)
                    {
                        if (downloaded_size)
                            cache->reduceSizeToDownloaded(key(), offset(), cache_lock);
                        else
                            cache->remove(key(), offset(), cache_lock);
                    }
                }
            }
        }

        if (is_downloader)
            downloader_id.clear();

        if (!download_can_continue)
            download_buffer.reset();
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
