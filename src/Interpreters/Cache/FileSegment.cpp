#include "FileSegment.h"

#include <base/getThreadId.h>
#include <Common/scope_guard_safe.h>
#include <Common/hex.h>
#include <Common/logger_useful.h>
#include <Interpreters/Cache/FileCache.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <filesystem>

namespace CurrentMetrics
{
extern const Metric CacheDetachedFileSegments;
}

namespace DB
{

/// A thread local variable to identify background download threads.
/// Only background download threads will make this value non-empty.
thread_local std::string background_caller_id;

namespace ErrorCodes
{
    extern const int REMOTE_FS_OBJECT_CACHE_ERROR;
    extern const int LOGICAL_ERROR;
}

FileSegment::FileSegment(
        size_t offset_,
        size_t size_,
        const Key & key_,
        FileCache * cache_,
        State download_state_,
        const CreateFileSegmentSettings & settings)
    : segment_range(offset_, offset_ + size_ - 1)
    , download_state(download_state_)
    , file_key(key_)
    , cache(cache_)
#ifndef NDEBUG
    , log(&Poco::Logger::get(fmt::format("FileSegment({}) : {}", getHexUIntLowercase(key_), range().toString())))
#else
    , log(&Poco::Logger::get("FileSegment"))
#endif
    , is_persistent(settings.is_persistent)
{
    /// On creation, file segment state can be EMPTY, DOWNLOADED, DOWNLOADING.
    switch (download_state)
    {
        /// EMPTY is used when file segment is not in cache and
        /// someone will _potentially_ want to download it (after calling getOrSetDownloader()).
        case (State::EMPTY):
        {
            break;
        }
        /// DOWNLOADED is used either on initial cache metadata load into memory on server startup
        /// or on reduceSizeToDownloaded() -- when file segment object is updated.
        case (State::DOWNLOADED):
        {
            reserved_size = downloaded_size = size_;
            is_downloaded = true;
            break;
        }
        case (State::SKIP_CACHE):
        {
            break;
        }
        default:
        {
            throw Exception(
                ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
                "Can create cell with either EMPTY, DOWNLOADED, DOWNLOADING state");
        }
    }

    if (settings.is_async_download)
        background_download = std::make_unique<BackgroundDownload>(cache);
}

String FileSegment::getPathInLocalCache() const
{
    return cache->getPathInLocalCache(key(), offset(), isPersistent());
}

FileSegment::State FileSegment::state() const
{
    std::unique_lock segment_lock(mutex);
    return download_state;
}

bool FileSegment::isInternal()
{
    /// Is this method called by background download thread?
    return !background_caller_id.empty();
}

void FileSegment::setDownloadState(State state)
{
    std::unique_lock segment_lock(mutex);
    return setDownloadStateUnlocked(state, segment_lock);
}

void FileSegment::setDownloadStateUnlocked(State state, std::unique_lock<std::mutex> & /* segment_lock */)
{
    chassert(!isInternal() || downloader_id.empty());
    LOG_TEST(log, "Updated state from {} to {}", stateToString(download_state), stateToString(state));
    download_state = state;
}

size_t FileSegment::getFirstNonDownloadedOffset() const
{
    std::unique_lock segment_lock(mutex);
    return getFirstNonDownloadedOffsetUnlocked(segment_lock);
}

size_t FileSegment::getFirstNonDownloadedOffsetUnlocked(std::unique_lock<std::mutex> & segment_lock) const
{
    return range().left + getDownloadedSizeUnlocked(segment_lock);
}

size_t FileSegment::getCurrentWriteOffset() const
{
    std::unique_lock segment_lock(mutex);
    return getCurrentWriteOffsetUnlocked(segment_lock);
}

size_t FileSegment::getCurrentWriteOffsetUnlocked(std::unique_lock<std::mutex> & segment_lock) const
{
    /// In case of synchronous cache writing getCurrentWriteOffset() == getFirstNonDownloadedOffset().
    /// In case of asynchronous cache writing getCurrentWriteOffset() >= getFirstNonDownloadedOffset().
    if (background_download)
    {
        /// Get offset as it would be if background download queue was finished.
        return range().left + background_download->getFutureDownloadedSize(segment_lock);
    }

    return getFirstNonDownloadedOffsetUnlocked(segment_lock);
}

size_t FileSegment::getDownloadedSize() const
{
    std::unique_lock segment_lock(mutex);
    return getDownloadedSizeUnlocked(segment_lock);
}

size_t FileSegment::getDownloadedSizeUnlocked(std::unique_lock<std::mutex> & /* segment_lock */) const
{
    if (download_state == State::DOWNLOADED)
        return downloaded_size;

    std::unique_lock download_lock(download_mutex);
    return downloaded_size;
}

bool FileSegment::isDownloaded() const
{
    std::lock_guard segment_lock(mutex);
    return is_downloaded;
}

String FileSegment::getCallerId()
{
    if (isInternal())
        return background_caller_id;

    if (!CurrentThread::isInitialized()
        || !CurrentThread::get().getQueryContext()
        || CurrentThread::getQueryId().empty())
        return "None:" + toString(getThreadId());

    return std::string(CurrentThread::getQueryId()) + ":" + toString(getThreadId());
}

String FileSegment::getDownloader() const
{
    std::unique_lock segment_lock(mutex);
    return getDownloaderUnlocked(segment_lock);
}

String FileSegment::getDownloaderUnlocked(std::unique_lock<std::mutex> & /* segment_lock */) const
{
    return isInternal() ? background_downloader_id : downloader_id;
}

String FileSegment::getOrSetDownloader()
{
    std::unique_lock segment_lock(mutex);

    assertNotDetachedUnlocked(segment_lock);

    auto current_downloader = getDownloaderUnlocked(segment_lock);

    if (current_downloader.empty())
    {
        bool allow_new_downloader = download_state == State::EMPTY || download_state == State::PARTIALLY_DOWNLOADED;

        if (allow_new_downloader && background_download && isBackgroundDownloadFailedOrCancelledUnlocked(segment_lock))
        {
            setDownloadStateUnlocked(State::PARTIALLY_DOWNLOADED_NO_CONTINUATION, segment_lock);
            allow_new_downloader = false;
        }

        if (!allow_new_downloader)
            return "notAllowed:" + stateToString(download_state);

        current_downloader = downloader_id = getCallerId();
        setDownloadStateUnlocked(State::DOWNLOADING, segment_lock);
    }

    return current_downloader;
}

void FileSegment::resetDownloadingStateUnlocked([[maybe_unused]] std::unique_lock<std::mutex> & segment_lock)
{
    assert(isDownloaderUnlocked(segment_lock));
    assert(download_state == State::DOWNLOADING);

    size_t current_downloaded_size = getDownloadedSizeUnlocked(segment_lock);
    /// range().size() can equal 0 in case of write-though cache.
    if (current_downloaded_size != 0 && current_downloaded_size == range().size())
        setDownloadedUnlocked(segment_lock);
    else
        setDownloadStateUnlocked(State::PARTIALLY_DOWNLOADED, segment_lock);
}

void FileSegment::resetDownloader()
{
    std::unique_lock segment_lock(mutex);

    assertNotDetachedUnlocked(segment_lock);
    assertIsDownloaderUnlocked("resetDownloader", segment_lock);

    assert(download_state == State::DOWNLOADING || download_state == State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);
    if (download_state == State::DOWNLOADING)
        resetDownloadingStateUnlocked(segment_lock);

    resetDownloaderUnlocked(segment_lock);
}

void FileSegment::resetDownloaderUnlocked(std::unique_lock<std::mutex> & /* segment_lock */)
{
    chassert(!isInternal());
    LOG_TEST(log, "Resetting downloader from {}", downloader_id);
    downloader_id.clear();
}

void FileSegment::assertIsDownloaderUnlocked(const std::string & operation, std::unique_lock<std::mutex> & segment_lock) const
{
    auto caller = getCallerId();
    auto current_downloader = getDownloaderUnlocked(segment_lock);
    LOG_TEST(log, "Downloader id: {}, caller id: {}", current_downloader, caller);

    if (caller != current_downloader)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Operation `{}` can be done only by downloader. ({})",
            operation, getInfoForLogUnlocked(segment_lock));
    }
}

bool FileSegment::isBackgroundDownloader(std::unique_lock<std::mutex> & /* segment_lock */) const
{
    if (!background_download)
        return false;

    return getCallerId() == background_downloader_id;
}

bool FileSegment::isDownloader() const
{
    std::unique_lock segment_lock(mutex);
    return isDownloaderUnlocked(segment_lock);
}

bool FileSegment::isDownloaderUnlocked(std::unique_lock<std::mutex> & segment_lock) const
{
    return getCallerId() == getDownloaderUnlocked(segment_lock);
}

FileSegment::RemoteFileReaderPtr FileSegment::getRemoteFileReader()
{
    std::unique_lock segment_lock(mutex);
    assertIsDownloaderUnlocked("getRemoteFileReader", segment_lock);
    return remote_file_reader;
}

FileSegment::RemoteFileReaderPtr FileSegment::extractRemoteFileReader()
{
    std::lock_guard cache_lock(cache->mutex);
    std::unique_lock segment_lock(mutex);

    if (!is_detached)
    {
        bool is_last_holder = cache->isLastFileSegmentHolder(key(), offset(), cache_lock, segment_lock);
        if (!downloader_id.empty() || !is_last_holder)
            return nullptr;
    }

    LOG_TRACE(log, "Extracted reader from file segment");
    return std::move(remote_file_reader);
}

void FileSegment::setRemoteFileReader(RemoteFileReaderPtr remote_file_reader_)
{
    std::unique_lock segment_lock(mutex);
    assertIsDownloaderUnlocked("setRemoteFileReader", segment_lock);

    if (remote_file_reader)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Remote file reader already exists");

    remote_file_reader = remote_file_reader_;
}

void FileSegment::resetRemoteFileReader()
{
    std::unique_lock segment_lock(mutex);
    assertIsDownloaderUnlocked("resetRemoteFileReader", segment_lock);

    if (!remote_file_reader)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Remote file reader does not exist");

    remote_file_reader.reset();
}

void FileSegment::write(const char * from, size_t size, size_t offset)
{
    try
    {
        if (background_download)
            asynchronousWrite(from, size, offset);
        else
            synchronousWrite(from, size, offset);
    }
    catch (...)
    {
        setDownloadState(State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);
        cv.notify_all();
        throw;
    }
}

void FileSegment::assertAsyncWriteStateInitialized() const
{
    if (!background_download)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no write state for background download");
}

bool FileSegment::isBackgroundDownloadFailedOrCancelled() const
{
    std::unique_lock segment_lock(mutex);
    return isBackgroundDownloadFailedOrCancelledUnlocked(segment_lock);
}

bool FileSegment::isBackgroundDownloadFailedOrCancelledUnlocked(std::unique_lock<std::mutex> & /* segment_lock */) const
{
    if (!background_download)
        return false;

    auto & state = *background_download;
    return state.exception || state.is_cancelled;
}

void FileSegment::waitBackgroundDownloadIfExists(size_t offset, size_t max_wait_seconds) const
{
    std::optional<std::shared_future<void>> shared_future;

    {
        std::unique_lock segment_lock(mutex);

        if (!background_download)
            return;

        const auto & state = *background_download;

        if (state.exception)
        {
#ifdef NDEBOG
            auto message = getExceptionMessage(background_download->exception, false);
            LOG_DEBUG(log, "Exception happened on background download: {}, will skip cache", message);
            return;
#else
            std::rethrow_exception(background_download->exception);
#endif
        }

        if (state.is_cancelled)
        {
            LOG_TRACE(log, "Background download was cancelled");
            return;
        }

        const auto & currently_downloading = state.currently_downloading;

        if (currently_downloading.empty())
            return;

#ifndef NDEBUG
        String currently_downloading_str;
        for (const auto & [download_offset, result] : currently_downloading)
        {
            if (!currently_downloading_str.empty())
                currently_downloading_str += ", ";
            currently_downloading_str += fmt::format("[{}:{}]", download_offset, download_offset + result.expected_size - 1);
        }

        LOG_TEST(
            log,
            "Requested offset: {}, background download ranges: {} ({})",
            offset, currently_downloading_str, getInfoForLogUnlocked(segment_lock));
#endif

        /// Get offset which corresponds to the first byte for which
        /// there is no in memory buffer in the background download queue:
        ///
        /// [___][___] ... [___] -- buffers in the background download queue
        ///  b1    b2       bn  ^
        ///                     current_write_offset
        const size_t current_write_offset = getCurrentWriteOffsetUnlocked(segment_lock);

        if (offset > current_write_offset)
        {
            /// ... [______] -- queue of background download buffers
            ///       bn
            ///                    ^
            ///                    offset
            ///             ^
            ///             current_write_offset

            /// There is no data starting from `offset` which is waiting
            /// to be downloaded by the background thread.
            LOG_TEST(log, "current write offset < offset");
            return;
        }

        size_t first_non_downloaded_offset = currently_downloading.begin()->first;

        if (offset < first_non_downloaded_offset)
        {
            ///             [______]
            ///                b1
            ///   ^         ^
            ///   offset    first_non_downloaded_offset

            /// Data containing `offset` is already downloaded.
            LOG_TEST(log, "offset < first_non_downloaded_offset");
            return;
        }

        auto it = std::lower_bound(
            currently_downloading.begin(),
            currently_downloading.end(),
            offset,
            [](const auto & map, size_t value) { return map.first < value; });

        /// [___][___] ... [___]
        ///  b1    b2       bn  ^
        ///
        ///  At this point we have the following invariant:
        ///  b1.offset <= offset <= it.offset <= bn.end

        chassert(!currently_downloading.empty());
        chassert(currently_downloading.begin()->first <= offset);

        if (it == currently_downloading.end())
        {
            ///  [______] -- bn
            ///     ^
            ///     offset
           it = std::prev(currently_downloading.end());
        }
        else
        {
            chassert(offset <= it->first);

            if (offset < it->first)
            {
                /// [______________][_________]
                ///                     it
                ///        ^         ^
                ///        offset    it.offset
                it = std::prev(it);
            }
        }

        shared_future = it->second.shared_future;
    }

    LOG_DEBUG(log, "Waiting for buffer at offset {} to be downloaded", offset);

    auto wait_result = shared_future->wait_for(std::chrono::seconds(max_wait_seconds));
    if (wait_result != std::future_status::ready)
    {
        LOG_DEBUG(
            log,
            "Timeout ({} sec) for waiting background download exceeded, will continue without cache",
            max_wait_seconds);
    }

    LOG_DEBUG(log, "Waiting for buffer at offset {} to be downloaded is finished", offset);
}

void FileSegment::cancelBackgroundDownloadIfExists(std::unique_lock<std::mutex> & /* segment_lock */)
{
    /// Background download might be cancelled in case file segment was detached
    /// (e.g. removed from cache, etc see detach() method comment ).
    /// In this case we need to wait for all the current tasks to finish.

    if (!background_download)
        return;

    auto & state = *background_download;

    state.is_cancelled = true;
    state.currently_downloading = {};

    /// We do not need to wait for background tasks here. Background download will fail
    /// with exception after this moment: each state change is done under file segment lock,
    /// so state.is_cancelled will be visible.
}

void FileSegment::asynchronousWrite(const char * from, size_t size, size_t offset)
{
    if (!size)
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "Writing zero size is not allowed");

    {
        std::unique_lock segment_lock(mutex);

        assertAsyncWriteStateInitialized();
        assertIsDownloaderUnlocked("write", segment_lock);
        assertNotDetachedUnlocked(segment_lock);

        if (!background_download->reserved_buffer)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Space was not reserved for background download");
        }

        size_t current_downloaded_size = getDownloadedSizeUnlocked(segment_lock);
        chassert(reserved_size >= current_downloaded_size);
        size_t free_reserved_size = reserved_size - current_downloaded_size;

        if (free_reserved_size < size)
            throw Exception(
                ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
                "Not enough space is reserved. Available: {}, expected: {}", free_reserved_size, size);
    }

    SCOPE_EXIT({
        std::unique_lock segment_lock(mutex);
        background_download->reserved_buffer.reset();
    });

    /// When we need to pass some data buffer, which need to be written to cache
    /// in the background, we create a threadpool task. But we need to note that
    /// each such buffer need to be written in direct order one after another within
    /// the same file segment, e.g. we can only write into file segments sequentially.
    /// Therefore the first task to set background_downloader becomes the executor and other
    /// tasks will become noop in case executor is set. On the other hand, we might
    /// add new buffers for write after previous executor reached the end of state.buffers,
    /// so instead of being noop, some of such tasks becomes a new executor.

    /// Adding new buffers, changing background_downloader_id and pulling buffers from
    /// state.buffers (by executor) is done under the same mutex, which allows to have an invariant:
    /// as long as !state.buffers.empty() (&& !state.exception) there should be a task
    /// which will set background_downloader_id and write those buffers to cache.

    memcpy(background_download->reserved_buffer->data(), from, size);

    std::unique_lock segment_lock(mutex);

    assertNotDetachedUnlocked(segment_lock);

    auto holder = std::make_unique<FileSegmentsHolder>();
    {
        auto & state = *background_download;

        /// If there was an exception on a previous attempt to write data - rethrow it.
        if (state.exception)
            std::rethrow_exception(state.exception);

        LOG_TEST(log, "Current background download state has {} buffers to be written", state.buffers.size());

        if (state.last_added_buffer_range)
        {
            auto & [prev_offset, prev_size] = *state.last_added_buffer_range;
            size_t expected_offset = prev_offset + prev_size;

            if (offset != expected_offset)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Attempt to add buffer with offset {}, but expected {} (previous offset: {}, previous size: {})",
                    offset, expected_offset, prev_offset, prev_size);
            }
        }

        auto buffer = std::move(state.reserved_buffer);
        buffer->offset = offset;

        state.buffers.emplace_back(std::move(buffer));

        state.last_added_buffer_range = {offset, size};
        state.future_downloaded_size += size;

         /// This holder is moved to `task` - protection against segfault: file segment object must
         /// exist while `task` is executed. Note: shared_from_this() must be done under segment_lock.
         /// It is also important to wrap in FileSegmentHolder as
         /// need to unsure complete() is called by each user of the file segment.
        holder->file_segments.push_back(shared_from_this());
    }

    ThreadGroupStatusPtr thread_group = CurrentThread::isInitialized() && CurrentThread::get().getThreadGroup()
            ? CurrentThread::get().getThreadGroup()
            : MainThreadStatus::getInstance().getThreadGroup();

    auto task = std::make_shared<std::packaged_task<void()>>(
        [this,
         executor_id = getCallerId() + "_async", /// Id for background downloader
         holder = std::move(holder),
         thread_group]()
    {
        if (thread_group)
            CurrentThread::attachTo(thread_group);

        SCOPE_EXIT_SAFE({
            if (thread_group)
                CurrentThread::detachQueryIfNotDetached();
        });

        auto & state = *background_download;
        const auto & file_segment = holder->file_segments.front();

        {
            std::unique_lock lock(file_segment->mutex);

            if (state.is_cancelled)
                return;

            /// If there was an exception on writing previous block of data, do not attempt
            /// to write later block. Once state.exception is set, each next attempt to add
            /// one more block into state.buffers will fail with that exception.
            if (state.exception)
                return;

            if (state.buffers.empty())
                return;

            /// If !background_downloader.empty(), state.buffers is currently used by another
            /// execution task, so we do not need to execute anything as this
            /// executing task will continue with all state.buffers.
            if (!background_downloader_id.empty())
                return;

            background_downloader_id = background_caller_id = executor_id;

            LOG_TEST(log, "Assigned background downloader: {}", executor_id);
        }

        SCOPE_EXIT({
            chassert(background_downloader_id != executor_id);
        });

        std::optional<size_t> start_offset;
        size_t total_size = 0;

        try
        {
            while (true)
            {
                std::unique_lock lock(file_segment->mutex);

                if (state.is_cancelled)
                {
                    chassert(background_download->currently_downloading.empty());
                    background_downloader_id.clear();

                    return;
                }

                if (state.currently_executing_range)
                {
                    auto erased = state.currently_downloading.erase(state.currently_executing_range->offset);
                    if (erased == 0)
                    {
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR,
                            "There is no offset {} in currently downloading list",
                            state.currently_executing_range->offset);
                    }

                    LOG_TEST(log, "Removed offset {} from currently downloading", state.currently_executing_range->offset);

                    state.currently_executing_range.reset();
                }
                else
                {
                    chassert(!state.buffers.empty());
                    chassert(!state.exception);
                }

                if (state.buffers.empty())
                {
                    LOG_TEST(log, "No buffers left, will reset downloader {}", background_downloader_id);

                    /// Resetting background_downloader must be done under state lock
                    /// along with the check state.buffers.empty()

                    completePartAndResetDownloaderUnlocked(lock);
                    background_downloader_id.clear();

                    break;
                }

                auto buffer_ptr = std::move(state.buffers.front());
                state.buffers.pop_front();

                auto & buffer = *buffer_ptr;

                if (!start_offset)
                    start_offset = buffer.offset;

                lock.unlock();

                LOG_DEBUG(log, "Background download: [{}:{})", buffer.offset, buffer.offset + buffer.size());

                synchronousWrite(buffer.data(), buffer.size(), buffer.offset);

                state.currently_executing_range = {buffer.offset, buffer.size()};
                total_size += buffer.size();
            }
        }
        catch (...)
        {
            std::unique_lock lock(file_segment->mutex);

            bool is_cancelled = state.is_cancelled;
            if (!is_cancelled)
                tryLogCurrentException(__PRETTY_FUNCTION__);

            try
            {
                if (!state.exception)
                    state.exception = std::current_exception();

                state.currently_downloading.clear();
                state.buffers.clear(); /// Clear all hold memory.

                if (!is_cancelled)
                    completePartAndResetDownloaderUnlocked(lock);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }

            background_downloader_id.clear();
        }

        if (start_offset)
            LOG_TEST(log, "Completed background download of file segment at offset: {}, size: {}", *start_offset, total_size);

        /// Notify that some part was written.
        /// This is needed to let other threads fall back into
        /// "can_start_from_cache" case:
        ///                      segment{k}
        /// cache:           [______|___________
        ///                         ^
        ///                         current_write_offset
        /// requested_range:    [__________]
        ///                     ^
        ///                     wait_offset
        cv.notify_all();
    });

    BackgroundDownload::BackgroundDownloadResult result(task->get_future(), size);
    auto [_, inserted] = background_download->currently_downloading.emplace(offset, result);
    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Offset {} is already marked as downloading", offset);

    segment_lock.unlock();

    try
    {
        cache->getThreadPoolForAsyncWrite().scheduleOrThrow([task = std::move(task)]{ (*task)(); });
    }
    catch (...)
    {
        std::unique_lock lock(mutex);
        [[maybe_unused]] bool erased = background_download->currently_downloading.erase(offset);
        chassert(erased);

        throw;
    }

    LOG_TEST(log, "Submitted task for background download for offset: {}", offset);
}

void FileSegment::synchronousWrite(const char * from, size_t size, size_t offset)
{
    if (!size)
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "Writing zero size is not allowed");

    {
        std::unique_lock segment_lock(mutex);

        if (downloaded_size == 0)
            stat.download_start_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

        if (background_download && background_download->is_cancelled)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Background download was cancelled");

        assertIsDownloaderUnlocked("write", segment_lock);
        assertNotDetachedUnlocked(segment_lock);

        if (!isInternal() && download_state != State::DOWNLOADING)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected DOWNLOADING state, got {}", stateToString(download_state));

        size_t first_non_downloaded_offset = getFirstNonDownloadedOffsetUnlocked(segment_lock);
        if (offset != first_non_downloaded_offset)
            throw Exception(
                ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
                "Attempt to write {} bytes to offset: {}, but current write offset is {}",
                size, offset, first_non_downloaded_offset);

        size_t current_downloaded_size = getDownloadedSizeUnlocked(segment_lock);
        chassert(reserved_size >= current_downloaded_size);
        size_t free_reserved_size = reserved_size - current_downloaded_size;

        if (free_reserved_size < size)
            throw Exception(
                ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
                "Not enough space is reserved. Available: {}, expected: {}", free_reserved_size, size);

        if (current_downloaded_size == range().size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "File segment is already fully downloaded");

        if (!cache_writer)
        {
            if (current_downloaded_size > 0)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cache writer was finalized (downloaded size: {}, state: {})",
                    current_downloaded_size, stateToString(download_state));

            auto download_path = getPathInLocalCache();
            cache_writer = std::make_unique<WriteBufferFromFile>(download_path);
        }
    }

    cache_writer->write(from, size);

    {
        std::lock_guard download_lock(download_mutex);
        cache_writer->next();
        downloaded_size += size;
    }

    if (downloaded_size == range().size())
    {
        std::lock_guard segment_lock(mutex);
        stat.download_end_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    }

#ifndef NDEBUG
    chassert(getFirstNonDownloadedOffset() == offset + size);
#endif
}

FileSegment::Stat FileSegment::getStat() const
{
    std::unique_lock segment_lock(mutex);
    return getStatUnlocked(segment_lock);
}

FileSegment::Stat FileSegment::getStatUnlocked(std::unique_lock<std::mutex> & /* segment_lock */) const
{
    return stat;
}

FileSegment::State FileSegment::wait()
{
    std::unique_lock segment_lock(mutex);

    if (is_detached)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cache file segment is in detached state, operation not allowed");

    if (downloader_id.empty())
        return download_state;

    if (download_state == State::EMPTY)
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "Cannot wait on a file segment with empty state");

    if (download_state == State::DOWNLOADING)
    {
        LOG_TEST(log, "{} waiting on: {}, current downloader: {}", getCallerId(), range().toString(), downloader_id);

        chassert(!getDownloaderUnlocked(segment_lock).empty());
        chassert(!isDownloaderUnlocked(segment_lock));

        cv.wait_for(segment_lock, std::chrono::seconds(60));
    }

    return download_state;
}

bool FileSegment::BackgroundDownload::reserve(size_t size, std::unique_lock<std::mutex> & /* segment_lock */)
{
    auto reservation = cache->tryReserveForBackgroundDownload(size);
    if (!reservation)
        return false;

    try
    {
        reserved_buffer = std::make_unique<BackgroundDownload::Buffer>(std::move(reservation));
    }
    catch (...)
    {
        /// catch only MEMORY_LIMIT_EXCEEDED?
#ifdef NDEBUG
        tryLogCurrentException(__PRETTY_FUNCTION__);
        return false;
#else
        throw;
#endif
    }

    return true;
}

FileSegment::BackgroundDownload::Buffer::Buffer(BackgroundDownloadReservationPtr reservation_)
    : memory(reservation_->size), buf_size(reservation_->size), reservation(std::move(reservation_))
{
}

std::optional<FileSegment::Range>
FileSegment::BackgroundDownload::getCurrentlyDownloadingRange(std::unique_lock<std::mutex> & /* segment_lock */) const
{
    if (!currently_executing_range)
        return std::nullopt;

    const auto & range = *currently_executing_range;
    return FileSegment::Range(range.offset, range.offset + range.size - 1);
}

FileSegment::BackgroundDownload::Ranges
FileSegment::BackgroundDownload::getDownloadQueueRanges(std::unique_lock<std::mutex> & /* segment_lock */) const
{
    Ranges result;
    for (const auto & buffer : buffers)
        result.emplace_back(buffer->offset, buffer->offset + buffer->size() - 1);
    return result;
}

bool FileSegment::reserve(size_t size_to_reserve)
{
    if (!size_to_reserve)
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "Zero space reservation is not allowed");

    size_t expected_downloaded_size;

    {
        std::unique_lock segment_lock(mutex);

        assertNotDetachedUnlocked(segment_lock);
        assertIsDownloaderUnlocked("reserve", segment_lock);

        if (background_download)
        {
            /// There is a limit for the memory usage kept by in-memory buffers of data
            /// which is waiting to be written to disk. If we reach this limit, discard the downloads.

            bool reserved = background_download->reserve(size_to_reserve, segment_lock);
            if (!reserved)
            {
                setDownloadStateUnlocked(State::PARTIALLY_DOWNLOADED_NO_CONTINUATION, segment_lock);
                return false;
            }
        }

        expected_downloaded_size = background_download
            ? background_download->getFutureDownloadedSize(segment_lock)
            : getDownloadedSizeUnlocked(segment_lock);

        if (expected_downloaded_size + size_to_reserve > range().size())
            throw Exception(
                ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
                "Attempt to reserve space too much space ({}) for file segment with range: {} (downloaded size: {})",
                size_to_reserve, range().toString(), downloaded_size);

        chassert(reserved_size >= expected_downloaded_size);
    }

    /**
     * It is possible to have downloaded_size < reserved_size when reserve is called
     * in case previous downloader did not fully download current file_segment
     * and the caller is going to continue;
     */

    size_t already_reserved_size = reserved_size - expected_downloaded_size;

    bool reserved = already_reserved_size >= size_to_reserve;
    if (!reserved)
    {
        std::lock_guard cache_lock(cache->mutex);

        size_to_reserve = size_to_reserve - already_reserved_size;
        reserved = cache->tryReserve(key(), offset(), size_to_reserve, cache_lock);

        std::unique_lock segment_lock(mutex);
        if (reserved)
            reserved_size += size_to_reserve;
        else
            setDownloadStateUnlocked(State::PARTIALLY_DOWNLOADED_NO_CONTINUATION, segment_lock);
    }

    return reserved;
}

void FileSegment::setDownloadedUnlocked([[maybe_unused]] std::unique_lock<std::mutex> & segment_lock)
{
    if (is_downloaded)
        return;

    setDownloadStateUnlocked(State::DOWNLOADED, segment_lock);
    is_downloaded = true;

    if (cache_writer)
    {
        cache_writer->finalize();
        cache_writer.reset();
        remote_file_reader.reset();
    }

    download_state = State::DOWNLOADED;
    is_downloaded = true;

    assert(getDownloadedSizeUnlocked(segment_lock) > 0);
    assert(std::filesystem::file_size(getPathInLocalCache()) > 0);
}

void FileSegment::completePartAndResetDownloader()
{
    std::unique_lock segment_lock(mutex);
    completePartAndResetDownloaderUnlocked(segment_lock);
}

void FileSegment::completePartAndResetDownloaderUnlocked(std::unique_lock<std::mutex> & segment_lock)
{
    assertNotDetachedUnlocked(segment_lock);
    assertIsDownloaderUnlocked("completePartAndResetDownloader", segment_lock);

    /// With background download we have two "downloaders". The first one is from the main thread,
    /// which calls getOrSetDownloader and executed the logic in CachedFromDiskReadBufferFromFile
    /// (identified by downloader_id). The second one is the background downloader who executes
    /// background download (identified by background_downloader_id). There is an invariant that
    /// download_state can be changed only by main downloader if downloader_id is non-empty, e.g.
    /// there is an active main downloader. Nevertheless, if downloader_id is empty, background
    /// downloader can and should change the download state. The state is always finalized because
    /// some of them will be the last to call completePartAndResetDownloader() and this file segment
    /// will be completed with the final state.
    if (isInternal())
    {
        if (downloader_id.empty())
        {
            LOG_TEST(log, "Setting DOWNLOADED state by background downloader");
            size_t current_downloaded_size = getDownloadedSizeUnlocked(segment_lock);
            if (current_downloaded_size != 0 && current_downloaded_size == range().size())
                setDownloadedUnlocked(segment_lock);
        }
        else
        {
            return;
        }
    }
    else
    {
        assert(download_state == State::DOWNLOADING || download_state == State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);

        if (download_state == State::DOWNLOADING)
            resetDownloadingStateUnlocked(segment_lock);

        resetDownloaderUnlocked(segment_lock);
    }

    LOG_TEST(log, "Complete batch. (is_internal: {}, {})", isInternal(), getInfoForLogUnlocked(segment_lock));

    cv.notify_all();
}

FileSegment::Stat FileSegment::complete()
{
    std::lock_guard cache_lock(cache->mutex);
    completeWithoutStateUnlocked(cache_lock);
    return stat;
}

void FileSegment::completeWithoutStateUnlocked(std::lock_guard<std::mutex> & cache_lock)
{
    std::unique_lock segment_lock(mutex);
    completeBasedOnCurrentState(cache_lock, segment_lock);
}

void FileSegment::completeBasedOnCurrentState(std::lock_guard<std::mutex> & cache_lock, std::unique_lock<std::mutex> & segment_lock)
{
    if (is_detached)
        return;

    bool is_downloader = isDownloaderUnlocked(segment_lock);
    bool is_last_holder = cache->isLastFileSegmentHolder(key(), offset(), cache_lock, segment_lock);
    size_t current_downloaded_size = getDownloadedSizeUnlocked(segment_lock);

    SCOPE_EXIT({
        if (is_downloader)
        {
            cv.notify_all();
        }
    });

    LOG_TEST(
        log,
        "Complete based on current state (is_last_holder: {}, {})",
        is_last_holder, getInfoForLogUnlocked(segment_lock));

    if (is_downloader && !isInternal())
    {
        if (download_state == State::DOWNLOADING) /// != in case of completeWithState
            resetDownloadingStateUnlocked(segment_lock);

        resetDownloaderUnlocked(segment_lock);
    }

    switch (download_state)
    {
        case State::SKIP_CACHE:
        {
            if (is_last_holder)
                cache->remove(key(), offset(), cache_lock, segment_lock);

            return;
        }
        case State::DOWNLOADED:
        {
            chassert(getDownloadedSizeUnlocked(segment_lock) == range().size());
            assert(is_downloaded);
            assert(!cache_writer);
            break;
        }
        case State::DOWNLOADING:
        {
            chassert(!is_last_holder);
            break;
        }
        case State::EMPTY:
        case State::PARTIALLY_DOWNLOADED:
        case State::PARTIALLY_DOWNLOADED_NO_CONTINUATION:
        {
            if (is_last_holder)
            {
                assert(background_downloader_id.empty());

                if (current_downloaded_size == 0)
                {
                    LOG_TEST(log, "Remove cell {} (nothing downloaded)", range().toString());

                    setDownloadStateUnlocked(State::SKIP_CACHE, segment_lock);
                    cache->remove(key(), offset(), cache_lock, segment_lock);
                }
                else
                {
                    LOG_TEST(log, "Resize cell {} to downloaded: {}", range().toString(), current_downloaded_size);

                    /**
                    * Only last holder of current file segment can resize the cell,
                    * because there is an invariant that file segments returned to users
                    * in FileSegmentsHolder represent a contiguous range, so we can resize
                    * it only when nobody needs it.
                    */
                    setDownloadStateUnlocked(State::PARTIALLY_DOWNLOADED_NO_CONTINUATION, segment_lock);

                    if (stat.download_start_time)
                        stat.download_end_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

                    /// Resize this file segment by creating a copy file segment with DOWNLOADED state,
                    /// but current file segment should remain PARRTIALLY_DOWNLOADED_NO_CONTINUATION and with detached state,
                    /// because otherwise an invariant that getOrSet() returns a contiguous range of file segments will be broken
                    /// (this will be crucial for other file segment holder, not for current one).
                    cache->reduceSizeToDownloaded(key(), offset(), cache_lock, segment_lock);
                }

                detachAssumeStateFinalized(segment_lock);
            }
            break;
        }
    }

    LOG_TEST(log, "Completed file segment: {}", getInfoForLogUnlocked(segment_lock));
}

String FileSegment::getInfoForLog() const
{
    std::unique_lock segment_lock(mutex);
    return getInfoForLogUnlocked(segment_lock);
}

String FileSegment::getInfoForLogUnlocked(std::unique_lock<std::mutex> & segment_lock) const
{
    WriteBufferFromOwnString info;
    info << "File segment: " << range().toString() << ", ";
    info << "key: " << key().toString() << ", ";
    info << "state: " << download_state << ", ";
    info << "downloaded size: " << getDownloadedSizeUnlocked(segment_lock) << ", ";
    info << "reserved size: " << reserved_size << ", ";
    info << "downloader id: " << (downloader_id.empty() ? "None" : downloader_id) << ", ";
    info << "background downloader id: " << background_downloader_id << ", ";
    info << "current write offset: " << getCurrentWriteOffsetUnlocked(segment_lock) << ", ";
    info << "first non-downloaded offset: " << getFirstNonDownloadedOffsetUnlocked(segment_lock) << ", ";
    info << "caller id: " << getCallerId() << ", ";
    info << "internal: " << isInternal() << ", ";
    info << "detached: " << is_detached << ", ";
    info << "persistent: " << is_persistent;

    return info.str();
}

void FileSegment::wrapWithCacheInfo(Exception & e, const String & message, std::unique_lock<std::mutex> & segment_lock) const
{
    e.addMessage(fmt::format("{}, current cache state: {}", message, getInfoForLogUnlocked(segment_lock)));
}

String FileSegment::stateToString(FileSegment::State state)
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
    __builtin_unreachable();
}

void FileSegment::assertCorrectness() const
{
    std::unique_lock segment_lock(mutex);
    assertCorrectnessUnlocked(segment_lock);
}

void FileSegment::assertCorrectnessUnlocked(std::unique_lock<std::mutex> & segment_lock) const
{
    auto current_downloader = getDownloaderUnlocked(segment_lock);
    if (current_downloader.empty())
        assert(download_state != State::DOWNLOADING);
    else
        assert(download_state == State::DOWNLOADING || download_state == State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);
    chassert(download_state != FileSegment::State::DOWNLOADED || std::filesystem::file_size(getPathInLocalCache()) > 0);
}

void FileSegment::throwIfDetachedUnlocked(std::unique_lock<std::mutex> & segment_lock) const
{
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Cache file segment is in detached state, operation not allowed. "
        "It can happen when cache was concurrently dropped with SYSTEM DROP FILESYSTEM CACHE FORCE. "
        "Please, retry. File segment info: {}", getInfoForLogUnlocked(segment_lock));
}

void FileSegment::assertNotDetached() const
{
    std::unique_lock segment_lock(mutex);
    assertNotDetachedUnlocked(segment_lock);
}

void FileSegment::assertNotDetachedUnlocked(std::unique_lock<std::mutex> & segment_lock) const
{
    if (is_detached)
        throwIfDetachedUnlocked(segment_lock);
}

void FileSegment::assertDetachedStatus(std::unique_lock<std::mutex> & segment_lock) const
{
    /// Detached file segment is allowed to have only a certain subset of states.
    /// It should be either EMPTY or one of the finalized states.

    if (download_state != State::EMPTY && !hasFinalizedStateUnlocked(segment_lock))
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Detached file segment has incorrect state: {}",
            getInfoForLogUnlocked(segment_lock));
    }
}

FileSegmentPtr FileSegment::getSnapshot(const FileSegmentPtr & file_segment, std::lock_guard<std::mutex> & /* cache_lock */)
{
    std::unique_lock segment_lock(file_segment->mutex);

    auto snapshot = std::make_shared<FileSegment>(
        file_segment->offset(),
        file_segment->range().size(),
        file_segment->key(),
        nullptr,
        State::EMPTY,
        CreateFileSegmentSettings{});

    snapshot->hits_count = file_segment->getHitsCount();
    snapshot->ref_count = file_segment.use_count();
    snapshot->downloaded_size = file_segment->getDownloadedSizeUnlocked(segment_lock);
    snapshot->download_state = file_segment->download_state;
    snapshot->is_persistent = file_segment->isPersistent();
    snapshot->stat = file_segment->getStatUnlocked(segment_lock);

    return snapshot;
}

bool FileSegment::hasFinalizedStateUnlocked(std::unique_lock<std::mutex> & /* segment_lock */) const
{
    return download_state == State::DOWNLOADED
        || download_state == State::PARTIALLY_DOWNLOADED_NO_CONTINUATION
        || download_state == State::SKIP_CACHE;
}

bool FileSegment::isDetached() const
{
    std::unique_lock segment_lock(mutex);
    return is_detached;
}

void FileSegment::detach(std::lock_guard<std::mutex> & /* cache_lock */, std::unique_lock<std::mutex> & segment_lock)
{
    if (is_detached)
        return;

    if (download_state == State::DOWNLOADING)
        resetDownloadingStateUnlocked(segment_lock);
    else
        setDownloadStateUnlocked(State::PARTIALLY_DOWNLOADED_NO_CONTINUATION, segment_lock);

    resetDownloaderUnlocked(segment_lock);
    detachAssumeStateFinalized(segment_lock);
}

void FileSegment::detachAssumeStateFinalized(std::unique_lock<std::mutex> & segment_lock)
{
    is_detached = true;
    cancelBackgroundDownloadIfExists(segment_lock);

    CurrentMetrics::add(CurrentMetrics::CacheDetachedFileSegments);
    LOG_TEST(log, "Detached file segment: {}", getInfoForLogUnlocked(segment_lock));
}

FileSegment::~FileSegment()
{
    std::unique_lock segment_lock(mutex);
    if (is_detached)
        CurrentMetrics::sub(CurrentMetrics::CacheDetachedFileSegments);
}

FileSegmentsHolder::~FileSegmentsHolder()
{
    /// In CacheableReadBufferFromRemoteFS file segment's downloader removes file segments from
    /// FileSegmentsHolder right after calling file_segment->complete(), so on destruction here
    /// remain only uncompleted file segments.

    FileCache * cache = nullptr;

    for (auto file_segment_it = file_segments.begin(); file_segment_it != file_segments.end();)
    {
        auto current_file_segment_it = file_segment_it;
        auto & file_segment = *current_file_segment_it;

        if (!cache)
            cache = file_segment->cache;

        try
        {
            bool is_detached = false;

            {
                std::unique_lock segment_lock(file_segment->mutex);
                is_detached = file_segment->isDetached(segment_lock);
                if (is_detached)
                    file_segment->assertDetachedStatus(segment_lock);
            }

            if (is_detached)
            {
                /// This file segment is not owned by cache, so it will be destructed
                /// at this point, therefore no completion required.
                file_segment_it = file_segments.erase(current_file_segment_it);
                continue;
            }

            /// File segment pointer must be reset right after calling complete() and
            /// under the same mutex, because complete() checks for segment pointers.
            std::lock_guard cache_lock(cache->mutex);

            file_segment->completeWithoutStateUnlocked(cache_lock);

            file_segment_it = file_segments.erase(current_file_segment_it);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

String FileSegmentsHolder::toString()
{
    String ranges;
    for (const auto & file_segment : file_segments)
    {
        if (!ranges.empty())
            ranges += ", ";
        ranges += file_segment->range().toString();
    }
    return ranges;
}

}
