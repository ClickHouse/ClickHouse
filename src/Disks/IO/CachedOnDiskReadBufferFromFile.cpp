#include "CachedOnDiskReadBufferFromFile.h"
#include <algorithm>

#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Disks/ObjectStorages/Cached/CachedObjectStorage.h>
#include <Interpreters/Cache/FileCache.h>
#include <IO/BoundedReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/SwapHelper.h>
#include <Interpreters/Context.h>
#include <base/hex.h>
#include <base/scope_guard.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/assert_cast.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>


namespace ProfileEvents
{
extern const Event FileSegmentWaitReadBufferMicroseconds;
extern const Event FileSegmentReadMicroseconds;
extern const Event FileSegmentCacheWriteMicroseconds;
extern const Event FileSegmentPredownloadMicroseconds;
extern const Event FileSegmentUsedBytes;

extern const Event CachedReadBufferReadFromSourceMicroseconds;
extern const Event CachedReadBufferReadFromCacheMicroseconds;
extern const Event CachedReadBufferCacheWriteMicroseconds;
extern const Event CachedReadBufferReadFromSourceBytes;
extern const Event CachedReadBufferReadFromCacheBytes;
extern const Event CachedReadBufferPredownloadedBytes;
extern const Event CachedReadBufferCacheWriteBytes;
extern const Event CachedReadBufferCreateBufferMicroseconds;

extern const Event CachedReadBufferReadFromCacheHits;
extern const Event CachedReadBufferReadFromCacheMisses;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int LOGICAL_ERROR;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

CachedOnDiskReadBufferFromFile::CachedOnDiskReadBufferFromFile(
    const String & source_file_path_,
    const FileCache::Key & cache_key_,
    FileCachePtr cache_,
    const FileCacheUserInfo & user_,
    ImplementationBufferCreator implementation_buffer_creator_,
    const ReadSettings & settings_,
    const String & query_id_,
    size_t file_size_,
    bool allow_seeks_after_first_read_,
    bool use_external_buffer_,
    std::optional<size_t> read_until_position_,
    std::shared_ptr<FilesystemCacheLog> cache_log_)
    : ReadBufferFromFileBase(use_external_buffer_ ? 0 : settings_.remote_fs_buffer_size, nullptr, 0, file_size_)
#ifdef DEBUG_OR_SANITIZER_BUILD
    , log(getLogger(fmt::format("CachedOnDiskReadBufferFromFile({})", cache_key_)))
#else
    , log(getLogger("CachedOnDiskReadBufferFromFile"))
#endif
    , cache_key(cache_key_)
    , source_file_path(source_file_path_)
    , cache(cache_)
    , settings(settings_)
    , read_until_position(read_until_position_ ? *read_until_position_ : file_size_)
    , implementation_buffer_creator(implementation_buffer_creator_)
    , query_id(query_id_)
    , current_buffer_id(getRandomASCIIString(8))
    , user(user_)
    , allow_seeks_after_first_read(allow_seeks_after_first_read_)
    , use_external_buffer(use_external_buffer_)
    , query_context_holder(cache_->getQueryContextHolder(query_id, settings_))
    , cache_log(settings.enable_filesystem_cache_log ? cache_log_ : nullptr)
{
    LOG_TEST(
        log, "Boundary alignment: {}, external buffer: {}, allow seeks after first read: {}",
        settings.filesystem_cache_boundary_alignment.has_value() ? DB::toString(settings.filesystem_cache_boundary_alignment.value()) : "None",
        use_external_buffer, allow_seeks_after_first_read);
}

void CachedOnDiskReadBufferFromFile::appendFilesystemCacheLog(
    const FileSegment & file_segment, CachedOnDiskReadBufferFromFile::ReadType type)
{
    if (!cache_log)
        return;

    const auto range = file_segment.range();
    FilesystemCacheLogElement elem
    {
        .event_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()),
        .query_id = query_id,
        .source_file_path = source_file_path,
        .file_segment_range = { range.left, range.right },
        .requested_range = { first_offset, read_until_position },
        .file_segment_key = file_segment.key().toString(),
        .file_segment_offset = file_segment.offset(),
        .file_segment_size = range.size(),
        .read_from_cache_attempted = true,
        .read_buffer_id = current_buffer_id,
        .profile_counters = std::make_shared<ProfileEvents::Counters::Snapshot>(
            current_file_segment_counters.getPartiallyAtomicSnapshot()),
    };

    current_file_segment_counters.reset();

    switch (type)
    {
        case CachedOnDiskReadBufferFromFile::ReadType::CACHED:
            elem.cache_type = FilesystemCacheLogElement::CacheType::READ_FROM_CACHE;
            break;
        case CachedOnDiskReadBufferFromFile::ReadType::REMOTE_FS_READ_BYPASS_CACHE:
            elem.cache_type = FilesystemCacheLogElement::CacheType::READ_FROM_FS_BYPASSING_CACHE;
            break;
        case CachedOnDiskReadBufferFromFile::ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE:
            elem.cache_type = FilesystemCacheLogElement::CacheType::READ_FROM_FS_AND_DOWNLOADED_TO_CACHE;
            break;
    }

    cache_log->add(std::move(elem));
}

bool CachedOnDiskReadBufferFromFile::nextFileSegmentsBatch()
{
    chassert(!file_segments || file_segments->empty());
    size_t size = getRemainingSizeToRead();
    if (!size)
        return false;

    if (settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache)
    {
        file_segments = cache->get(cache_key, file_offset_of_buffer_end, size, settings.filesystem_cache_segments_batch_size, user.user_id);
    }
    else
    {
        CreateFileSegmentSettings create_settings(FileSegmentKind::Regular);
        file_segments = cache->getOrSet(
            cache_key, file_offset_of_buffer_end, size, file_size.value(),
            create_settings, settings.filesystem_cache_segments_batch_size, user, settings.filesystem_cache_boundary_alignment);
    }

    return !file_segments->empty();
}

void CachedOnDiskReadBufferFromFile::initialize()
{
    if (initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Caching buffer already initialized");

    implementation_buffer.reset();

    /**
     * Segments in returned list are ordered in ascending order and represent a full contiguous
     * interval (no holes). Each segment in returned list has state: DOWNLOADED, DOWNLOADING or EMPTY.
     */
    if (!nextFileSegmentsBatch())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "List of file segments cannot be empty");

    chassert(!file_segments->empty());

    LOG_TEST(
        log,
        "Having {} file segments to read: {}, current read range: [{}, {})",
        file_segments->size(), file_segments->toString(), file_offset_of_buffer_end, read_until_position);

    initialized = true;
}

CachedOnDiskReadBufferFromFile::ImplementationBufferPtr
CachedOnDiskReadBufferFromFile::getCacheReadBuffer(const FileSegment & file_segment)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::CachedReadBufferCreateBufferMicroseconds);

    auto path = file_segment.getPath();
    if (cache_file_reader)
    {
        chassert(cache_file_reader->getFileName() == path);
        if (cache_file_reader->getFileName() == path)
            return cache_file_reader;

        cache_file_reader.reset();
    }

    ReadSettings local_read_settings{settings};
    local_read_settings.local_fs_method = LocalFSReadMethod::pread;

    if (use_external_buffer)
        local_read_settings.local_fs_buffer_size = 0;

    cache_file_reader
        = createReadBufferFromFileBase(path, local_read_settings, std::nullopt, std::nullopt, file_segment.getFlagsForLocalRead());

    if (getFileSizeFromReadBuffer(*cache_file_reader) == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read from an empty cache file: {}", path);

    return cache_file_reader;
}

CachedOnDiskReadBufferFromFile::ImplementationBufferPtr
CachedOnDiskReadBufferFromFile::getRemoteReadBuffer(FileSegment & file_segment, ReadType read_type_)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::CachedReadBufferCreateBufferMicroseconds);

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
            *     | segment1  | segment2
            *
            * So query2 can reuse implementation buffer, which downloaded segment1.
            * Implementation buffer from segment1 is passed to segment2 once segment1 is loaded.
            */

            auto remote_fs_segment_reader = file_segment.getRemoteFileReader();

            if (!remote_fs_segment_reader)
            {
                auto impl = implementation_buffer_creator();
                if (impl->supportsRightBoundedReads())
                    remote_fs_segment_reader = std::move(impl);
                else
                    remote_fs_segment_reader = std::make_unique<BoundedReadBuffer>(std::move(impl));

                file_segment.setRemoteFileReader(remote_fs_segment_reader);
            }
            else
            {
                chassert(remote_fs_segment_reader->getFileOffsetOfBufferEnd() == file_segment.getCurrentWriteOffset());
            }

            return remote_fs_segment_reader;
        }
        case ReadType::REMOTE_FS_READ_BYPASS_CACHE:
        {
            /// Result buffer is owned only by current buffer -- not shareable like in the case above.

            if (remote_file_reader && remote_file_reader->getFileOffsetOfBufferEnd() == file_offset_of_buffer_end)
                return remote_file_reader;

            auto remote_fs_segment_reader = file_segment.extractRemoteFileReader();
            if (remote_fs_segment_reader && file_offset_of_buffer_end == remote_fs_segment_reader->getFileOffsetOfBufferEnd())
                remote_file_reader = remote_fs_segment_reader;
            else
                remote_file_reader = implementation_buffer_creator();

            return remote_file_reader;
        }
        default:
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot use remote filesystem reader with read type: {}",
                toString(read_type));
    }
}

bool CachedOnDiskReadBufferFromFile::canStartFromCache(size_t current_offset, const FileSegment & file_segment)
{
    ///                      segment{k} state: DOWNLOADING
    /// cache:           [______|___________
    ///                         ^
    ///                         current_write_offset (in progress)
    /// requested_range:    [__________]
    ///                     ^
    ///                     current_offset
    size_t current_write_offset = file_segment.getCurrentWriteOffset();
    return current_write_offset > current_offset;
}

String CachedOnDiskReadBufferFromFile::toString(ReadType type)
{
    return String(magic_enum::enum_name(type));
}

CachedOnDiskReadBufferFromFile::ImplementationBufferPtr
CachedOnDiskReadBufferFromFile::getReadBufferForFileSegment(FileSegment & file_segment)
{
    auto download_state = file_segment.state();

    if (settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache)
    {
        if (download_state == FileSegment::State::DOWNLOADED)
        {
            read_type = ReadType::CACHED;
            return getCacheReadBuffer(file_segment);
        }

        LOG_TEST(log, "Bypassing cache because `read_from_filesystem_cache_if_exists_otherwise_bypass_cache` option is used");
        read_type = ReadType::REMOTE_FS_READ_BYPASS_CACHE;
        return getRemoteReadBuffer(file_segment, read_type);
    }

    while (true)
    {
        switch (download_state)
        {
            case FileSegment::State::DETACHED:
            {
                LOG_TRACE(log, "Bypassing cache because file segment state is `DETACHED`");
                read_type = ReadType::REMOTE_FS_READ_BYPASS_CACHE;
                return getRemoteReadBuffer(file_segment, read_type);
            }
            case FileSegment::State::DOWNLOADING:
            {
                if (canStartFromCache(file_offset_of_buffer_end, file_segment))
                {
                    ///                      segment{k} state: DOWNLOADING
                    /// cache:           [______|___________
                    ///                         ^
                    ///                         current_write_offset (in progress)
                    /// requested_range:    [__________]
                    ///                     ^
                    ///                     file_offset_of_buffer_end

                    read_type = ReadType::CACHED;
                    return getCacheReadBuffer(file_segment);
                }

                download_state = file_segment.wait(file_offset_of_buffer_end);
                continue;
            }
            case FileSegment::State::DOWNLOADED:
            {
                read_type = ReadType::CACHED;
                return getCacheReadBuffer(file_segment);
            }
            case FileSegment::State::EMPTY:
            case FileSegment::State::PARTIALLY_DOWNLOADED:
            {
                if (canStartFromCache(file_offset_of_buffer_end, file_segment))
                {
                    ///                      segment{k} state: PARTIALLY_DOWNLOADED
                    /// cache:           [______|___________
                    ///                         ^
                    ///                         current_write_offset (in progress)
                    /// requested_range:    [__________]
                    ///                     ^
                    ///                     file_offset_of_buffer_end

                    read_type = ReadType::CACHED;
                    return getCacheReadBuffer(file_segment);
                }

                auto downloader_id = file_segment.getOrSetDownloader();
                if (downloader_id == FileSegment::getCallerId())
                {
                    if (canStartFromCache(file_offset_of_buffer_end, file_segment))
                    {
                        ///                      segment{k}
                        /// cache:           [______|___________
                        ///                         ^
                        ///                         current_write_offset
                        /// requested_range:    [__________]
                        ///                     ^
                        ///                     file_offset_of_buffer_end

                        read_type = ReadType::CACHED;
                        file_segment.resetDownloader();
                        return getCacheReadBuffer(file_segment);
                    }

                    auto current_write_offset = file_segment.getCurrentWriteOffset();
                    if (current_write_offset < file_offset_of_buffer_end)
                    {
                        ///                   segment{1}
                        /// cache:         [_____|___________
                        ///                      ^
                        ///                      current_write_offset
                        /// requested_range:          [__________]
                        ///                           ^
                        ///                           file_offset_of_buffer_end

                        LOG_TEST(log, "Predownload. File segment info: {}", file_segment.getInfoForLog());
                        chassert(file_offset_of_buffer_end > current_write_offset);
                        bytes_to_predownload = file_offset_of_buffer_end - current_write_offset;
                        chassert(bytes_to_predownload < file_segment.range().size());
                    }

                    read_type = ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE;
                    return getRemoteReadBuffer(file_segment, read_type);
                }

                download_state = file_segment.state();
                continue;
            }
            case FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION:
            {
                if (canStartFromCache(file_offset_of_buffer_end, file_segment))
                {
                    read_type = ReadType::CACHED;
                    return getCacheReadBuffer(file_segment);
                }

                LOG_TRACE(
                    log,
                    "Bypassing cache because file segment state is "
                    "`PARTIALLY_DOWNLOADED_NO_CONTINUATION` and downloaded part already used");
                read_type = ReadType::REMOTE_FS_READ_BYPASS_CACHE;
                return getRemoteReadBuffer(file_segment, read_type);
            }
        }
    }
}

CachedOnDiskReadBufferFromFile::ImplementationBufferPtr
CachedOnDiskReadBufferFromFile::getImplementationBuffer(FileSegment & file_segment)
{
    chassert(!file_segment.isDownloader());
    chassert(file_offset_of_buffer_end >= file_segment.range().left);

    auto range = file_segment.range();
    bytes_to_predownload = 0;

    Stopwatch watch(CLOCK_MONOTONIC);

    auto read_buffer_for_file_segment = getReadBufferForFileSegment(file_segment);

    watch.stop();

    LOG_TEST(
        log,
        "Current read type: {}, read offset: {}, impl read range: {}, file segment: {}",
        toString(read_type),
        file_offset_of_buffer_end,
        read_buffer_for_file_segment->getFileOffsetOfBufferEnd(),
        file_segment.getInfoForLog());

    current_file_segment_counters.increment(
        ProfileEvents::FileSegmentWaitReadBufferMicroseconds, watch.elapsedMicroseconds());

    ProfileEvents::increment(ProfileEvents::FileSegmentWaitReadBufferMicroseconds, watch.elapsedMicroseconds());

    [[maybe_unused]] auto download_current_segment = read_type == ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE;
    chassert(download_current_segment == file_segment.isDownloader());

    chassert(file_segment.range() == range);
    chassert(file_offset_of_buffer_end >= range.left && file_offset_of_buffer_end <= range.right);

    read_buffer_for_file_segment->setReadUntilPosition(range.right + 1); /// [..., range.right]

    switch (read_type)
    {
        case ReadType::CACHED:
        {
#ifdef DEBUG_OR_SANITIZER_BUILD
            size_t file_size = getFileSizeFromReadBuffer(*read_buffer_for_file_segment);
            if (file_size == 0 || range.left + file_size <= file_offset_of_buffer_end)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Unexpected state of cache file. Cache file size: {}, cache file offset: {}, "
                    "expected file size to be non-zero and file downloaded size to exceed "
                    "current file read offset (expected: {} > {})",
                    file_size,
                    range.left,
                    range.left + file_size,
                    file_offset_of_buffer_end);
#endif

            size_t seek_offset = file_offset_of_buffer_end - range.left;

            if (file_offset_of_buffer_end < range.left)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Invariant failed. Expected {} > {} (current offset > file segment's start offset)",
                    file_offset_of_buffer_end,
                    range.left);

            read_buffer_for_file_segment->seek(seek_offset, SEEK_SET);

            break;
        }
        case ReadType::REMOTE_FS_READ_BYPASS_CACHE:
        {
            read_buffer_for_file_segment->seek(file_offset_of_buffer_end, SEEK_SET);
            break;
        }
        case ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE:
        {
            chassert(file_segment.isDownloader());

            if (bytes_to_predownload)
            {
                const size_t current_write_offset = file_segment.getCurrentWriteOffset();
                read_buffer_for_file_segment->seek(current_write_offset, SEEK_SET);
            }
            else
            {
                read_buffer_for_file_segment->seek(file_offset_of_buffer_end, SEEK_SET);

                chassert(read_buffer_for_file_segment->getFileOffsetOfBufferEnd() == file_offset_of_buffer_end);
            }

            const auto current_write_offset = file_segment.getCurrentWriteOffset();
            if (current_write_offset != static_cast<size_t>(read_buffer_for_file_segment->getPosition()))
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Buffer's offsets mismatch. Cached buffer offset: {}, current_write_offset: {}, "
                    "implementation buffer position: {}, implementation buffer end position: {}, file segment info: {}",
                    file_offset_of_buffer_end,
                    current_write_offset,
                    read_buffer_for_file_segment->getPosition(),
                    read_buffer_for_file_segment->getFileOffsetOfBufferEnd(),
                    file_segment.getInfoForLog());
            }

            break;
        }
    }

    chassert(!read_buffer_for_file_segment->hasPendingData());

    return read_buffer_for_file_segment;
}

bool CachedOnDiskReadBufferFromFile::completeFileSegmentAndGetNext()
{
    auto * current_file_segment = &file_segments->front();
    auto completed_range = current_file_segment->range();

    if (cache_log)
        appendFilesystemCacheLog(*current_file_segment, read_type);

    chassert(file_offset_of_buffer_end > completed_range.right);
    cache_file_reader.reset();

    file_segments->completeAndPopFront(settings.filesystem_cache_allow_background_download);
    if (file_segments->empty() && !nextFileSegmentsBatch())
        return false;

    current_file_segment = &file_segments->front();
    current_file_segment->increasePriority();
    implementation_buffer = getImplementationBuffer(*current_file_segment);

    LOG_TEST(
        log, "New segment range: {}, old range: {}",
        current_file_segment->range().toString(), completed_range.toString());

    return true;
}

CachedOnDiskReadBufferFromFile::~CachedOnDiskReadBufferFromFile()
{
    if (cache_log && file_segments && !file_segments->empty())
    {
        appendFilesystemCacheLog(file_segments->front(), read_type);
    }

    if (file_segments && !file_segments->empty() && !file_segments->front().isCompleted())
    {
        file_segments->completeAndPopFront(settings.filesystem_cache_allow_background_download);
        file_segments = {};
    }
}

bool CachedOnDiskReadBufferFromFile::predownload(FileSegment & file_segment)
{
    Stopwatch predownload_watch(CLOCK_MONOTONIC);
    SCOPE_EXIT({
        predownload_watch.stop();
        current_file_segment_counters.increment(
            ProfileEvents::FileSegmentPredownloadMicroseconds, predownload_watch.elapsedMicroseconds());
    });

    OpenTelemetry::SpanHolder span("CachedOnDiskReadBufferFromFile::predownload");
    span.addAttribute("clickhouse.key", file_segment.key().toString());
    span.addAttribute("clickhouse.size", bytes_to_predownload);

    if (bytes_to_predownload)
    {
        /// Consider this case. Some user needed segment [a, b] and downloaded it partially.
        /// But before he called complete(state) or his holder called complete(),
        /// some other user, who needed segment [a', b'], a < a' < b', started waiting on [a, b] to be
        /// downloaded because it intersects with the range he needs.
        /// But then first downloader fails and second must continue. In this case we need to
        /// download from offset a'' < a', but return buffer from offset a'.
        LOG_TEST(log, "Bytes to predownload: {}, caller_id: {}", bytes_to_predownload, FileSegment::getCallerId());

        /// chassert(implementation_buffer->getFileOffsetOfBufferEnd() == file_segment.getCurrentWriteOffset());
        size_t current_offset = file_segment.getCurrentWriteOffset();
        chassert(static_cast<size_t>(implementation_buffer->getPosition()) == current_offset);
        const auto & current_range = file_segment.range();

        while (true)
        {
            bool has_more_data;
            {
                Stopwatch watch(CLOCK_MONOTONIC);

                has_more_data = !implementation_buffer->eof();

                watch.stop();
                auto elapsed = watch.elapsedMicroseconds();
                current_file_segment_counters.increment(ProfileEvents::FileSegmentReadMicroseconds, elapsed);
                ProfileEvents::increment(ProfileEvents::CachedReadBufferReadFromSourceMicroseconds, elapsed);
            }

            if (!bytes_to_predownload || !has_more_data)
            {
                if (bytes_to_predownload)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Failed to predownload remaining {} bytes. Current file segment: {}, "
                        "current download offset: {}, expected: {}, eof: {}, internal buffer size: {}",
                        bytes_to_predownload,
                        current_range.toString(),
                        file_segment.getCurrentWriteOffset(),
                        file_offset_of_buffer_end,
                        implementation_buffer->eof(),
                        implementation_buffer->internalBuffer().size());

                auto result = implementation_buffer->hasPendingData();

                if (result)
                {
                    nextimpl_working_buffer_offset = implementation_buffer->offset();

                    auto current_write_offset = file_segment.getCurrentWriteOffset();
                    if (current_write_offset != static_cast<size_t>(implementation_buffer->getPosition())
                        || current_write_offset != file_offset_of_buffer_end)
                    {
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR,
                            "Buffer's offsets mismatch after predownloading; download offset: {}, "
                            "cached buffer offset: {}, implementation buffer offset: {}, "
                            "file segment info: {}",
                            current_write_offset,
                            file_offset_of_buffer_end,
                            implementation_buffer->getPosition(),
                            file_segment.getInfoForLog());
                    }
                }

                break;
            }

            size_t current_impl_buffer_size = implementation_buffer->buffer().size();
            size_t current_predownload_size = std::min(current_impl_buffer_size, bytes_to_predownload);

            ProfileEvents::increment(ProfileEvents::CachedReadBufferReadFromSourceBytes, current_impl_buffer_size);
            ProfileEvents::increment(ProfileEvents::CachedReadBufferPredownloadedBytes, current_impl_buffer_size);

            std::string failure_reason;
            bool continue_predownload = file_segment.reserve(
                current_predownload_size, settings.filesystem_cache_reserve_space_wait_lock_timeout_milliseconds, failure_reason);
            if (continue_predownload)
            {
                LOG_TEST(log, "Left to predownload: {}, buffer size: {}", bytes_to_predownload, current_impl_buffer_size);

                chassert(file_segment.getCurrentWriteOffset() == static_cast<size_t>(implementation_buffer->getPosition()));

                continue_predownload = writeCache(implementation_buffer->buffer().begin(), current_predownload_size, current_offset, file_segment);
                if (continue_predownload)
                {
                    current_offset += current_predownload_size;

                    bytes_to_predownload -= current_predownload_size;
                    implementation_buffer->position() += current_predownload_size;
                }
                else
                {
                    LOG_TEST(log, "Bypassing cache because writeCache (in predownload) method failed");
                }
            }

            if (!continue_predownload)
            {
                /// We were predownloading:
                ///                   segment{1}
                /// cache:         [_____|___________
                ///                      ^
                ///                      current_write_offset
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
                file_segment.completePartAndResetDownloader();
                chassert(file_segment.state() == FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);

                LOG_TEST(log, "Bypassing cache for {}", file_segment.getInfoForLog());

                read_type = ReadType::REMOTE_FS_READ_BYPASS_CACHE;

                LOG_TRACE(
                    log,
                    "Predownload failed because of space limit. "
                    "Will read from remote filesystem starting from offset: {}",
                    file_offset_of_buffer_end);

                return false;
            }
        }
    }
    return true;
}

bool CachedOnDiskReadBufferFromFile::updateImplementationBufferIfNeeded()
{
    chassert(!file_segments->empty());
    auto & file_segment = file_segments->front();
    const auto & current_read_range = file_segment.range();
    auto current_state = file_segment.state();

    chassert(current_read_range.left <= file_offset_of_buffer_end);
    chassert(!file_segment.isDownloader());

    if (file_offset_of_buffer_end > current_read_range.right)
    {
        return completeFileSegmentAndGetNext();
    }

    if (read_type == ReadType::CACHED && current_state != FileSegment::State::DOWNLOADED)
    {
        /// If current read_type is ReadType::CACHED and file segment is not DOWNLOADED,
        /// it means the following case, e.g. we started from CacheReadBuffer and continue with RemoteFSReadBuffer.
        ///                  segment{k}
        /// cache:           [______|___________]
        ///                         ^
        ///                         current_write_offset
        /// requested_range:    [__________
        ///                     ^
        ///                     file_offset_of_buffer_end

        if (file_offset_of_buffer_end >= file_segment.getCurrentWriteOffset())
        {
            implementation_buffer = getImplementationBuffer(file_segment);
            return true;
        }
    }
    else if (read_type == ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE)
    {
        /**
        * ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE means that on previous getImplementationBuffer() call
        * current buffer successfully called file_segment->getOrSetDownloader() and became a downloader
        * for this file segment. However, the downloader's term has a lifespan of 1 nextImpl() call,
        * e.g. downloader reads buffer_size byte and calls completePartAndResetDownloader() and some other
        * thread can become a downloader if it calls getOrSetDownloader() faster.
        *
        * So downloader is committed to download only buffer_size bytes and then is not a downloader anymore,
        * because there is no guarantee on a higher level, that current buffer will not disappear without
        * being destructed till the end of query or without finishing the read range, which he was supposed
        * to read by marks range given to him. Therefore, each nextImpl() call, in case of
        * READ_AND_PUT_IN_CACHE, starts with getOrSetDownloader().
        */
        implementation_buffer = getImplementationBuffer(file_segment);
    }

    return true;
}

bool CachedOnDiskReadBufferFromFile::writeCache(char * data, size_t size, size_t offset, FileSegment & file_segment)
{
    Stopwatch watch(CLOCK_MONOTONIC);

    try
    {
        file_segment.write(data, size, offset);
    }
    catch (ErrnoException & e)
    {
        int code = e.getErrno();
        if (code == /* No space left on device */28 || code == /* Quota exceeded */122)
        {
            LOG_INFO(log, "Insert into cache is skipped due to insufficient disk space. ({})", e.displayText());
            return false;
        }
        chassert(file_segment.state() == FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);
        throw;
    }

    watch.stop();
    auto elapsed = watch.elapsedMicroseconds();
    current_file_segment_counters.increment(ProfileEvents::FileSegmentCacheWriteMicroseconds, elapsed);
    ProfileEvents::increment(ProfileEvents::CachedReadBufferCacheWriteMicroseconds, elapsed);
    ProfileEvents::increment(ProfileEvents::CachedReadBufferCacheWriteBytes, size);

    return true;
}

bool CachedOnDiskReadBufferFromFile::nextImpl()
{
    try
    {
        return nextImplStep();
    }
    catch (Exception & e)
    {
        e.addMessage("Cache info: {}", nextimpl_step_log_info);
        throw;
    }
}

bool CachedOnDiskReadBufferFromFile::nextImplStep()
{
    last_caller_id = FileSegment::getCallerId();

    chassert(file_offset_of_buffer_end <= read_until_position);
    if (file_offset_of_buffer_end == read_until_position)
        return false;

    if (!initialized)
        initialize();

    if (file_segments->empty() && !nextFileSegmentsBatch())
        return false;

    const size_t original_buffer_size = internal_buffer.size();
    if (!original_buffer_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Internal buffer cannot be empty (use external buffer: {}; {})", use_external_buffer, getInfoForLog());

    bool implementation_buffer_can_be_reused = false;
    SCOPE_EXIT({
        try
        {
            /// Save state of current file segment before it is completed. But we'll use it only if exception happened.
            if (std::uncaught_exceptions() > 0)
                nextimpl_step_log_info = getInfoForLog();

            if (!use_external_buffer)
            {
                chassert(!internal_buffer.empty());
                chassert(internal_buffer.begin());
            }

            if (file_segments->empty())
                return;

            auto & file_segment = file_segments->front();

            bool download_current_segment = read_type == ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE;
            if (download_current_segment)
            {
                bool need_complete_file_segment = file_segment.isDownloader();
                if (need_complete_file_segment)
                {
                    if (!implementation_buffer_can_be_reused)
                        file_segment.resetRemoteFileReader();

                    file_segment.completePartAndResetDownloader();
                }
            }

            if (use_external_buffer && !internal_buffer.empty())
                internal_buffer.resize(original_buffer_size);

            chassert(!file_segment.isDownloader());
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
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
        implementation_buffer = getImplementationBuffer(file_segments->front());
        file_segments->front().increasePriority();
    }

    auto & file_segment = file_segments->front();
    const auto & current_read_range = file_segment.range();

    if (use_external_buffer && read_type == ReadType::CACHED)
    {
        /// We allocate buffers not less than 1M so that s3 requests will not be too small. But the same buffers (members of AsynchronousReadIndirectBufferFromRemoteFS)
        /// are used for reading from files. Some of these readings are fairly small and their performance degrade when we use big buffers (up to ~20% for queries like Q23 from ClickBench).
        if (settings.local_fs_buffer_size && settings.local_fs_buffer_size < internal_buffer.size())
            internal_buffer.resize(settings.local_fs_buffer_size);

        /// It would make sense to reduce buffer size to what is left to read (when we read the last segment) regardless of the read_type.
        /// But we have to use big enough buffers when we [pre]download segments to amortize netw and FileCache overhead (space reservation and relevant locks).
        if (file_segments->size() == 1)
        {
            const size_t remaining_size_to_read
                = std::min(current_read_range.right, read_until_position - 1) - file_offset_of_buffer_end + 1;
            const size_t new_buf_size = std::min(internal_buffer.size(), remaining_size_to_read);
            chassert((internal_buffer.size() >= nextimpl_working_buffer_offset + new_buf_size) && (new_buf_size > 0));
            internal_buffer.resize(nextimpl_working_buffer_offset + new_buf_size);
        }
    }

    // Pass a valid external buffer for implementation_buffer to read into.
    // We then take it back with another swap() after reading is done.
    // (If we get an exception in between, we'll be left with an invalid internal_buffer. That's ok, as long as
    // the caller doesn't try to use this CachedOnDiskReadBufferFromFile after it threw an exception.)
    std::optional<SwapHelper> swap;
    swap.emplace(*this, *implementation_buffer);

    LOG_TEST(
        log,
        "Current read type: {}, read offset: {}, impl offset: {}, impl position: {}, file segment: {}",
        toString(read_type),
        file_offset_of_buffer_end,
        implementation_buffer->getFileOffsetOfBufferEnd(),
        implementation_buffer->getPosition(),
        file_segment.getInfoForLog());

    chassert(current_read_range.left <= file_offset_of_buffer_end);
    chassert(current_read_range.right >= file_offset_of_buffer_end);

    bool result = false;
    size_t size = 0;

    size_t needed_to_predownload = bytes_to_predownload;
    if (needed_to_predownload)
    {
        if (predownload(file_segment))
        {
            result = implementation_buffer->hasPendingData();
            size = implementation_buffer->available();
        }
        else
        {
            /// Move buffer back from implementation_buffer to us.
            swap.reset();
            resetWorkingBuffer();

            implementation_buffer = getRemoteReadBuffer(file_segment, read_type);

            /// Recreate swap helper.
            swap.emplace(*this, *implementation_buffer);

            implementation_buffer->setReadUntilPosition(file_segment.range().right + 1); /// [..., range.right]
            implementation_buffer->seek(file_offset_of_buffer_end, SEEK_SET);
        }
    }

    auto download_current_segment = read_type == ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE;
    if (download_current_segment != file_segment.isDownloader())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Incorrect segment state. Having read type: {}, file segment info: {}",
            toString(read_type), file_segment.getInfoForLog());
    }

    if (!result)
    {
#ifdef DEBUG_OR_SANITIZER_BUILD
        if (read_type == ReadType::CACHED)
        {
            size_t cache_file_size = getFileSizeFromReadBuffer(*implementation_buffer);
            if (cache_file_size == 0)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Attempt to read from an empty cache file: {} (just before actual read)",
                    cache_file_size);
            }
        }
        else
        {
            chassert(file_offset_of_buffer_end == static_cast<size_t>(implementation_buffer->getFileOffsetOfBufferEnd()));
        }
        chassert(!implementation_buffer->hasPendingData());
#endif

        Stopwatch watch(CLOCK_MONOTONIC);

        result = implementation_buffer->next();

        watch.stop();
        auto elapsed = watch.elapsedMicroseconds();
        current_file_segment_counters.increment(ProfileEvents::FileSegmentReadMicroseconds, elapsed);

        // We don't support implementation_buffer implementations that use nextimpl_working_buffer_offset.
        chassert(implementation_buffer->position() == implementation_buffer->buffer().begin());

        if (result)
            size = implementation_buffer->buffer().size();

        LOG_TEST(
            log,
            "Read {} bytes, read type {}, file offset: {}, impl offset: {}/{}, impl position: {}, segment: {}",
            size, toString(read_type), file_offset_of_buffer_end,
            implementation_buffer->getFileOffsetOfBufferEnd(), read_until_position,
            implementation_buffer->getPosition(), file_segment.range().toString());

        if (read_type == ReadType::CACHED)
        {
            ProfileEvents::increment(ProfileEvents::CachedReadBufferReadFromCacheHits);
            ProfileEvents::increment(ProfileEvents::CachedReadBufferReadFromCacheBytes, size);
            ProfileEvents::increment(ProfileEvents::CachedReadBufferReadFromCacheMicroseconds, elapsed);
        }
        else
        {
            ProfileEvents::increment(ProfileEvents::CachedReadBufferReadFromCacheMisses);
            ProfileEvents::increment(ProfileEvents::CachedReadBufferReadFromSourceBytes, size);
            ProfileEvents::increment(ProfileEvents::CachedReadBufferReadFromSourceMicroseconds, elapsed);
        }
    }

    if (result)
    {
        bool download_current_segment_succeeded = false;
        if (download_current_segment)
        {
            chassert(file_offset_of_buffer_end + size - 1 <= file_segment.range().right);

            std::string failure_reason;
            bool success = file_segment.reserve(size, settings.filesystem_cache_reserve_space_wait_lock_timeout_milliseconds, failure_reason);
            if (success)
            {
                chassert(file_segment.getCurrentWriteOffset() == static_cast<size_t>(implementation_buffer->getPosition()));

                success = writeCache(implementation_buffer->position(), size, file_offset_of_buffer_end, file_segment);
                if (success)
                {
                    chassert(file_segment.getCurrentWriteOffset() <= file_segment.range().right + 1);
                    chassert(
                        /* last_file_segment */file_segments->size() == 1
                        || file_segment.getCurrentWriteOffset() == implementation_buffer->getFileOffsetOfBufferEnd());

                    LOG_TEST(log, "Successfully written {} bytes", size);
                    download_current_segment_succeeded = true;

                    // The implementation_buffer is valid and positioned correctly (at file_segment->getCurrentWriteOffset()).
                    // Later reads for this file segment can reuse it.
                    // (It's reusable even if we don't reach the swap(*implementation_buffer) below,
                    // because the reuser must assign implementation_buffer's buffer anyway.)
                    implementation_buffer_can_be_reused = true;
                }
                else
                    LOG_TRACE(log, "Bypassing cache because writeCache method failed");
            }
            else
                LOG_TRACE(log, "No space left in cache to reserve {} bytes, reason: {}, "
                          "will continue without cache download", size, failure_reason);

            if (!success)
            {
                read_type = ReadType::REMOTE_FS_READ_BYPASS_CACHE;
                chassert(file_segment.state() == FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);
            }
        }

        /// - If last file segment was read from remote fs, then we read up to segment->range().right,
        /// but the requested right boundary could be
        /// segment->range().left < requested_right_boundary <  segment->range().right.
        /// Therefore need to resize to a smaller size. And resize must be done after write into cache.
        /// - If last file segment was read from local fs, then we could read more than
        /// file_segemnt->range().right, so resize is also needed.
        if (file_segments->size() == 1)
        {
            size_t remaining_size_to_read = std::min(current_read_range.right, read_until_position - 1) - file_offset_of_buffer_end + 1;

            LOG_TEST(log, "Remaining size to read: {}, read: {}. Resizing buffer to {}",
                     remaining_size_to_read, size, nextimpl_working_buffer_offset + std::min(size, remaining_size_to_read));

            size = std::min(size, remaining_size_to_read);
            chassert(implementation_buffer->buffer().size() >= nextimpl_working_buffer_offset + size);
            implementation_buffer->buffer().resize(nextimpl_working_buffer_offset + size);
        }

        file_offset_of_buffer_end += size;

        if (download_current_segment && download_current_segment_succeeded)
            chassert(file_segment.getCurrentWriteOffset() >= file_offset_of_buffer_end);

        chassert(
            file_offset_of_buffer_end <= read_until_position,
            fmt::format("Expected {} <= {} (size: {}, read range: {}, hold file segments: {} ({}))",
                        file_offset_of_buffer_end, read_until_position, size, current_read_range.toString(), file_segments->size(), file_segments->toString(true)));
    }

    swap.reset();

    current_file_segment_counters.increment(ProfileEvents::FileSegmentUsedBytes, available());

    if (size == 0 && file_offset_of_buffer_end < read_until_position)
    {
        size_t cache_file_size = getFileSizeFromReadBuffer(*implementation_buffer);
        auto cache_file_path = getFileNameFromReadBuffer(*implementation_buffer);

        std::string download_finished_time;
        if (file_segment.isDownloaded())
        {
            WriteBufferFromString buf{download_finished_time};
            writeDateTimeText(file_segment.getFinishedDownloadTime(), buf);
        }
        else
            download_finished_time = "None";

        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Having zero bytes, but range is not finished: "
            "result: {}, "
            "file offset: {}, "
            "read bytes: {}, "
            "initial read offset: {}, "
            "nextimpl working buffer offset: {},"
            "reading until: {}, "
            "read type: {}, "
            "working buffer size: {}, "
            "internal buffer size: {}, "
            "finished download time: {}, "
            "cache file size: {}, "
            "cache file path: {}, "
            "cache file offset: {}, "
            "remaining file segments: {}, "
            "current file segment: {}",
            result,
            file_offset_of_buffer_end,
            size,
            first_offset,
            nextimpl_working_buffer_offset,
            read_until_position,
            toString(read_type),
            implementation_buffer->buffer().size(),
            implementation_buffer->internalBuffer().size(),
            download_finished_time,
            cache_file_size ? std::to_string(cache_file_size) : "None",
            cache_file_path,
            implementation_buffer->getFileOffsetOfBufferEnd(),
            file_segments->size(),
            file_segment.getInfoForLog());
    }

    // No necessary because of the SCOPE_EXIT above, but useful for logging below.
    if (download_current_segment)
        file_segment.completePartAndResetDownloader();

    chassert(!file_segment.isDownloader());

    LOG_TEST(
        log,
        "Key: {}. Returning with {} bytes, buffer position: {} (offset: {}, predownloaded: {}), "
        "buffer available: {}, current range: {}, file offset of buffer end: {}, file segment state: {}, "
        "current write offset: {}, read_type: {}, reading until position: {}, started with offset: {}, "
        "remaining ranges: {}",
        cache_key.toString(),
        working_buffer.size(),
        getPosition(),
        offset(),
        needed_to_predownload,
        available(),
        current_read_range.toString(),
        file_offset_of_buffer_end,
        FileSegment::stateToString(file_segment.state()),
        file_segment.getCurrentWriteOffset(),
        toString(read_type),
        read_until_position,
        first_offset,
        file_segments->toString());

    /// Release buffer a little bit earlier.
    if (read_until_position == file_offset_of_buffer_end)
        implementation_buffer.reset();
    else if (file_offset_of_buffer_end > current_read_range.right)
        completeFileSegmentAndGetNext();

    return result;
}

off_t CachedOnDiskReadBufferFromFile::seek(off_t offset, int whence)
{
    if (initialized && !allow_seeks_after_first_read)
    {
        throw Exception(
            ErrorCodes::CANNOT_SEEK_THROUGH_FILE,
            "Seek is allowed only before first read attempt from the buffer");
    }

    size_t new_pos = offset;

    if (allow_seeks_after_first_read && !use_external_buffer)
    {
        if (whence != SEEK_SET && whence != SEEK_CUR)
        {
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Expected SEEK_SET or SEEK_CUR as whence");
        }

        if (whence == SEEK_CUR)
        {
            new_pos = file_offset_of_buffer_end - (working_buffer.end() - pos) + offset;
        }

        if (new_pos + (working_buffer.end() - pos) == file_offset_of_buffer_end)
            return new_pos;

        if (file_offset_of_buffer_end - working_buffer.size() <= new_pos && new_pos <= file_offset_of_buffer_end)
        {
            pos = working_buffer.end() - file_offset_of_buffer_end + new_pos;
            chassert(pos >= working_buffer.begin());
            chassert(pos <= working_buffer.end());
            return new_pos;
        }
    }
    else if (whence != SEEK_SET)
    {
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET allowed");
    }

    if (!use_external_buffer)
        resetWorkingBuffer();

    first_offset = file_offset_of_buffer_end = new_pos;
    file_segments.reset();
    implementation_buffer.reset();
    cache_file_reader.reset();
    initialized = false;

    LOG_TEST(log, "Reset state for seek to position {}", new_pos);

    return new_pos;
}

size_t CachedOnDiskReadBufferFromFile::getRemainingSizeToRead()
{
    /// Last position should be guaranteed to be set, as at least we always know file size.
    if (!read_until_position)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Last position was not set");

    /// On this level should be guaranteed that read size is non-zero.
    if (file_offset_of_buffer_end >= read_until_position)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Read boundaries mismatch. Expected {} < {}",
            file_offset_of_buffer_end, read_until_position);

    return read_until_position - file_offset_of_buffer_end;
}

void CachedOnDiskReadBufferFromFile::setReadUntilPosition(size_t position)
{
    if (initialized && !allow_seeks_after_first_read)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Method `setReadUntilPosition()` not allowed");

    if (read_until_position == position)
        return;

    file_offset_of_buffer_end = getPosition();
    resetWorkingBuffer();
    file_segments.reset();
    implementation_buffer.reset();
    initialized = false;
    cache_file_reader.reset();

    read_until_position = position;

    LOG_TEST(log, "Set read_until_position to {}", read_until_position);
}

void CachedOnDiskReadBufferFromFile::setReadUntilEnd()
{
    setReadUntilPosition(getFileSize());
}

off_t CachedOnDiskReadBufferFromFile::getPosition()
{
    return file_offset_of_buffer_end - available();
}

String CachedOnDiskReadBufferFromFile::getInfoForLog()
{
    String current_file_segment_info;
    if (file_segments->empty())
        current_file_segment_info = "None";
    else
        current_file_segment_info = file_segments->front().getInfoForLog();

    return fmt::format(
        "Buffer path: {}, hash key: {}, file_offset_of_buffer_end: {}, read_until_position: {}, "
        "internal buffer end: {}, read_type: {}, last caller: {}, file segment info: {}",
        source_file_path,
        cache_key.toString(),
        file_offset_of_buffer_end,
        read_until_position,
        implementation_buffer ? std::to_string(implementation_buffer->getFileOffsetOfBufferEnd()) : "None",
        toString(read_type),
        last_caller_id,
        current_file_segment_info);
}

bool CachedOnDiskReadBufferFromFile::isSeekCheap()
{
    return !initialized || read_type == ReadType::CACHED;
}

static bool isRangeContainedInSegments(size_t left, size_t right, const FileSegmentsHolderPtr & file_segments)
{
    if (!FileSegment::Range{file_segments->front().range().left, file_segments->back().range().right}.contains({left, right}))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Requested range is not contained in the segments: left={}, right={}, file_segments={}",
            left,
            right,
            file_segments->toString());

    if (!file_segments->front().range().contains(left))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There are redundant segments at the beginning of file_segments");

    const auto end_of_intersection
        = std::ranges::find_if(*file_segments, [right](const auto & segment) { return segment->range().left > right; });

    return std::all_of(
        file_segments->begin(),
        end_of_intersection,
        [right](const auto & segment)
        {
            if (right <= segment->range().right)
            {
                /// We need only a prefix of the last segment (I assume the case when file_segments size is 1 is common enough)
                return right < segment->getCurrentWriteOffset();
            }
            return segment->state() == FileSegment::State::DOWNLOADED;
        });
}

bool CachedOnDiskReadBufferFromFile::isContentCached(size_t offset, size_t size)
{
    if (!initialized)
        initialize();

    if (file_segments->empty())
        return false;

    /// We don't hold all the segments simultaneously, if there are more than `filesystem_cache_segments_batch_size` of them.
    /// So we need to take minimum of the following two values to determine the intersection between [offset, offset + size - 1]
    /// and the range covered by this segment currently.
    const auto right_boundary = std::min(file_segments->back().range().right, read_until_position - 1);
    return isRangeContainedInSegments(offset, std::min(offset + size - 1, right_boundary), file_segments);
}
}
