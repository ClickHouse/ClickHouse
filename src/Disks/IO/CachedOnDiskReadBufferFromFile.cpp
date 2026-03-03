#include <Disks/IO/CachedOnDiskReadBufferFromFile.h>
#include <algorithm>

#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Cached/CachedObjectStorage.h>
#include <Interpreters/Cache/FileCache.h>
#include <IO/BoundedReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromS3.h>
#include <Interpreters/Context.h>
#include <base/hex.h>
#include <base/scope_guard.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/assert_cast.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>
#include <Common/ErrnoException.h>


namespace ProfileEvents
{
extern const Event CachedReadBufferWaitReadBufferMicroseconds;
extern const Event CachedReadBufferReadFromSourceMicroseconds;
extern const Event CachedReadBufferPredownloadedFromSourceMicroseconds;
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
    extern const int UNKNOWN_FILE_SIZE;
}

CachedOnDiskReadBufferFromFile::ReadInfo::ReadInfo(
    const FileCacheKey & cache_key_,
    const std::string & source_file_path_,
    ImplementationBufferCreator impl_creator_,
    bool use_external_buffer_,
    const ReadSettings & read_settings_,
    size_t read_until_position_)
    : cache_key(cache_key_)
    , source_file_path(source_file_path_)
    , implementation_buffer_creator(impl_creator_)
    , use_external_buffer(use_external_buffer_)
    , settings(read_settings_)
    , read_until_position(read_until_position_)
{
}

void CachedOnDiskReadBufferFromFile::ReadInfo::reset()
{
    remote_file_reader.reset();
    cache_file_reader.reset();
    file_segments = {};
}

CachedOnDiskReadBufferFromFile::CachedOnDiskReadBufferFromFile(
    const String & source_file_path_,
    const FileCache::Key & cache_key_,
    FileCachePtr cache_,
    const FileCacheOriginInfo & origin_,
    ImplementationBufferCreator implementation_buffer_creator_,
    const ReadSettings & settings_,
    const String & query_id_,
    size_t file_size_,
    bool allow_seeks_after_first_read_,
    bool use_external_buffer_,
    std::optional<size_t> read_until_position_,
    std::shared_ptr<FilesystemCacheLog> cache_log_)
    : ReadBufferFromFileBase(
        /* buf_size */use_external_buffer_ ? 0 : settings_.remote_fs_buffer_size,
        /* existing_memory */nullptr,
        /* alignment */0,
        /* file_size */file_size_ ? std::optional<size_t>(file_size_) : std::nullopt)
#ifdef DEBUG_OR_SANITIZER_BUILD
    , log(getLogger(fmt::format("CachedOnDiskReadBufferFromFile({})", cache_key_)))
#else
    , log(getLogger("CachedOnDiskReadBufferFromFile"))
#endif
    , cache(cache_)
    , query_id(query_id_)
    , origin(origin_)
    , current_buffer_id(getRandomASCIIString(8))
    , allow_seeks_after_first_read(allow_seeks_after_first_read_)
    , use_external_buffer(use_external_buffer_)
    , cache_log(settings_.enable_filesystem_cache_log ? cache_log_ : nullptr)
    , query_context_holder(cache_->getQueryContextHolder(query_id, settings_))
    , info(
        cache_key_,
        source_file_path_,
        implementation_buffer_creator_,
        use_external_buffer_,
        settings_,
        read_until_position_.value_or(file_size_))
{
    LOG_TEST(
        log, "Cache key: {}, source file path: {}, boundary alignment: {}, "
        "external buffer: {}, allow seeks after first read: {}, file size: {}",
        cache_key_.toString(), source_file_path_,
        settings_.filesystem_cache_boundary_alignment.has_value() ? DB::toString(settings_.filesystem_cache_boundary_alignment.value()) : "None",
        use_external_buffer_, allow_seeks_after_first_read, file_size_);
}

std::optional<size_t> CachedOnDiskReadBufferFromFile::tryGetFileSize()
{
    if (file_size.has_value())
        return file_size;

    file_size = info.implementation_buffer_creator()->tryGetFileSize();
    return file_size;
}

size_t CachedOnDiskReadBufferFromFile::getFileSize()
{
    const auto object_size = tryGetFileSize();
    if (!object_size.has_value())
        throw Exception(ErrorCodes::UNKNOWN_FILE_SIZE, "Cannot get file size for object {}", info.source_file_path);
    return object_size.value();
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
        .source_file_path = info.source_file_path,
        .file_segment_range = { range.left, range.right },
        .requested_range = { first_offset, info.read_until_position },
        .file_segment_key = file_segment.key().toString(),
        .file_segment_offset = file_segment.offset(),
        .file_segment_size = range.size(),
        .read_from_cache_attempted = true,
        .read_buffer_id = current_buffer_id,
        .user_id = origin.user_id,
    };

    switch (type)
    {
        case CachedOnDiskReadBufferFromFile::ReadType::NONE:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Read type cannot be None");
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
    chassert(!info.file_segments || info.file_segments->empty());
    size_t size = getRemainingSizeToRead();
    if (!size)
        return false;

    if (info.settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache)
    {
        info.file_segments = cache->get(
            info.cache_key,
            file_offset_of_buffer_end,
            size,
            info.settings.filesystem_cache_segments_batch_size,
            origin.user_id);
    }
    else
    {
        const auto object_size = getFileSize();
        CreateFileSegmentSettings create_settings(FileSegmentKind::Regular);

        info.file_segments = cache->getOrSet(
            info.cache_key,
            file_offset_of_buffer_end,
            size,
            object_size,
            create_settings,
            info.settings.filesystem_cache_segments_batch_size,
            origin,
            info.settings.filesystem_cache_boundary_alignment);
    }

    return !info.file_segments->empty();
}

void CachedOnDiskReadBufferFromFile::initialize()
{
    if (initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Caching buffer already initialized");

    state.reset();

    /**
     * Segments in returned list are ordered in ascending order and represent a full contiguous
     * interval (no holes). Each segment in returned list has state: DOWNLOADED, DOWNLOADING or EMPTY.
     */
    if (!nextFileSegmentsBatch())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "List of file segments cannot be empty");

    chassert(!info.file_segments->empty());

    LOG_TEST(
        log,
        "Having {} file segments to read: {}, current read range: [{}, {})",
        info.file_segments->size(), info.file_segments->toString(), file_offset_of_buffer_end, info.read_until_position);

    initialized = true;
}

namespace
{

using ReadType = CachedOnDiskReadBufferFromFile::ReadType;
using ReadInfo = CachedOnDiskReadBufferFromFile::ReadInfo;

std::shared_ptr<ReadBufferFromFileBase> getCacheReadBuffer(
    const FileSegment & file_segment,
    ReadInfo & info)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::CachedReadBufferCreateBufferMicroseconds);

    auto path = file_segment.getPath();
    if (info.cache_file_reader)
    {
        chassert(info.cache_file_reader->getFileName() == path);
        if (info.cache_file_reader->getFileName() == path)
            return info.cache_file_reader;

        info.cache_file_reader.reset();
    }

    ReadSettings local_read_settings{info.settings};
    local_read_settings.local_fs_method = LocalFSReadMethod::pread;

    if (info.use_external_buffer)
        local_read_settings.local_fs_buffer_size = 0;

    info.cache_file_reader
        = createReadBufferFromFileBase(path, local_read_settings, std::nullopt, std::nullopt, file_segment.getFlagsForLocalRead());

    if (getFileSizeFromReadBuffer(*info.cache_file_reader) == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read from an empty cache file: {}", path);

    return info.cache_file_reader;
}

std::shared_ptr<ReadBufferFromFileBase> getRemoteReadBuffer(
    FileSegment & file_segment,
    size_t offset,
    ReadType read_type,
    ReadInfo & info)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::CachedReadBufferCreateBufferMicroseconds);

    switch (read_type)
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
                auto impl = info.implementation_buffer_creator();
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
            if (info.remote_file_reader && offset == info.remote_file_reader->getFileOffsetOfBufferEnd())
                return info.remote_file_reader;

            /// Result buffer is owned only by current buffer -- not shareable like in the case above.
            /// We cannot directly check info.remote_file_reader because of a possible race with background downloader.
            auto reader = file_segment.extractRemoteFileReader();
            if (reader && offset == reader->getFileOffsetOfBufferEnd())
                info.remote_file_reader = reader;
            else
                info.remote_file_reader = info.implementation_buffer_creator();

            return info.remote_file_reader;
        }
        default:
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot use remote filesystem reader with read type: {}",
                toString(read_type));
    }
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

CachedOnDiskReadBufferFromFile::ReadFromFileSegmentStatePtr
CachedOnDiskReadBufferFromFile::createReadFromFileSegmentState(
    FileSegment & file_segment,
    size_t offset,
    ReadInfo & info_,
    LoggerPtr log)
{
    auto create = [&](ReadType type, size_t bytes_to_predownload = 0)
    {
        std::shared_ptr<ReadBufferFromFileBase> buf;
        switch (type)
        {
            case ReadType::CACHED:
            {
                buf = getCacheReadBuffer(file_segment, info_);
                break;
            }
            case ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE:
            case ReadType::REMOTE_FS_READ_BYPASS_CACHE:
            {
                buf = getRemoteReadBuffer(file_segment, offset, type, info_);
                break;
            }
            case ReadType::NONE:
                UNREACHABLE();
        }
        return std::make_unique<ReadFromFileSegmentState>(buf, type, bytes_to_predownload);
    };

    auto download_state = file_segment.state();
    if (info_.settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache)
    {
        if (download_state == FileSegment::State::DOWNLOADED)
            return create(ReadType::CACHED);

        LOG_TEST(
            log, "Bypassing cache because "
            "`read_from_filesystem_cache_if_exists_otherwise_bypass_cache` option is used");

        return create(ReadType::REMOTE_FS_READ_BYPASS_CACHE);
    }

    while (true)
    {
        switch (download_state)
        {
            case FileSegment::State::DETACHED:
            {
                LOG_TEST(log, "Bypassing cache because file segment state is `DETACHED`");

                return create(ReadType::REMOTE_FS_READ_BYPASS_CACHE);
            }
            case FileSegment::State::DOWNLOADING:
            {
                if (canStartFromCache(offset, file_segment))
                {
                    ///                      segment{k} state: DOWNLOADING
                    /// cache:           [______|___________
                    ///                         ^
                    ///                         current_write_offset (in progress)
                    /// requested_range:    [__________]
                    ///                     ^
                    ///                     offset
                    return create(ReadType::CACHED);
                }

                download_state = file_segment.wait(offset);
                continue;
            }
            case FileSegment::State::DOWNLOADED:
            {
                return create(ReadType::CACHED);
            }
            case FileSegment::State::EMPTY:
            case FileSegment::State::PARTIALLY_DOWNLOADED:
            {
                if (canStartFromCache(offset, file_segment))
                {
                    ///                      segment{k} state: PARTIALLY_DOWNLOADED
                    /// cache:           [______|___________
                    ///                         ^
                    ///                         current_write_offset (in progress)
                    /// requested_range:    [__________]
                    ///                     ^
                    ///                     offset
                    return create(ReadType::CACHED);
                }

                auto downloader_id = file_segment.getOrSetDownloader();
                if (downloader_id == FileSegment::getCallerId())
                {
                    if (canStartFromCache(offset, file_segment))
                    {
                        ///                      segment{k}
                        /// cache:           [______|___________
                        ///                         ^
                        ///                         current_write_offset
                        /// requested_range:    [__________]
                        ///                     ^
                        ///                     offset

                        file_segment.resetDownloader();
                        return create(ReadType::CACHED);
                    }

                    auto current_write_offset = file_segment.getCurrentWriteOffset();
                    size_t bytes_to_predownload = 0;
                    if (current_write_offset < offset)
                    {
                        ///                   segment{1}
                        /// cache:         [_____|___________
                        ///                      ^
                        ///                      current_write_offset
                        /// requested_range:          [__________]
                        ///                           ^
                        ///                           offset

                        LOG_TEST(log, "Predownload. File segment info: {}", file_segment.getInfoForLog());
                        chassert(offset > current_write_offset);

                        bytes_to_predownload = offset - current_write_offset;
                        chassert(bytes_to_predownload < file_segment.range().size());
                    }

                    return create(ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE, bytes_to_predownload);
                }

                download_state = file_segment.state();
                continue;
            }
            case FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION:
            {
                if (canStartFromCache(offset, file_segment))
                    return create(ReadType::CACHED);

                LOG_TRACE(
                    log,
                    "Bypassing cache because file segment state is "
                    "`PARTIALLY_DOWNLOADED_NO_CONTINUATION` and downloaded part already used");

                return create(ReadType::REMOTE_FS_READ_BYPASS_CACHE);
            }
        }
    }
}

CachedOnDiskReadBufferFromFile::ReadFromFileSegmentStatePtr
CachedOnDiskReadBufferFromFile::prepareReadFromFileSegmentState(
    FileSegment & file_segment,
    size_t offset,
    ReadInfo & info,
    size_t file_size_,
    LoggerPtr log)
{
    const auto range = file_segment.range();

    chassert(
        !file_segment.isDownloader(),
        fmt::format("!isDownloader() failed in prepareReadFromFileSegmentState: {}",
                    getInfoForLog(nullptr, info, offset)));
    chassert(offset >= range.left && offset <= range.right);

    ReadFromFileSegmentStatePtr state;
    {
        Stopwatch watch(CLOCK_MONOTONIC);

        state = createReadFromFileSegmentState(file_segment, offset, info, log);

        watch.stop();

        const auto elapsed = watch.elapsedMicroseconds();
        ProfileEvents::increment(ProfileEvents::CachedReadBufferWaitReadBufferMicroseconds, elapsed);
    }

    LOG_TEST(log, "Current read type: {}, read offset: {}/{}, impl file offset: {}, file segment: {}",
             toString(state->read_type),
             offset,
             file_size_,
             state->buf->getFileOffsetOfBufferEnd(),
             file_segment.getInfoForLog());

    [[maybe_unused]] auto download_current_segment = state->read_type == ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE;
    chassert(download_current_segment == file_segment.isDownloader());

    /// We set position to the end of file segment end,
    /// and not to read_until_position, because in case of concurrent queries
    /// which read the same file segment from different offsets
    /// (same different threads of the same query), it will allow read buffer to be reused,
    /// reducing number of s3 requests. This does apply however only to case when
    /// those different threads hold the file segment at the same time, making its ref count > 2.
    ///
    /// We add min with getFileSize here, because only in case of DistributedCache
    /// we do not resize file segment when write-through cache buffer is destructed,
    /// because we do not know at that moment if all data was fully sent or we just disconnected (this is in TODO to fix).
    /// So here we can have file segment size bigger than actual object size.
    state->buf->setReadUntilPosition(std::min(range.right + 1, file_size_)); /// [..., range.right]

    switch (state->read_type)
    {
        case ReadType::NONE:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Read type not set");
        }
        case ReadType::CACHED:
        {
#ifdef DEBUG_OR_SANITIZER_BUILD
            size_t file_size = getFileSizeFromReadBuffer(*state->buf);
            if (file_size == 0 || range.left + file_size <= offset)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Unexpected state of cache file. Cache file size: {}, cache file offset: {}, "
                    "expected file size to be non-zero and file downloaded size to exceed "
                    "current file read offset (expected: {} > {})",
                    file_size,
                    range.left,
                    range.left + file_size,
                    offset);
#endif

            size_t seek_offset = offset - range.left;

            if (offset < range.left)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Invariant failed. Expected {} > {} (current offset > file segment's start offset)",
                    offset,
                    range.left);

            state->buf->seek(seek_offset, SEEK_SET);
            break;
        }
        case ReadType::REMOTE_FS_READ_BYPASS_CACHE:
        {
            state->buf->seek(offset, SEEK_SET);
            break;
        }
        case ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE:
        {
            chassert(file_segment.isDownloader());

            if (state->bytes_to_predownload)
            {
                const size_t current_write_offset = file_segment.getCurrentWriteOffset();
                state->buf->seek(current_write_offset, SEEK_SET);
            }
            else
            {
                state->buf->seek(offset, SEEK_SET);

                chassert(state->buf->getFileOffsetOfBufferEnd() == offset);
            }

            const auto current_write_offset = file_segment.getCurrentWriteOffset();
            if (current_write_offset != static_cast<size_t>(state->buf->getPosition()))
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Buffer's offsets mismatch. Cached buffer offset: {}, current_write_offset: {}, "
                    "implementation buffer position: {}, implementation buffer end position: {}, file segment info: {}",
                    offset,
                    current_write_offset,
                    state->buf->getPosition(),
                    state->buf->getFileOffsetOfBufferEnd(),
                    file_segment.getInfoForLog());
            }

            break;
        }
    }

    chassert(!state->buf->hasPendingData());
    return state;
}

bool CachedOnDiskReadBufferFromFile::completeFileSegmentAndGetNext()
{
    auto * current_file_segment = &info.file_segments->front();
    auto completed_range = current_file_segment->range();

    if (cache_log)
        appendFilesystemCacheLog(*current_file_segment, state->read_type);

    chassert(file_offset_of_buffer_end > completed_range.right);
    info.cache_file_reader.reset();
    info.remote_file_reader.reset();

    info.file_segments->completeAndPopFront(
        info.settings.filesystem_cache_allow_background_download,
        /*force_shrink_to_downloaded_size=*/false);

    if (info.file_segments->empty() && !nextFileSegmentsBatch())
        return false;

    current_file_segment = &info.file_segments->front();
    current_file_segment->increasePriority();

    state = prepareReadFromFileSegmentState(*current_file_segment, file_offset_of_buffer_end, info, getFileSize(), log);

    LOG_TEST(
        log, "New segment range: {}, old range: {}",
        current_file_segment->range().toString(), completed_range.toString());

    return true;
}

CachedOnDiskReadBufferFromFile::~CachedOnDiskReadBufferFromFile()
{
    if (cache_log && info.file_segments && !info.file_segments->empty())
    {
        appendFilesystemCacheLog(info.file_segments->front(), state->read_type);
    }

    if (info.file_segments && !info.file_segments->empty() && !info.file_segments->front().isCompleted())
    {
        info.file_segments->completeAndPopFront(
            info.settings.filesystem_cache_allow_background_download,
            /*force_shrink_to_downloaded_size=*/false);

        info.file_segments = {};
    }
}

bool CachedOnDiskReadBufferFromFile::predownloadForFileSegment(
    FileSegment & file_segment,
    size_t offset,
    ReadFromFileSegmentState & state,
    ReadInfo & info,
    LoggerPtr log)
{
    OpenTelemetry::SpanHolder span("CachedOnDiskReadBufferFromFile::predownload");
    span.addAttribute("clickhouse.key", file_segment.key().toString());
    span.addAttribute("clickhouse.size", state.bytes_to_predownload);

    if (state.bytes_to_predownload)
    {
        /// Consider this case. Some user needed segment [a, b] and downloaded it partially.
        /// But before he called complete(state) or his holder called complete(),
        /// some other user, who needed segment [a', b'], a < a' < b', started waiting on [a, b] to be
        /// downloaded because it intersects with the range he needs.
        /// But then first downloader fails and second must continue. In this case we need to
        /// download from offset a'' < a', but return buffer from offset a'.
        LOG_TEST(log, "Bytes to predownload: {}, caller_id: {}, buffer size: {}",
                 state.bytes_to_predownload, FileSegment::getCallerId(), state.buf->internalBuffer().size());

        size_t current_write_offset = file_segment.getCurrentWriteOffset();

        chassert(static_cast<size_t>(state.buf->getPosition()) == current_write_offset);
        chassert(state.buf->getFileOffsetOfBufferEnd() == current_write_offset);
        chassert(current_write_offset == offset - state.bytes_to_predownload);

        char * initial_buffer = state.buf->internalBuffer().begin();
        size_t initial_buffer_size = state.buf->internalBuffer().size();
        chassert(initial_buffer && initial_buffer_size);

        char *  predownload_buffer;
        size_t predownload_buffer_size;
        if (initial_buffer_size < DBMS_DEFAULT_BUFFER_SIZE)
        {
            state.predownload_memory.resize(std::min<size_t>(state.bytes_to_predownload, DBMS_DEFAULT_BUFFER_SIZE));
            predownload_buffer = state.predownload_memory.data();
            predownload_buffer_size = state.predownload_memory.size();
        }
        else
        {
            predownload_buffer = initial_buffer;
            predownload_buffer_size = initial_buffer_size;
        }

        chassert(initial_buffer);
        chassert(predownload_buffer && predownload_buffer_size);

        SCOPE_EXIT({
            state.buf->set(initial_buffer, initial_buffer_size);
        });

        while (true)
        {
            bool has_more_data = false;
            if (state.bytes_to_predownload)
            {
                Stopwatch watch(CLOCK_MONOTONIC);

                predownload_buffer_size = std::min(predownload_buffer_size, state.bytes_to_predownload);
                state.buf->set(predownload_buffer, predownload_buffer_size);
                has_more_data = !state.buf->eof();

                watch.stop();
                auto elapsed = watch.elapsedMicroseconds();
                ProfileEvents::increment(ProfileEvents::CachedReadBufferPredownloadedFromSourceMicroseconds, elapsed);
            }

            if (!state.bytes_to_predownload || !has_more_data)
            {
                if (state.bytes_to_predownload)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Failed to predownload remaining {} bytes. Current file segment: {}, "
                        "current download offset: {}, expected: {}, eof: {}, internal buffer size: {}",
                        state.bytes_to_predownload,
                        file_segment.range().toString(),
                        file_segment.getCurrentWriteOffset(),
                        offset,
                        state.buf->eof(),
                        state.buf->internalBuffer().size());

                chassert(!state.buf->hasPendingData());
                auto result = state.buf->hasPendingData();
                if (result)
                {
                    const auto write_offset = file_segment.getCurrentWriteOffset();
                    if (write_offset != static_cast<size_t>(state.buf->getPosition())
                        || write_offset != offset)
                    {
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR,
                            "Buffer's offsets mismatch after predownloading; download offset: {}, "
                            "cached buffer offset: {}, implementation buffer offset: {}, "
                            "file segment info: {}",
                            write_offset,
                            offset,
                            state.buf->getPosition(),
                            file_segment.getInfoForLog());
                    }
                }

                break;
            }

            size_t size = state.buf->buffer().size();
            chassert(size == state.buf->available());
            chassert(size <= state.bytes_to_predownload);

            ProfileEvents::increment(ProfileEvents::CachedReadBufferReadFromSourceBytes, size);
            ProfileEvents::increment(ProfileEvents::CachedReadBufferPredownloadedBytes, size);

            std::string failure_reason;
            bool continue_predownload = file_segment.reserve(
                size,
                info.settings.filesystem_cache_reserve_space_wait_lock_timeout_milliseconds,
                failure_reason);

            if (continue_predownload)
            {
                LOG_TEST(log, "Left to predownload: {}, buffer size: {}", state.bytes_to_predownload, size);

                chassert(file_segment.getCurrentWriteOffset() == static_cast<size_t>(state.buf->getPosition()));

                continue_predownload = writeCache(
                    state.buf->buffer().begin(),
                    size,
                    current_write_offset,
                    file_segment,
                    log);

                if (continue_predownload)
                {
                    current_write_offset += size;
                    state.buf->position() += size;
                    chassert(state.bytes_to_predownload >= size);
                    state.bytes_to_predownload -= size;
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

                state.bytes_to_predownload = 0;
                file_segment.completePartAndResetDownloader();
                chassert(file_segment.state() == FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);

                LOG_TEST(log, "Bypassing cache for {}", file_segment.getInfoForLog());

                state.read_type = ReadType::REMOTE_FS_READ_BYPASS_CACHE;

                LOG_TRACE(
                    log,
                    "Predownload failed because of space limit. "
                    "Will read from remote filesystem starting from offset: {}",
                    offset);

                return false;
            }
        }
    }
    return true;
}

void CachedOnDiskReadBufferFromFile::updateReadStateIfNeeded(
    FileSegment & file_segment,
    size_t offset,
    ReadFromFileSegmentStatePtr & state,
    ReadInfo & info,
    size_t file_size_,
    LoggerPtr log)
{
    if (state->read_type == ReadType::CACHED && file_segment.state() != FileSegment::State::DOWNLOADED)
    {
        /// If current read_type is ReadType::CACHED and file segment is not DOWNLOADED,
        /// it means the following case, e.g. we started from CacheReadBuffer and continue with RemoteFSReadBuffer.
        ///                  segment{k}
        /// cache:           [______|___________]
        ///                         ^
        ///                         current_write_offset
        /// requested_range:    [__________
        ///                     ^
        ///                     offset

        if (offset >= file_segment.getCurrentWriteOffset())
            state = prepareReadFromFileSegmentState(file_segment, offset, info, file_size_, log);
    }
    else if (state->read_type == ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE)
    {
        /**
        * ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE means that on previous prepareReadFromFileSegmentState() call
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
        state = prepareReadFromFileSegmentState(file_segment, offset, info, file_size_, log);
    }
}

bool CachedOnDiskReadBufferFromFile::updateImplementationBufferIfNeeded()
{
    chassert(!info.file_segments->empty());
    auto & file_segment = info.file_segments->front();
    const auto & current_read_range = file_segment.range();

    chassert(current_read_range.left <= file_offset_of_buffer_end);
    chassert(!file_segment.isDownloader(), getInfoForLog());

    if (file_offset_of_buffer_end > current_read_range.right)
        return completeFileSegmentAndGetNext();

    updateReadStateIfNeeded(file_segment, file_offset_of_buffer_end, state, info, getFileSize(), log);
    return true;
}

bool CachedOnDiskReadBufferFromFile::writeCache(
    char * data,
    size_t size,
    size_t offset,
    FileSegment & file_segment,
    LoggerPtr log)
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
    chassert(
        file_offset_of_buffer_end <= info.read_until_position,
        fmt::format("file_offset_of_buffer_end {}, read_until_position {}",
                    file_offset_of_buffer_end, info.read_until_position));

    if (file_offset_of_buffer_end == info.read_until_position)
        return false;

    if (!initialized)
        initialize();

    if (info.file_segments->empty() && !nextFileSegmentsBatch())
        return false;

    chassert(!internal_buffer.empty());
    const size_t original_buffer_size = internal_buffer.size();
    if (!original_buffer_size)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Internal buffer cannot be empty (use external buffer: {}; {})",
            use_external_buffer, getInfoForLog());
    }

    bool implementation_buffer_can_be_reused = false;
    SCOPE_EXIT({
        try
        {
            /// Save state of current file segment before it is completed. But we'll use it only if exception happened.
            if (std::uncaught_exceptions() > 0)
                nextimpl_step_log_info = getInfoForLog();

            chassert(!internal_buffer.empty());
            chassert(internal_buffer.begin());

            if (info.file_segments->empty())
                return;

            auto & file_segment = info.file_segments->front();

            if (file_segment.isDownloader())
            {
                if (!implementation_buffer_can_be_reused)
                    file_segment.resetRemoteFileReader();

                file_segment.completePartAndResetDownloader();
            }

            if (use_external_buffer && !internal_buffer.empty())
                internal_buffer.resize(original_buffer_size);

            chassert(!file_segment.isDownloader(), "!isDownloader() failed in scope exit: " + getInfoForLog());
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    });

    if (state && state->buf)
    {
        bool can_read_further = updateImplementationBufferIfNeeded();
        if (!can_read_further)
            return false;
    }
    else
    {
        state = prepareReadFromFileSegmentState(info.file_segments->front(), file_offset_of_buffer_end, info, getFileSize(), log);
        info.file_segments->front().increasePriority();
    }

    auto & file_segment = info.file_segments->front();
    const auto & current_read_range = file_segment.range();

    if (use_external_buffer && state->read_type == ReadType::CACHED)
    {
        /// We allocate buffers not less than 1M so that s3 requests will not be too small. But the same buffers (members of AsynchronousReadIndirectBufferFromRemoteFS)
        /// are used for reading from files. Some of these readings are fairly small and their performance degrade when we use big buffers (up to ~20% for queries like Q23 from ClickBench).
        if (info.settings.local_fs_buffer_size && info.settings.local_fs_buffer_size < internal_buffer.size())
            internal_buffer.resize(info.settings.local_fs_buffer_size);

        /// It would make sense to reduce buffer size to what is left to read
        /// (when we read the last segment) regardless of the read_type.
        /// But we have to use big enough buffers when we [pre]download segments
        /// to amortize netw and FileCache overhead (space reservation and relevant locks).
        if (info.file_segments->size() == 1)
        {
            const size_t remaining_size_to_read
                = std::min(current_read_range.right, info.read_until_position - 1) - file_offset_of_buffer_end + 1;
            const size_t new_buf_size = std::min(internal_buffer.size(), remaining_size_to_read);

            chassert((internal_buffer.size() >= new_buf_size) && (new_buf_size > 0));
            internal_buffer.resize(nextimpl_working_buffer_offset + new_buf_size);
        }
    }

    state->buf->set(internal_buffer.begin(), internal_buffer.size());
    size_t size = 0;
    {
        SCOPE_EXIT({
            state->buf->set(nullptr, 0);
        });

        size = readFromFileSegment(
            file_segment,
            file_offset_of_buffer_end,
            getFileSize(),
            *state,
            info,
            implementation_buffer_can_be_reused,
            log);

        chassert(state->buf->buffer().begin() == internal_buffer.begin());
        chassert(state->buf->available() == size);
    }

    /// We don't support state.buf implementations that use nextimpl_working_buffer_offset.
    chassert(!nextimpl_working_buffer_offset);

    if (size)
        file_offset_of_buffer_end += size;

    working_buffer = Buffer(internal_buffer.begin(), internal_buffer.begin() + size);

    if (file_segment.isDownloader())
        file_segment.completePartAndResetDownloader();

    chassert(!file_segment.isDownloader(), "!isDownloader() failed in the end of nextImpl: " + getInfoForLog());

    if (file_offset_of_buffer_end > current_read_range.right)
        completeFileSegmentAndGetNext();

    return size;
}

size_t CachedOnDiskReadBufferFromFile::readFromFileSegment(
    FileSegment & file_segment,
    size_t offset,
    size_t file_size_,
    ReadFromFileSegmentState & state,
    ReadInfo & info,
    bool & implementation_buffer_can_be_reused,
    LoggerPtr log)
{
    LOG_TEST(log, "Reading file segment: {}", getInfoForLog(&state, info, offset));

    const auto & current_read_range = file_segment.range();
    chassert(current_read_range.contains(offset));

    size_t size = 0;
    if (state.bytes_to_predownload)
    {
        if (!predownloadForFileSegment(file_segment, offset, state, info, log))
        {
            chassert(!state.buf->available());
            chassert(state.read_type == ReadType::REMOTE_FS_READ_BYPASS_CACHE);

            auto buf = getRemoteReadBuffer(file_segment, offset, ReadType::REMOTE_FS_READ_BYPASS_CACHE, info);
            buf->setReadUntilPosition(file_segment.range().right + 1); /// [..., range.right]
            buf->seek(offset, SEEK_SET);
            buf->swap(*state.buf);
            state.buf = buf;

            chassert(!state.buf->internalBuffer().empty());
        }
    }

#ifdef DEBUG_OR_SANITIZER_BUILD
    chassert(state.buf->available() == 0);
    if (state.read_type != ReadType::CACHED)
        chassert(state.buf->getFileOffsetOfBufferEnd() == offset);
#endif

    auto do_download = state.read_type == ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE;
    if (do_download != file_segment.isDownloader())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Incorrect segment state. Having read type: {}, file segment info: {}",
            toString(state.read_type), file_segment.getInfoForLog());
    }

    if (!size)
    {
#ifdef DEBUG_OR_SANITIZER_BUILD
        if (state.read_type == ReadType::CACHED)
        {
            size_t cache_file_size = getFileSizeFromReadBuffer(*state.buf);
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
            chassert(offset == static_cast<size_t>(state.buf->getFileOffsetOfBufferEnd()));
        }
        chassert(!state.buf->hasPendingData());
#endif

        Stopwatch watch(CLOCK_MONOTONIC);

        if (state.buf->internalBuffer().empty())
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Internal buffer cannot be empty (use external buffer: {}; {})",
                info.use_external_buffer, getInfoForLog(&state, info, offset));
        }

        auto result = state.buf->next();

        watch.stop();
        auto elapsed = watch.elapsedMicroseconds();

        chassert(state.buf->position() == state.buf->buffer().begin());

        if (result)
        {
            size = state.buf->buffer().size();
        }

        if (state.read_type == ReadType::CACHED)
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

    if (size)
    {
        chassert(state.buf->available());
        bool download_current_segment_succeeded = false;
        if (do_download)
        {
            chassert(
                offset + size - 1 <= file_segment.range().right,
                fmt::format("Offset: {}, size: {}, file segment range: {}, impl offset: {}", offset, size, file_segment.range().toString(), state.buf->getPosition()));

            std::string failure_reason;
            bool success = file_segment.reserve(
                size,
                info.settings.filesystem_cache_reserve_space_wait_lock_timeout_milliseconds,
                failure_reason);

            if (success)
            {
                chassert(file_segment.getCurrentWriteOffset() == static_cast<size_t>(state.buf->getPosition()));

                success = writeCache(state.buf->buffer().begin(), size, offset, file_segment, log);
                if (success)
                {
                    chassert(file_segment.getCurrentWriteOffset() <= file_segment.range().right + 1);
                    chassert(
                        /* last_file_segment */info.file_segments->size() == 1
                        || file_segment.getCurrentWriteOffset() == state.buf->getFileOffsetOfBufferEnd());

                    LOG_TEST(log, "Successfully written {} bytes", size);
                    download_current_segment_succeeded = true;

                    // The state.buf is valid and positioned correctly (at file_segment->getCurrentWriteOffset()).
                    // Later reads for this file segment can reuse it.
                    // (It's reusable even if we don't reach the swap(*state.buf) below,
                    // because the reuser must assign state.buf's buffer anyway.)
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
                state.read_type = ReadType::REMOTE_FS_READ_BYPASS_CACHE;
                chassert(file_segment.state() == FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);
            }
        }

        /// - If last file segment was read from remote fs, then we read up to segment->range().right,
        /// but the requested right boundary could be
        /// segment->range().left < requested_right_boundary <  segment->range().right.
        /// Therefore need to resize to a smaller size. And resize must be done after write into cache.
        /// - If last file segment was read from local fs, then we could read more than
        /// file_segemnt->range().right, so resize is also needed.
        if (info.file_segments->size() == 1)
        {
            size_t remaining_size_to_read = std::min(current_read_range.right, info.read_until_position - 1) - offset + 1;

            LOG_TEST(log, "Remaining size to read: {}, read: {}. Resizing buffer to {}",
                     remaining_size_to_read, size, state.buf->offset() + std::min(size, remaining_size_to_read));

            if (size > remaining_size_to_read)
            {
                size = remaining_size_to_read;
                chassert(state.buf->buffer().size() >= size);
                state.buf->buffer().resize(size);
            }
        }

        if (do_download && download_current_segment_succeeded)
            chassert(file_segment.getCurrentWriteOffset() >= offset + size);

        chassert(
            offset + size <= info.read_until_position,
            fmt::format("Expected {} <= {} (size: {}, read range: {}, hold file segments: {} ({}))",
                        offset + size, info.read_until_position, size, current_read_range.toString(),
                        info.file_segments->size(), info.file_segments->toString(true)));
    }

    if (size)
        chassert(state.buf->available());

    if (size == 0 && offset + size < info.read_until_position)
    {
        size_t cache_file_size = getFileSizeFromReadBuffer(*state.buf);
        auto cache_file_path = getFileNameFromReadBuffer(*state.buf);

        std::string download_finished_time;
        if (file_segment.isDownloaded())
        {
            WriteBufferFromString buf{download_finished_time};
            writeDateTimeText(file_segment.getFinishedDownloadTime(), buf);
        }
        else
            download_finished_time = "None";

        std::optional<size_t> object_size;
        std::optional<size_t> impl_read_until_position;
        std::optional<std::string> impl_read_stop_reason;
        if (state.read_type != ReadType::CACHED)
        {
#if USE_AWS_S3
            if (const auto * s3_buf = dynamic_cast<const ReadBufferFromS3 *>(state.buf.get()))
            {
                impl_read_until_position = s3_buf->getReadUntilPosition();
                impl_read_stop_reason = s3_buf->getStopReason();
                object_size = s3_buf->getObjectSizeFromS3();
            }
#endif
        }

        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Having zero bytes, but range is not finished: "
            "file offset: {}, "
            "read bytes: {}, "
            "object size: {}, "
            "expected object size: {}, "
            "reading until: {}, "
            "read type: {}, "
            "impl read stop reason: {}, "
            "impl working buffer size: {}, "
            "impl internal buffer size: {}, "
            "impl available: {}, "
            "impl offset: {}, "
            "impl last position: {}, "
            "impl eof: {}, "
            "finished download time: {}, "
            "cache file size: {}, "
            "cache file path: {}, "
            "remaining file segments: {}, "
            "current file segment: {}",
            offset + size,
            size,
            object_size ? std::to_string(*object_size) : "None",
            file_size_,
            info.read_until_position,
            toString(state.read_type),
            impl_read_stop_reason ? *impl_read_stop_reason : "None",
            state.buf->buffer().size(),
            state.buf->internalBuffer().size(),
            state.buf->available(),
            state.buf->getFileOffsetOfBufferEnd(),
            impl_read_until_position ? std::to_string(*impl_read_until_position) : "None",
            state.buf->eof(),
            download_finished_time,
            cache_file_size ? std::to_string(cache_file_size) : "None",
            cache_file_path,
            info.file_segments->size(),
            file_segment.getInfoForLog());
    }

    // No necessary because of the SCOPE_EXIT above, but useful for logging below.
    LOG_TEST(log, "Read {} bytes (buffer size: {}). Read info: {}",
             size, state.buf->internalBuffer().size(), getInfoForLog(&state, info, offset + size));

    return size;
}

size_t CachedOnDiskReadBufferFromFile::readBigAt(
    char * to,
    size_t n,
    size_t range_begin,
    const std::function<bool(size_t)> & progress_callback) const
{
    ReadInfo current_info(
        info.cache_key, info.source_file_path, info.implementation_buffer_creator,
        info.use_external_buffer, info.settings, /* read_until_position */range_begin + n);

    if (info.settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache)
    {
        current_info.file_segments = cache->get(
            info.cache_key,
            /* offset */range_begin,
            /* size */n,
            /* batch_size */0,
            origin.user_id);
    }
    else
    {
        CreateFileSegmentSettings create_settings(FileSegmentKind::Regular);
        current_info.file_segments = cache->getOrSet(
            info.cache_key,
            /* offset */range_begin,
            /* size */n,
            file_size.value(),
            create_settings,
            /* batch_size */0,
            origin);
    }

    if (current_info.file_segments->empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No file segments");

    LOG_TEST(
        log, "ReadBigAt() at offset {} size {} file segments {}",
        range_begin, n, current_info.file_segments->size());

    size_t read_bytes = 0;
    size_t offset = range_begin;
    bool cancelled = false;
    bool implementation_buffer_can_be_reused = false;
    ReadFromFileSegmentStatePtr current_state;
    auto object_size = const_cast<CachedOnDiskReadBufferFromFile &>(*this).getFileSize();

    SCOPE_EXIT({
        if (current_info.file_segments->empty())
            return;

        auto & file_segment = current_info.file_segments->front();
        if (file_segment.isDownloader())
            file_segment.completePartAndResetDownloader();
    });

    while (!cancelled && read_bytes < n)
    {
        if (current_info.file_segments->empty())
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Unexpected end of file segments. Offset: {}, read: {}/{}",
                offset, read_bytes, n);
        }

        auto & file_segment = current_info.file_segments->front();

        const size_t file_segment_end_non_included = file_segment.range().right + 1;
        chassert(offset <= file_segment_end_non_included);
        if (offset == file_segment_end_non_included)
        {
            current_info.cache_file_reader.reset();
            current_info.file_segments->completeAndPopFront(
                info.settings.filesystem_cache_allow_background_download,
                /* force_shrink_to_downloaded_size */false);

            current_state.reset();
            continue;
        }

        chassert(
            file_segment.range().contains(offset),
            fmt::format("Current offset: {}, file segment: {}", offset, file_segment.getInfoForLog()));

        if (current_state)
        {
            updateReadStateIfNeeded(file_segment, offset, current_state, current_info, object_size, log);
        }
        else
        {
            file_segment.increasePriority();
            current_state = prepareReadFromFileSegmentState(
                file_segment,
                offset,
                current_info,
                object_size,
                log);
        }

        [[maybe_unused]] size_t remaining_size_in_file_segment = file_segment.range().right - offset + 1;
        current_state->buf->set(to + read_bytes, n - read_bytes);
        SCOPE_EXIT({
            current_state->buf->set(nullptr, 0);
        });

        const auto size = readFromFileSegment(
            file_segment,
            offset,
            object_size,
            *current_state,
            current_info,
            implementation_buffer_can_be_reused,
            log);

        LOG_TEST(
            log, "ReadBigAt() read {} bytes at offset: {}. Total: {}/{}",
            size, offset, read_bytes + size, n);

        offset += size;
        read_bytes += size;

        chassert(read_bytes <= n);
        chassert(
            offset <= file_segment.range().right + 1,
            fmt::format(
                "Offset: {}, size: {}, remaining size in file segment: {}, file segment: {}",
                offset, size, remaining_size_in_file_segment, file_segment.getInfoForLog()));

        if (!size)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot read all data. Offset: {} (initial offset: {}), read bytes {}/{}",
                offset, range_begin, read_bytes, n);
        }

        if (progress_callback)
            cancelled = progress_callback(size);
    }

    return read_bytes;
}

off_t CachedOnDiskReadBufferFromFile::seek(off_t offset, int whence)
{
    if (offset == getPosition() && whence == SEEK_SET)
        return offset;

    if (initialized && !allow_seeks_after_first_read)
    {
        throw Exception(
            ErrorCodes::CANNOT_SEEK_THROUGH_FILE,
            "Seek is allowed only before first read attempt from the buffer. "
            "Current offset {}, seek offset: {}", file_offset_of_buffer_end, offset);
    }

    size_t new_pos = offset;

    if (allow_seeks_after_first_read)
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

    first_offset = file_offset_of_buffer_end = new_pos;

    info.reset();
    state.reset();
    initialized = false;

    LOG_TEST(log, "Reset state for seek to position {} (allow_seeks_after_first_reader = {})", new_pos, allow_seeks_after_first_read);

    return new_pos;
}

size_t CachedOnDiskReadBufferFromFile::getRemainingSizeToRead()
{
    /// Last position should be guaranteed to be set, as at least we always know file size.
    if (!info.read_until_position)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Last position was not set");

    /// On this level should be guaranteed that read size is non-zero.
    if (file_offset_of_buffer_end > info.read_until_position)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Read boundaries mismatch. Expected {} < {}",
            file_offset_of_buffer_end, info.read_until_position);

    return info.read_until_position - file_offset_of_buffer_end;
}

void CachedOnDiskReadBufferFromFile::setReadUntilPosition(size_t position)
{
    if (initialized && !allow_seeks_after_first_read)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Method `setReadUntilPosition()` not allowed");

    if (info.read_until_position == position)
        return;

    file_offset_of_buffer_end = getPosition();

    info.reset();
    state.reset();
    initialized = false;

    info.read_until_position = position;
    LOG_TEST(log, "Set read_until_position to {}", info.read_until_position);
}

void CachedOnDiskReadBufferFromFile::setReadUntilEnd()
{
    setReadUntilPosition(getFileSize());
}

off_t CachedOnDiskReadBufferFromFile::getPosition()
{
    return file_offset_of_buffer_end - available();
}

std::string CachedOnDiskReadBufferFromFile::getInfoForLog()
{
    return getInfoForLog(state.get(), info, file_offset_of_buffer_end);
}

std::string CachedOnDiskReadBufferFromFile::getInfoForLog(
    const ReadFromFileSegmentState * state,
    const ReadInfo & info,
    size_t offset)
{
    WriteBufferFromOwnString wb;
    wb << "key: " << info.cache_key.toString() << ", ";
    wb << "source_path: " << info.source_file_path << ", ";
    wb << "offset: " << offset << "/" << info.read_until_position << ", ";

    if (state)
    {
        wb << "read_type: " << toString(state ? state->read_type : ReadType::NONE) << ", ";
        wb << "bytes_to_predownload: " << state->bytes_to_predownload << ", ";
        if (state->buf)
        {
            wb << "buf.available: " << state->buf->available() << ", ";
            wb << "buf.offset: " << state->buf->offset() << ", ";
            wb << "buf.size: " << state->buf->buffer().size() << ", ";
            wb << "buf.buffer_end_offset: " << state->buf->getFileOffsetOfBufferEnd() << ", ";
        }
    }

    wb << "file segments: " << info.file_segments->size();
    if (!info.file_segments->empty())
        wb << " (front: " << info.file_segments->front().getInfoForLog() << ")";

    return wb.str();
}

bool CachedOnDiskReadBufferFromFile::isSeekCheap()
{
    return !initialized || (state && state->read_type == ReadType::CACHED);
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

    if (info.file_segments->empty())
        return false;

    /// We don't hold all the segments simultaneously, if there are more than `filesystem_cache_segments_batch_size` of them.
    /// So we need to take minimum of the following two values to determine the intersection between [offset, offset + size - 1]
    /// and the range covered by this segment currently.
    const auto right_boundary = std::min(info.file_segments->back().range().right, info.read_until_position - 1);
    return isRangeContainedInSegments(offset, std::min(offset + size - 1, right_boundary), info.file_segments);
}

}
