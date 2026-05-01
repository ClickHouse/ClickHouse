#include <Storages/MergeTree/RemoteReadingManager.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <IO/ReadBufferFromRRM.h>
#include <Common/CurrentThread.h>
#include <Common/ErrnoException.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/ThreadPool.h>

#include <fmt/format.h>
#include <future>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
}

ReadScope::ReadScope(String part_id_, MarkRanges mark_ranges_, Phase phase_,
                     std::vector<ByteRange> reading_ranges_,
                     std::vector<size_t> cache_pre_padding_bytes_)
    : part_id(std::move(part_id_))
    , mark_ranges(std::move(mark_ranges_))
    , phase(phase_)
    , reading_ranges(std::move(reading_ranges_))
    , cache_pre_padding_bytes(std::move(cache_pre_padding_bytes_))
    , thread_group(CurrentThread::getGroup())
{
}

ReadScope::ReadScope(String part_id_, MarkRanges mark_ranges_, Phase phase_,
                     std::vector<ByteRange> reading_ranges_,
                     std::vector<size_t> cache_pre_padding_bytes_,
                     ThreadGroupPtr thread_group_,
                     size_t encryption_header_bytes_)
    : part_id(std::move(part_id_))
    , mark_ranges(std::move(mark_ranges_))
    , phase(phase_)
    , reading_ranges(std::move(reading_ranges_))
    , cache_pre_padding_bytes(std::move(cache_pre_padding_bytes_))
    , encryption_header_bytes(encryption_header_bytes_)
    , thread_group(std::move(thread_group_))
{
}

std::shared_ptr<const ReadScope> ReadScope::create(
    String part_id_, MarkRanges mark_ranges_, Phase phase_,
    std::vector<ByteRange> reading_ranges_,
    std::vector<size_t> cache_pre_padding_bytes_)
{
    /// Cannot use make_shared — constructor is private.
    return ReadScopePtr(new ReadScope(
        std::move(part_id_), std::move(mark_ranges_), phase_,
        std::move(reading_ranges_), std::move(cache_pre_padding_bytes_)));
}

String ReadScope::toString() const
{
    String result;
    result += "part_id=" + part_id;
    result += " phase=" + String(phase == Phase::Prewhere ? "Prewhere" : "Rest");
    result += " mark_ranges=[";
    for (size_t i = 0; i < mark_ranges.size(); ++i)
    {
        if (i > 0)
            result += ", ";
        result += std::to_string(mark_ranges[i].begin) + ":" + std::to_string(mark_ranges[i].end);
    }
    result += "]";
    result += " bytes=" + std::to_string([&]
    {
        size_t total = 0;
        for (const auto & r : reading_ranges)
            total += r.end - r.begin;
        return total;
    }());
    if (encryption_header_bytes)
        result += " enc_header=" + std::to_string(encryption_header_bytes);
    return result;
}

std::shared_ptr<const ReadScope> ReadScope::adjustForObject(size_t object_start_offset, size_t object_size) const
{
    /// Fast path: object starts at file offset 0 and covers all granule ranges.
    if (object_start_offset == 0
        && (reading_ranges.empty() || reading_ranges.back().end <= object_size))
        return shared_from_this();

    size_t object_end = object_start_offset + object_size;

    std::vector<ByteRange> adjusted;
    adjusted.reserve(reading_ranges.size());
    for (const auto & r : reading_ranges)
    {
        if (r.end <= object_start_offset)
            continue;  /// Entirely before this object.
        if (r.begin >= object_end)
            break;  /// Past this object — ranges are ordered.

        size_t clamped_begin = std::max(r.begin, object_start_offset);
        size_t clamped_end = std::min(r.end, object_end);
        adjusted.push_back({clamped_begin - object_start_offset, clamped_end - object_start_offset});
    }
    return std::make_shared<ReadScope>(part_id, mark_ranges, phase, std::move(adjusted), std::vector<size_t>{}, thread_group, encryption_header_bytes);
}

std::shared_ptr<const ReadScope> ReadScope::withEncryptionHeader(size_t header_bytes) const
{
    if (header_bytes == 0)
        return shared_from_this();

    return std::make_shared<ReadScope>(
        part_id, mark_ranges, phase, reading_ranges, cache_pre_padding_bytes, thread_group, header_bytes);
}

RemoteReadingManager & RemoteReadingManager::instance()
{
    static RemoteReadingManager manager;
    return manager;
}

std::unique_ptr<ReadBufferFromFileBase> RemoteReadingManager::createReadBuffer(
    ReadScopePtr scope,
    const IDataPartStorage & storage,
    const String & file_name,
    const ReadSettings & settings,
    std::optional<size_t> estimated_size)
{
    LOG_DEBUG(log, "createReadBuffer: file={} scope=[{}]", file_name, scope->toString());

    /// Pass the scope down through ReadSettings so that object storage
    /// implementations can call back into createObjectReadBuffer.
    ReadSettings patched_settings = settings;
    patched_settings.read_scope = scope;

    return storage.readFile(file_name, patched_settings, estimated_size);
}

std::unique_ptr<ReadBufferFromFileBase> RemoteReadingManager::createObjectReadBuffer(
    const ReadScope & scope,
    const StoredObject & object,
    RemoteConnectionFactory connection_factory)
{
    /// Compute the byte range to prefetch from reading_ranges.
    /// All granules may not be contiguous, but for the prototype we fetch
    /// the bounding range [min_begin, max_end) in a single HTTP request.
    if (scope.reading_ranges.empty())
    {
        LOG_DEBUG(log, "createObjectReadBuffer: no reading_ranges, falling back. blob={} scope=[{}]",
            object.remote_path, scope.toString());
        return nullptr;
    }

    /// Find the first non-empty reading range.  The prefix range [0, 0)
    /// (for columns without prefix data) is empty and must be skipped.
    size_t first_nonempty = 0;
    while (first_nonempty < scope.reading_ranges.size()
           && scope.reading_ranges[first_nonempty].begin == scope.reading_ranges[first_nonempty].end)
        ++first_nonempty;

    /// The padding for the first non-empty range might be recorded on an
    /// earlier empty range (e.g. [0,0) prefix or a zero-width granule).
    /// Take the maximum padding across all ranges up to and including
    /// the first non-empty one.
    size_t first_padding = 0;
    for (size_t i = 0; i <= first_nonempty && i < scope.cache_pre_padding_bytes.size(); ++i)
        first_padding = std::max(first_padding, scope.cache_pre_padding_bytes[i]);
    size_t range_begin = scope.reading_ranges[first_nonempty].begin - first_padding;
    size_t range_end = scope.reading_ranges.back().end + scope.encryption_header_bytes;

    /// If there is a non-empty prefix range before the first granule range,
    /// extend the fetch to cover it (e.g. LowCardinality shared dictionary).
    if (first_nonempty > 0 && scope.reading_ranges.front().end > 0)
        range_begin = std::min(range_begin, scope.reading_ranges.front().begin);

    /// When the object has an encryption header, always fetch from offset 0
    /// so that `DiskEncrypted::readFile` can read the header before wrapping.
    if (scope.encryption_header_bytes > 0)
        range_begin = 0;
    size_t range_size = range_end - range_begin;

    LOG_DEBUG(log, "createObjectReadBuffer: blob={} object_bytes_size={} range=[{}, {}) size={} enc_header={} scope=[{}]",
        object.remote_path, object.bytes_size, range_begin, range_end, range_size,
        scope.encryption_header_bytes, scope.toString());

    /// Launch async prefetch via ThreadFromGlobalPool (not std::async) so that
    /// the worker thread has a proper ThreadStatus, which ThreadGroupSwitcher requires.
    /// The thread group comes from the scope (captured at ReadScope creation time
    /// in the query thread) so log messages get the correct query_id.
    auto thread_group = scope.thread_group;
    auto promise = std::make_shared<std::promise<Memory<>>>();
    auto future = promise->get_future();

    ThreadFromGlobalPool([promise, factory = std::move(connection_factory),
                          range_begin, range_end, range_size,
                          key = object.remote_path, thread_group]()
        {
            ThreadGroupSwitcher switcher(thread_group, ThreadName::RRM_PREFETCH);
            try
            {
                LoggerPtr prefetch_log = getLogger("RRMPrefetch");

                static constexpr size_t max_attempts = 20;
                Memory<> buffer(range_size);
                size_t bytes_read = 0;

                for (size_t attempt = 0; attempt < max_attempts && bytes_read < range_size; ++attempt)
                {
                    size_t current_begin = range_begin + bytes_read;
                    auto conn = factory(current_begin, range_end, attempt);

                    LOG_DEBUG(prefetch_log, "Connection opened: key={} fd={} has_body={} range=[{}, {}) attempt={}",
                        key, conn.fd, conn.body_stream != nullptr, current_begin, range_end, attempt);

                    if (!conn.body_stream)
                        throw Exception(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR,
                            "RRM prefetch: no body stream for {}", key);

                    /// Read from the HTTP body stream (not the raw FD).
                    /// The body stream includes data from Poco's internal buffer
                    /// and handles chunked transfer encoding transparently.
                    while (bytes_read < range_size)
                    {
                        conn.body_stream->read(buffer.m_data + bytes_read, range_size - bytes_read);
                        auto n = conn.body_stream->gcount();
                        if (n == 0)
                            break;

                        if (bytes_read == 0)
                        {
                            String hex;
                            size_t dump_n = std::min(static_cast<size_t>(n), size_t(32));
                            for (size_t i = 0; i < dump_n; ++i)
                            {
                                if (i > 0) hex += ' ';
                                hex += fmt::format("{:02x}", static_cast<unsigned char>(buffer.m_data[i]));
                            }
                            LOG_DEBUG(prefetch_log, "First read: key={} n={} first_bytes=[{}]", key, n, hex);
                        }

                        bytes_read += n;
                    }

                    if (bytes_read < range_size)
                        LOG_WARNING(prefetch_log, "Short read: key={} got={} expected={} attempt={}",
                            key, bytes_read, range_size, attempt);
                }

                LOG_DEBUG(prefetch_log, "Read complete: key={} total_bytes={} expected={}", key, bytes_read, range_size);

                if (bytes_read < range_size)
                    throw Exception(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR,
                        "RRM prefetch: short read for {}, got {} of {} bytes after {} attempts",
                        key, bytes_read, range_size, max_attempts);

                buffer.m_size = bytes_read;
                promise->set_value(std::move(buffer));
            }
            catch (...)
            {
                promise->set_exception(std::current_exception());
            }
        }).detach();

    return std::make_unique<ReadBufferFromRRM>(
        object.remote_path, range_begin, range_size, std::move(future), scope.thread_group,
        scope.shared_from_this());
}

}
