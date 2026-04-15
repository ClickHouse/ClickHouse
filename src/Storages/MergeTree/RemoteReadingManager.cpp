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
                     ThreadGroupPtr thread_group_)
    : part_id(std::move(part_id_))
    , mark_ranges(std::move(mark_ranges_))
    , phase(phase_)
    , reading_ranges(std::move(reading_ranges_))
    , cache_pre_padding_bytes(std::move(cache_pre_padding_bytes_))
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
    return ReadScopePtr(new ReadScope(part_id, mark_ranges, phase, std::move(adjusted), {}, thread_group));
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

    size_t first_padding = scope.cache_pre_padding_bytes.empty() ? 0 : scope.cache_pre_padding_bytes.front();
    size_t range_begin = scope.reading_ranges.front().begin - first_padding;
    size_t range_end = scope.reading_ranges.back().end;
    size_t range_size = range_end - range_begin;

    LOG_DEBUG(log, "createObjectReadBuffer: blob={} range=[{}, {}) size={} scope=[{}]",
        object.remote_path, range_begin, range_end, range_size, scope.toString());

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

                auto conn = factory(range_begin, range_end, /* attempt = */ 0);

                LOG_DEBUG(prefetch_log, "Connection opened: key={} fd={} has_body={} range=[{}, {})",
                    key, conn.fd, conn.body_stream != nullptr, range_begin, range_end);

                if (!conn.body_stream)
                    throw Exception(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR,
                        "RRM prefetch: no body stream for {}", key);

                /// Read from the HTTP body stream (not the raw FD).
                /// The body stream includes data from Poco's internal buffer
                /// and handles chunked transfer encoding transparently.
                Memory<> buffer(range_size);
                size_t bytes_read = 0;
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

                LOG_DEBUG(prefetch_log, "Read complete: key={} total_bytes={} expected={}", key, bytes_read, range_size);
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
