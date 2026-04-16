#include <IO/ReadBufferFromRRM.h>
#include <Common/Exception.h>
#include <fmt/format.h>


namespace DB
{

namespace
{
/// Hex-dump first N bytes for debug logging.
String hexDump(const char * data, size_t size, size_t max_bytes = 32)
{
    size_t n = std::min(size, max_bytes);
    String result;
    result.reserve(n * 3);
    for (size_t i = 0; i < n; ++i)
    {
        if (i > 0)
            result += ' ';
        result += fmt::format("{:02x}", static_cast<unsigned char>(data[i]));
    }
    if (size > max_bytes)
        result += "...";
    return result;
}
}

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}

ReadBufferFromRRM::ReadBufferFromRRM(
    String object_key_,
    size_t range_begin_,
    size_t range_size_,
    std::future<Memory<>> prefetch_future_,
    ThreadGroupPtr thread_group_,
    ReadScopePtr scope_)
    : ReadBufferFromFileBase(0 /* buf_size — will be set after prefetch */, nullptr, 0)
    , object_key(std::move(object_key_))
    , range_begin(range_begin_)
    , range_size(range_size_)
    , prefetch_future(std::move(prefetch_future_))
    , thread_group(std::move(thread_group_))
    , scope(std::move(scope_))
{
}

ReadBufferFromRRM::~ReadBufferFromRRM() = default;

void ReadBufferFromRRM::waitPrefetch()
{
    if (prefetch_consumed)
        return;

    LOG_DEBUG(log, "Waiting for prefetch: object={} range=[{}, {})",
        object_key, range_begin, range_begin + range_size);

    prefetched_data = prefetch_future.get();
    prefetch_consumed = true;

    LOG_DEBUG(log, "Prefetch complete: object={} got {} bytes, first_bytes=[{}]",
        object_key, prefetched_data.m_size,
        hexDump(prefetched_data.m_data, prefetched_data.m_size));
}

bool ReadBufferFromRRM::nextImpl()
{
    waitPrefetch();

    if (data_offset >= prefetched_data.m_size)
    {
        LOG_DEBUG(log, "nextImpl: EOF object={} data_offset={} prefetched_size={} range=[{}, {})",
            object_key, data_offset, prefetched_data.m_size, range_begin, range_begin + range_size);
        return false;
    }

    char * data_begin = prefetched_data.m_data + data_offset;
    size_t remaining = prefetched_data.m_size - data_offset;

    if (internal_buffer.empty())
    {
        /// Standalone mode: no external buffer provided, point directly at prefetched data.
        internal_buffer = Buffer(data_begin, data_begin + remaining);
        working_buffer = internal_buffer;
        data_offset = prefetched_data.m_size;
    }
    else
    {
        /// External buffer mode (CachedOnDiskReadBufferFromFile or SwapHelper):
        /// copy into the caller's buffer to preserve its pointer invariant.
        size_t to_copy = std::min(remaining, internal_buffer.size());
        memcpy(internal_buffer.begin(), data_begin, to_copy);
        working_buffer = Buffer(internal_buffer.begin(), internal_buffer.begin() + to_copy);
        data_offset += to_copy;
    }

    LOG_DEBUG(log, "nextImpl: object={} serving local_offset={} abs_offset={} remaining={} first_bytes=[{}]",
        object_key, data_offset, range_begin + data_offset, remaining,
        hexDump(working_buffer.begin(), working_buffer.size()));

    return true;
}

off_t ReadBufferFromRRM::seek(off_t off, int whence)
{
    waitPrefetch();

    LOG_DEBUG(log, "seek: object={} off={} whence={} current_pos={}",
        object_key, off, whence == SEEK_SET ? "SEEK_SET" : "SEEK_CUR", getPosition());

    /// Callers use absolute offsets within the S3 object.
    /// Translate to local offset within prefetched_data.
    off_t absolute_pos = 0;
    if (whence == SEEK_SET)
        absolute_pos = off;
    else if (whence == SEEK_CUR)
        absolute_pos = getPosition() + off;
    else
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE,
            "Only SEEK_SET and SEEK_CUR seek modes allowed");

    if (absolute_pos < static_cast<off_t>(range_begin)
        || static_cast<size_t>(absolute_pos) > range_begin + prefetched_data.m_size)
    {
        throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND,
            "Seek position {} is out of prefetched range [{}, {})",
            absolute_pos, range_begin, range_begin + prefetched_data.m_size);
    }

    validateSeekPosition(static_cast<size_t>(absolute_pos));

    size_t local_target = static_cast<size_t>(absolute_pos) - range_begin;

    /// If target is within the current working_buffer, just reposition.
    if (prefetched_data.m_data && local_target >= (data_offset - working_buffer.size()) && local_target <= data_offset)
    {
        size_t buf_start = data_offset - working_buffer.size();
        pos = working_buffer.begin() + (local_target - buf_start);
        return absolute_pos;
    }

    /// Otherwise, reset — next `nextImpl` will serve from the new offset.
    data_offset = local_target;
    resetWorkingBuffer();

    return absolute_pos;
}

off_t ReadBufferFromRRM::getPosition()
{
    if (!prefetch_consumed)
        return static_cast<off_t>(range_begin);

    return static_cast<off_t>(range_begin + data_offset - available());
}

std::optional<size_t> ReadBufferFromRRM::tryGetFileSize()
{
    LOG_DEBUG(log, "tryGetFileSize: object={} returning range_size={}", object_key, range_size);
    return range_size;
}

size_t ReadBufferFromRRM::getFileOffsetOfBufferEnd() const
{
    size_t result = range_begin + data_offset;
    LOG_DEBUG(log, "getFileOffsetOfBufferEnd: object={} range_begin={} data_offset={} result={}",
        object_key, range_begin, data_offset, result);
    return result;
}

void ReadBufferFromRRM::validateSeekPosition(size_t absolute_pos) const
{
    if (!scope || scope->reading_ranges.empty())
        return;

    size_t enc_header = scope->encryption_header_bytes;

    /// The encryption header region [0, enc_header) is always valid.
    if (enc_header > 0 && absolute_pos < enc_header)
        return;

    /// reading_ranges are in plaintext coordinates; seeks from the
    /// encrypted reader arrive shifted by enc_header.
    for (size_t i = 0; i < scope->reading_ranges.size(); ++i)
    {
        const auto & rr = scope->reading_ranges[i];
        size_t padding = (i < scope->cache_pre_padding_bytes.size()) ? scope->cache_pre_padding_bytes[i] : 0;
        size_t padded_begin = rr.begin - padding + enc_header;
        size_t range_end = rr.end + enc_header;

        if (absolute_pos >= padded_begin && absolute_pos < range_end)
            return;
    }

    throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND,
        "Seek to {} does not match any reading_range or cache_pre_padding in scope [{}]"
        " enc_header={} last_range=[{}, {})",
        absolute_pos, scope->toString(),
        enc_header,
        scope->reading_ranges.empty() ? 0 : scope->reading_ranges.back().begin,
        scope->reading_ranges.empty() ? 0 : scope->reading_ranges.back().end);
}

}
