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
    ThreadGroupPtr thread_group_)
    : ReadBufferFromFileBase(0 /* buf_size — will be set after prefetch */, nullptr, 0)
    , object_key(std::move(object_key_))
    , range_begin(range_begin_)
    , range_size(range_size_)
    , prefetch_future(std::move(prefetch_future_))
    , thread_group(std::move(thread_group_))
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
        return false;

    /// Point the working buffer at the remaining prefetched data.
    char * data_begin = prefetched_data.m_data + data_offset;
    size_t remaining = prefetched_data.m_size - data_offset;

    LOG_DEBUG(log, "nextImpl: object={} serving local_offset={} abs_offset={} remaining={} first_bytes=[{}]",
        object_key, data_offset, range_begin + data_offset, remaining,
        hexDump(data_begin, remaining));

    /// Serve all remaining data in one go.
    internal_buffer = Buffer(data_begin, data_begin + remaining);
    working_buffer = internal_buffer;

    data_offset = prefetched_data.m_size;
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

}
