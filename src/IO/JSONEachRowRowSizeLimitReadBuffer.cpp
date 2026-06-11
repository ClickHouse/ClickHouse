#include <IO/JSONEachRowRowSizeLimitReadBuffer.h>

#include <Common/Exception.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

void throwJSONEachRowObjectTooLarge(size_t position, size_t max_bytes, size_t current_bytes)
{
    throw Exception(
        ErrorCodes::INCORRECT_DATA,
        "Size of JSON object at position {} is extremely large. Expected not greater than {} bytes, but current is "
        "{} bytes per row. Increase the value setting 'min_chunk_bytes_for_parallel_parsing' or check your data manually, "
        "most likely JSON is malformed",
        position, max_bytes, current_bytes);
}

namespace
{
    /// Expose at most this many bytes of the nested buffer per refill. Keeps the working
    /// window bounded so an oversized row is detected even when the whole input is in one
    /// buffer (e.g. mmapped files), while staying large enough not to add per-row refills
    /// for normal data.
    size_t windowSize(size_t max_bytes_per_row)
    {
        if (max_bytes_per_row == 0)
            return std::numeric_limits<size_t>::max();
        /// +1 so a row of exactly max_bytes_per_row is read in one window and only a row
        /// that strictly exceeds the limit forces the extra refill that throws.
        return max_bytes_per_row + 1;
    }
}

JSONEachRowRowSizeLimitReadBuffer::JSONEachRowRowSizeLimitReadBuffer(ReadBuffer & in_, size_t max_bytes_per_row_)
    : ReadBuffer(in_.position(), std::min(in_.available(), windowSize(max_bytes_per_row_)), 0)
    , in(in_)
    , max_bytes_per_row(max_bytes_per_row_)
{
}

JSONEachRowRowSizeLimitReadBuffer::~JSONEachRowRowSizeLimitReadBuffer()
{
    /// Hand the position where this row finished back to the nested buffer so the
    /// caller can continue reading the next row from it.
    if (!working_buffer.empty())
        in.position() = position();
}

bool JSONEachRowRowSizeLimitReadBuffer::nextImpl()
{
    /// Bytes of this row consumed from the shared window since the last refill.
    in.position() = position();
    counted_bytes += offset();

    if (max_bytes_per_row != 0 && counted_bytes > max_bytes_per_row)
        throwJSONEachRowObjectTooLarge(in.count(), max_bytes_per_row, counted_bytes);

    /// The nested buffer may still have data in its current working buffer (when we only
    /// exposed a bounded window of it); pull more only if it is actually exhausted.
    if (!in.hasPendingData() && !in.next())
        return false;

    size_t remaining_window = windowSize(max_bytes_per_row) - std::min(counted_bytes, windowSize(max_bytes_per_row));
    BufferBase::set(in.position(), std::min(in.available(), remaining_window), 0);
    return true;
}

}
