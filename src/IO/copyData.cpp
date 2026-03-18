#include <Common/Exception.h>
#include <Common/Throttler.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/copyData.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int CANNOT_READ_ALL_DATA;
}

namespace
{

void copyDataImpl(ReadBuffer & from, WriteBuffer & to, bool check_bytes, size_t bytes, const std::atomic<int> * is_cancelled, ThrottlerPtr throttler)
{
    /// If read to the end of the buffer, eof() either fills the buffer with new data and moves the cursor to the beginning, or returns false.
    while (bytes > 0 && !from.eof())
    {
        if (is_cancelled && *is_cancelled)
            return;

        /// buffer() - a piece of data available for reading; position() - the cursor of the place to which you have already read.
        size_t count = std::min(bytes, static_cast<size_t>(from.buffer().end() - from.position()));
        to.write(from.position(), count);
        from.position() += count;
        bytes -= count;

        if (throttler)
            throttler->add(count);
    }

    if (check_bytes && bytes > 0)
        throw Exception(ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF, "Attempt to read after EOF, left to copy {} bytes.", bytes);
}

void copyDataImpl(ReadBuffer & from, WriteBuffer & to, bool check_bytes, size_t bytes, std::function<void()> cancellation_hook, ThrottlerPtr throttler)
{
    /// If read to the end of the buffer, eof() either fills the buffer with new data and moves the cursor to the beginning, or returns false.
    while (bytes > 0 && !from.eof())
    {
        if (cancellation_hook)
            cancellation_hook();

        /// buffer() - a piece of data available for reading; position() - the cursor of the place to which you have already read.
        size_t count = std::min(bytes, static_cast<size_t>(from.buffer().end() - from.position()));
        to.write(from.position(), count);
        from.position() += count;
        bytes -= count;

        if (throttler)
            throttler->add(count);
    }

    if (check_bytes && bytes > 0)
        throw Exception(ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF, "Attempt to read after EOF.");
}

}

void copyData(ReadBuffer & from, WriteBuffer & to)
{
    copyDataImpl(from, to, false, std::numeric_limits<size_t>::max(), nullptr, nullptr);
}

void copyData(ReadBuffer & from, WriteBuffer & to, const std::atomic<int> & is_cancelled)
{
    copyDataImpl(from, to, false, std::numeric_limits<size_t>::max(), &is_cancelled, nullptr);
}

void copyData(ReadBuffer & from, WriteBuffer & to, std::function<void()> cancellation_hook)
{
    copyDataImpl(from, to, false, std::numeric_limits<size_t>::max(), cancellation_hook, nullptr);
}

void copyData(ReadBuffer & from, WriteBuffer & to, size_t bytes)
{
    copyDataImpl(from, to, true, bytes, nullptr, nullptr);
}

void copyData(ReadBuffer & from, WriteBuffer & to, size_t bytes, const std::atomic<int> & is_cancelled)
{
    copyDataImpl(from, to, true, bytes, &is_cancelled, nullptr);
}

void copyData(ReadBuffer & from, WriteBuffer & to, size_t bytes, std::function<void()> cancellation_hook)
{
    copyDataImpl(from, to, true, bytes, cancellation_hook, nullptr);
}

void copyDataMaxBytes(ReadBuffer & from, WriteBuffer & to, size_t max_bytes)
{
    copyDataImpl(from, to, false, max_bytes, nullptr, nullptr);
    if (!from.eof())
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read all data, max readable size reached.");
}

void copyDataWithThrottler(ReadBuffer & from, WriteBuffer & to, const std::atomic<int> & is_cancelled, ThrottlerPtr throttler)
{
    copyDataImpl(from, to, false, std::numeric_limits<size_t>::max(), &is_cancelled, throttler);
}

void copyDataWithThrottler(ReadBuffer & from, WriteBuffer & to, size_t bytes, const std::atomic<int> & is_cancelled, ThrottlerPtr throttler)
{
    copyDataImpl(from, to, true, bytes, &is_cancelled, throttler);
}

void copyDataWithThrottler(ReadBuffer & from, WriteBuffer & to, std::function<void()> cancellation_hook, ThrottlerPtr throttler)
{
    copyDataImpl(from, to, false, std::numeric_limits<size_t>::max(), cancellation_hook, throttler);
}

}
