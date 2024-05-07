#pragma once

#include <atomic>
#include <functional>


namespace DB
{

class ReadBuffer;
class WriteBuffer;
class Throttler;

using ThrottlerPtr = std::shared_ptr<Throttler>;


/// Copies data from ReadBuffer to WriteBuffer, all that is.
void copyData(ReadBuffer & from, WriteBuffer & to);

/// Copies `bytes` bytes from ReadBuffer to WriteBuffer. If there are no `bytes` bytes, then throws an exception.
void copyData(ReadBuffer & from, WriteBuffer & to, size_t bytes);

/// The same, with the condition to cancel.
void copyData(ReadBuffer & from, WriteBuffer & to, const std::atomic<int> & is_cancelled);
void copyData(ReadBuffer & from, WriteBuffer & to, size_t bytes, const std::atomic<int> & is_cancelled);

void copyData(ReadBuffer & from, WriteBuffer & to, std::function<void()> cancellation_hook);
void copyData(ReadBuffer & from, WriteBuffer & to, size_t bytes, std::function<void()> cancellation_hook);

/// Copies at most `max_bytes` bytes from ReadBuffer to WriteBuffer. If there are more bytes, then throws an exception.
void copyDataMaxBytes(ReadBuffer & from, WriteBuffer & to, size_t max_bytes);

/// Same as above but also use throttler to limit maximum speed
void copyDataWithThrottler(ReadBuffer & from, WriteBuffer & to, const std::atomic<int> & is_cancelled, ThrottlerPtr throttler);
void copyDataWithThrottler(ReadBuffer & from, WriteBuffer & to, size_t bytes, const std::atomic<int> & is_cancelled, ThrottlerPtr throttler);
void copyDataWithThrottler(ReadBuffer & from, WriteBuffer & to, std::function<void()> cancellation_hook, ThrottlerPtr throttler);

}
