#pragma once

#include <atomic>
#include <functional>


namespace DB
{

class ReadBuffer;
class WriteBuffer;


/** Copies data from ReadBuffer to WriteBuffer, all that is.
  */
void copyData(ReadBuffer & from, WriteBuffer & to);

/** Copies `bytes` bytes from ReadBuffer to WriteBuffer. If there are no `bytes` bytes, then throws an exception.
  */
void copyData(ReadBuffer & from, WriteBuffer & to, size_t bytes);

/** The same, with the condition to cancel.
  */
void copyData(ReadBuffer & from, WriteBuffer & to, const std::atomic<int> & is_cancelled);
void copyData(ReadBuffer & from, WriteBuffer & to, size_t bytes, const std::atomic<int> & is_cancelled);

void copyData(ReadBuffer & from, WriteBuffer & to, std::function<void()> cancellation_hook);
void copyData(ReadBuffer & from, WriteBuffer & to, size_t bytes, std::function<void()> cancellation_hook);

}
