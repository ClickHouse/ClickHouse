#pragma once

#include <algorithm>
#include <memory>
#include <iostream>
#include <cassert>
#include <cstring>
#include <mutex>

#include <Common/Exception.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <IO/WriteBuffer.h>


namespace DB
{

/** A simple abstract class for buffered data writing (char sequences) somewhere.
  * Unlike std::ostream, it provides access to the internal buffer,
  *  and also allows you to manually manage the position inside the buffer.
  *
  * Derived classes must implement the nextImpl() method.
  */
class WriteBufferENet : public WriteBuffer
{
public:
    WriteBufferENet(Position ptr, size_t size) : WriteBuffer(ptr, size) {}

    char* res()
    {
      return st;
    }

private:
    std::mutex mutex;
    char st[DBMS_DEFAULT_BUFFER_SIZE];
    /** Write the data in the buffer (from the beginning of the buffer to the current position).
      * Throw an exception if something is wrong.
      */
    void nextImpl() override
    {
        std::lock_guard lock(mutex);
        std::strcat(st, working_buffer.begin());
        std::memset(working_buffer.begin(), '\0', working_buffer.size());
        return;
    }

using WriteBufferENetPtr = std::shared_ptr<WriteBufferENet>;

};

}
