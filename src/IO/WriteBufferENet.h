#pragma once

#include <algorithm>
#include <memory>
#include <iostream>
#include <cassert>
#include <cstring>

#include <Common/Exception.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <IO/WriteBuffer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_WRITE_AFTER_END_OF_BUFFER;
    extern const int LOGICAL_ERROR;
}


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

private:
    /** Write the data in the buffer (from the beginning of the buffer to the current position).
      * Throw an exception if something is wrong.
      */
    void nextImpl() override
    {
        return;
    }
};


using WriteBufferENetPtr = std::shared_ptr<WriteBufferENet>;


}
