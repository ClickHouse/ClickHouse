#pragma once

#include <memory>
#include <iostream>
#include <cstring>
#include <mutex>

#include <Common/Exception.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromOStream.h>


namespace DB
{

/** A simple abstract class for buffered data writing (char sequences) somewhere.
  * Unlike std::ostream, it provides access to the internal buffer,
  *  and also allows you to manually manage the position inside the buffer.
  *
  * Derived classes must implement the nextImpl() method.
  */
class WriteBufferENet : public BufferWithOwnMemory<WriteBuffer>
{
public:
    WriteBufferENet(std::ostream & _out) : BufferWithOwnMemory<WriteBuffer>(DBMS_DEFAULT_BUFFER_SIZE), out_str(&_out)
    {
    }

private:
    std::ostream* out_str;
    std::mutex mutex;
    std::unique_ptr<WriteBufferFromOStream> out;
    /** Write the data in the buffer (from the beginning of the buffer to the current position).
      * Throw an exception if something is wrong.
      */
    void nextImpl() override
    {
        std::lock_guard lock(mutex);

        if (!out) {
          out = std::make_unique<WriteBufferFromOStream>(*out_str, working_buffer.size(), working_buffer.begin());
        }

        out->buffer() = buffer();
        out->position() = position();
        out->next();
    }

using WriteBufferENetPtr = std::shared_ptr<WriteBufferENet>;

};

}
