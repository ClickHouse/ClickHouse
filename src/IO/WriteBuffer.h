#pragma once

#include <algorithm>
#include <memory>
#include <iostream>
#include <cassert>
#include <cstring>

#include <Common/Exception.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/logger_useful.h>
#include <IO/BufferBase.h>


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
class WriteBuffer : public BufferBase
{
public:
    using BufferBase::set;
    using BufferBase::position;
    void set(Position ptr, size_t size) { BufferBase::set(ptr, size, 0); }

    /** write the data in the buffer (from the beginning of the buffer to the current position);
      * set the position to the beginning; throw an exception, if something is wrong
      */
    inline void next()
    {
        if (!offset())
            return;

        auto bytes_in_buffer = offset();

        try
        {
            nextImpl();
        }
        catch (...)
        {
            /** If the nextImpl() call was unsuccessful, move the cursor to the beginning,
              * so that later (for example, when the stack was expanded) there was no second attempt to write data.
              */
            pos = working_buffer.begin();
            bytes += bytes_in_buffer;
            throw;
        }

        bytes += bytes_in_buffer;
        pos = working_buffer.begin();
    }

    /// Calling finalize() in the destructor of derived classes is a bad practice.
    /// This causes objects to be left on the remote FS when a write operation is rolled back.
    /// Do call finalize() explicitly, before this call you have no guarantee that the file has been written
    virtual ~WriteBuffer()
    {
        // That destructor could be call with finalized=false in case of exceptions
        if (count() > 0 && !finalized)
        {
            /// It is totally OK to destroy instance without finalization when an exception occurs
            /// However it is suspicious to destroy instance without finalization at the green path
            if (!std::uncaught_exceptions())
            {
                Poco::Logger * log = &Poco::Logger::get("WriteBuffer");
                LOG_ERROR(
                    log,
                    "WriteBuffer is not finalized in destructor. "
                    "No exceptions in flight are detected. "
                    "The file might not be written at all or might be truncated. "
                    "Stack trace: {}",
                    StackTrace().toString());
            }
        }
    }

    inline void nextIfAtEnd()
    {
        if (!hasPendingData())
            next();
    }


    void write(const char * from, size_t n)
    {
        if (finalized)
            throw Exception{ErrorCodes::LOGICAL_ERROR, "Cannot write to finalized buffer"};

        size_t bytes_copied = 0;

        /// Produces endless loop
        assert(!working_buffer.empty());

        while (bytes_copied < n)
        {
            nextIfAtEnd();
            size_t bytes_to_copy = std::min(static_cast<size_t>(working_buffer.end() - pos), n - bytes_copied);
            memcpy(pos, from + bytes_copied, bytes_to_copy);
            pos += bytes_to_copy;
            bytes_copied += bytes_to_copy;
        }
    }


    inline void write(char x)
    {
        if (finalized)
            throw Exception{ErrorCodes::LOGICAL_ERROR, "Cannot write to finalized buffer"};

        nextIfAtEnd();
        *pos = x;
        ++pos;
    }

    /// This method may be called before finalize() to tell there would not be any more data written.
    /// Used does not have to call it, implementation should check it itself if needed.
    ///
    /// The idea is similar to prefetch. In case if all data is written, we can flush the buffer
    /// and start sending data asynchronously. It may improve writing performance in case you have
    /// multiple files to finalize. Mainly, for blob storage, finalization has high latency,
    /// and calling preFinalize in a loop may parallelize it.
    virtual void preFinalize() { next(); }

    /// Write the last data.
    void finalize()
    {
        if (finalized)
            return;

        /// finalize() is often called from destructors.
        LockMemoryExceptionInThread lock(VariableContext::Global);
        try
        {
            finalizeImpl();
            finalized = true;
        }
        catch (...)
        {
            pos = working_buffer.begin();
            finalized = true;
            throw;
        }
    }

    /// Wait for data to be reliably written. Mainly, call fsync for fd.
    /// May be called after finalize() if needed.
    virtual void sync()
    {
        next();
    }

protected:
    WriteBuffer(Position ptr, size_t size) : BufferBase(ptr, size, 0) {}

    virtual void finalizeImpl()
    {
        next();
    }

    bool finalized = false;

private:
    /** Write the data in the buffer (from the beginning of the buffer to the current position).
      * Throw an exception if something is wrong.
      */
    virtual void nextImpl()
    {
        throw Exception(ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER, "Cannot write after end of buffer.");
    }
};


using WriteBufferPtr = std::shared_ptr<WriteBuffer>;


class WriteBufferFromPointer : public WriteBuffer
{
public:
    WriteBufferFromPointer(Position ptr, size_t size) : WriteBuffer(ptr, size) {}

private:
    virtual void finalizeImpl() override
    {
        /// no op
    }

    virtual void sync() override
    {
        /// no on
    }
};

}
