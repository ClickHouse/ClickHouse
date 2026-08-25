#pragma once

#include <algorithm>
#include <exception>
#include <memory>

#include <IO/BufferBase.h>


namespace DB
{

/** A simple abstract class for buffered data writing (char sequences) somewhere.
  * Unlike std::ostream, it provides access to the internal buffer,
  *  and also allows you to manually manage the position inside the buffer.
  *
  * Derived classes must implement the nextImpl() method.
  */
class WriteBuffer : public BufferBase
{
public:
    /// Special exception to throw when the current MemoryWriteBuffer cannot receive data
    class CurrentBufferExhausted : public std::exception
    {
    public:
        const char * what() const noexcept override { return "WriteBuffer limit is exhausted"; }
    };

    using BufferBase::set;
    using BufferBase::position;
    void set(Position ptr, size_t size) { BufferBase::set(ptr, size, 0); }

    /** write the data in the buffer (from the beginning of the buffer to the current position);
      * set the position to the beginning; throw an exception, if something is wrong.
      *
      * Next call doesn't guarantee that buffer capacity is regained after.
      * Some buffers (i.g WriteBufferFromS3) flush its data only after certain amount of consumed data.
      * If direct write is performed into [position(), buffer().end()) and its length is not enough,
      * you need to fill it first (i.g with write call), after it the capacity is regained.
      */
    void next()
    {
        if (canceled)
            return;

        if (!offset())
            return;

        auto bytes_in_buffer = offset();

        try
        {
            nextImpl();
        }
        catch (CurrentBufferExhausted &)
        {
            pos = working_buffer.begin();
            bytes += bytes_in_buffer;
            throw;
        }
        catch (...)
        {
            /** If the nextImpl() call was unsuccessful, move the cursor to the beginning,
              * so that later (for example, when the stack was expanded) there was no second attempt to write data.
              */
            pos = working_buffer.begin();
            bytes += bytes_in_buffer;

            cancel();

            throw;
        }

        bytes += bytes_in_buffer;
        pos = working_buffer.begin() + nextimpl_working_buffer_offset;
        nextimpl_working_buffer_offset = 0;
    }

    /// Calling finalize() in the destructor of derived classes is a bad practice.
    virtual ~WriteBuffer();

    void nextIfAtEnd()
    {
        if (!hasPendingData())
            next();
    }

    void write(const char * from, size_t n);

    void write(char x);

    /// This method may be called before finalize() to tell there would not be any more data written.
    /// Used does not have to call it, implementation should check it itself if needed.
    ///
    /// The idea is similar to prefetch. In case if all data is written, we can flush the buffer
    /// and start sending data asynchronously. It may improve writing performance in case you have
    /// multiple files to finalize. Mainly, for blob storage, finalization has high latency,
    /// and calling preFinalize in a loop may parallelize it.
    virtual void preFinalize() { next(); }

    /// Write the last data.
    void finalize();
    void cancel() noexcept;

    bool isFinalized() const { return finalized; }
    bool isCanceled() const { return canceled; }

    /// Wait for data to be reliably written. Mainly, call fsync for fd.
    /// May be called after finalize() if needed.
    virtual void sync()
    {
        next();
    }

    size_t rejectBufferedDataSave()
    {
        size_t before = count();
        bool data_has_been_sent = count() != offset();
        if (!data_has_been_sent)
            position() -= offset();
        return before - count();
    }

protected:
    WriteBuffer(Position ptr, size_t size) : BufferBase(ptr, size, 0) {}

    virtual void finalizeImpl() { next(); }
    virtual void cancelImpl() noexcept { }

    bool finalized = false;

    /// The number of bytes to preserve from the initial position of `working_buffer`
    /// buffer. Apparently this is an additional out-parameter for nextImpl(),
    /// not a real field.
    size_t nextimpl_working_buffer_offset = 0;

private:
    /** Write the data in the buffer (from the beginning of the buffer to the current position).
      * Throw an exception if something is wrong.
      */
    virtual void nextImpl();

    bool isStackUnwinding() const
    {
        return exception_level < std::uncaught_exceptions();
    }

    int exception_level = std::uncaught_exceptions();
};


using WriteBufferPtr = std::shared_ptr<WriteBuffer>;


class WriteBufferFromPointer : public WriteBuffer
{
public:
    WriteBufferFromPointer(Position ptr, size_t size) : WriteBuffer(ptr, size) {}

private:
    void finalizeImpl() override
    {
        /// no op
    }

    void sync() override
    {
        /// no on
    }
};

// AutoCanceledWriteBuffer cancel the buffer in d-tor when it has not been finalized before d-tor
// AutoCanceledWriteBuffer could not be inherited.
// Otherwise cancel method could not call proper cancelImpl ьуерщв because inheritor is destroyed already.
// But the ussage of final inheritance is avoided in favor to keep the possibility to use std::make_shared.
template<class Base>
class AutoCanceledWriteBuffer final : public Base
{
    static_assert(std::derived_from<Base, WriteBuffer>);

public:
    using Base::Base;

    ~AutoCanceledWriteBuffer() override
    {
        if (!this->finalized && !this->canceled)
            this->cancel();
    }
};
}
