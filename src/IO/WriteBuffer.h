#pragma once

#include <algorithm>
#include <atomic>
#include <cstring>
#include <exception>
#include <memory>

#include <IO/BufferBase.h>
#include <base/defines.h>


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
            ++flush_count;
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

    /// The common case, where the data fits into the working buffer of a buffer that is neither finalized nor
    /// canceled, is inlined: delimiters, numbers and short strings are written a few bytes at a time, and a call
    /// would cost more than the copy. Everything else, including the exceptions, is in `writeSlow`.
    void write(const char * from, size_t n)
    {
        /// `closed` is checked first: after `finalize` some buffers reset their storage to null (`set(nullptr, 0)`),
        /// and the pointer arithmetic in `available` must not be evaluated then.
        /// The comparison is unsigned as in `available`: for a constant `n` it is a single `cmp`.
        if (!closed && n <= available()) [[likely]]
        {
            /// Zero bytes, common for empty strings, touch neither `memcpy` nor `pos`.
            if (n != 0)
                pos = copyBytes(pos, from, n);
            return;
        }
        pos = writeSlow(from, n);
    }

    void write(char x)
    {
        if (!closed && hasPendingData()) [[likely]]
        {
            /// The byte store may alias `pos`, so `++pos` would reload it.
            Position to = pos;
            *to = x;
            pos = to + 1;
            return;
        }
        pos = writeSlow(x);
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
    void finalize();
    void cancel() noexcept;

    bool isFinalized() const { return finalized; }
    bool isCanceled() const { return canceled; }

    /// Get number of times next() has been called (number of flushes)
    size_t getFlushCount() const { return flush_count.load(std::memory_order_relaxed); }

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
    /// `finalized || canceled`, in one flag for the inlined `write`. Set and reset together with them.
    bool closed = false;

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

    /// Out of line, like `ReadBuffer::throwReadAfterEOF`: an inlined `throw` would put a
    /// `-fstack-protector-strong` canary on `write`, which runs per byte. `NO_INLINE` because they sit
    /// in the same translation unit as `writeSlow`.
    [[noreturn]] NO_INLINE static void throwWriteToFinalizedBuffer();
    [[noreturn]] NO_INLINE static void throwWriteToCanceledBuffer(int code);

    /// Flushes the buffer as many times as needed, and throws for a finalized or canceled buffer.
    /// Returns `pos` for the inlined `write` to assign, so the position stays in a register on every path.
    [[nodiscard]] Position writeSlow(const char * from, size_t n);
    [[nodiscard]] Position writeSlow(char x);

    /// A size known at compile time becomes plain moves. A run-time size is copied with `memcpy`, which keeps
    /// the inlined code small: a larger fast path changes how its callers are optimized (inlining, loop unswitching).
    /// Returns `to + n`: the copy may alias `pos`, so the new position is computed from `to`, not reloaded.
    ALWAYS_INLINE static Position copyBytes(char * __restrict to, const char * __restrict from, size_t n)
    {
        if (__builtin_constant_p(n))
            __builtin_memcpy(to, from, n);
        else
            memcpy(to, from, n);
        return to + n;
    }

    int exception_level = std::uncaught_exceptions();

    /// Number of flushes for debugging/assertions
    std::atomic<size_t> flush_count = 0;
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
// But the usage of final inheritance is avoided in favor to keep the possibility to use std::make_shared.
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
