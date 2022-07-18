#pragma once

#include <cassert>
#include <cstring>
#include <algorithm>
#include <memory>

#include <Common/Exception.h>
#include <IO/BufferBase.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int CANNOT_READ_ALL_DATA;
}

/** A simple abstract class for buffered data reading (char sequences) from somewhere.
  * Unlike std::istream, it provides access to the internal buffer,
  *  and also allows you to manually manage the position inside the buffer.
  *
  * Note! `char *`, not `const char *` is used
  *  (so that you can take out the common code into BufferBase, and also so that you can fill the buffer in with new data).
  * This causes inconveniences - for example, when using ReadBuffer to read from a chunk of memory const char *,
  *  you have to use const_cast.
  *
  * Derived classes must implement the nextImpl() method.
  */
class ReadBuffer : public BufferBase
{
public:
    /** Creates a buffer and sets a piece of available data to read to zero size,
      *  so that the next() function is called to load the new data portion into the buffer at the first try.
      */
    ReadBuffer(Position ptr, size_t size) : BufferBase(ptr, size, 0) { working_buffer.resize(0); }

    /** Used when the buffer is already full of data that can be read.
      *  (in this case, pass 0 as an offset)
      */
    ReadBuffer(Position ptr, size_t size, size_t offset) : BufferBase(ptr, size, offset) {}

    // Copying the read buffers can be dangerous because they can hold a lot of
    // memory or open files, so better to disable the copy constructor to prevent
    // accidental copying.
    ReadBuffer(const ReadBuffer &) = delete;

    // FIXME: behavior differs greately from `BufferBase::set()` and it's very confusing.
    void set(Position ptr, size_t size) { BufferBase::set(ptr, size, 0); working_buffer.resize(0); }

    /// Set buffer to given piece of memory but with certain bytes ignored from beginning.
    ///
    /// internal_buffer: |__________________|
    /// working_buffer:  |xxxxx|____________|
    ///                  ^     ^
    ///               bytes_to_ignore
    ///
    /// It's used for lazy seek. We also have another lazy seek mechanism that uses
    /// `nextimpl_working_buffer_offset` to set offset in `next` method. It's important that we
    /// don't do double lazy seek, which means `nextimpl_working_buffer_offset` should be zero. It's
    /// useful to keep internal_buffer points to the real span of the underlying memory, because its
    /// size might be used to allocate other buffers. It's also important to have pos starts at
    /// working_buffer.begin(), because some buffers assume this condition to be true and uses
    /// offset() to check read bytes.
    void setWithBytesToIgnore(Position ptr, size_t size, size_t bytes_to_ignore)
    {
        assert(bytes_to_ignore < size);
        assert(nextimpl_working_buffer_offset == 0);
        internal_buffer = Buffer(ptr, ptr + size);
        working_buffer = Buffer(ptr + bytes_to_ignore, ptr + size);
        pos = ptr + bytes_to_ignore;
    }

    /** read next data and fill a buffer with it; set position to the beginning;
      * return `false` in case of end, `true` otherwise; throw an exception, if something is wrong
      */
    bool next()
    {
        assert(!hasPendingData());
        assert(position() <= working_buffer.end());

        bytes += offset();
        bool res = nextImpl();
        if (!res)
            working_buffer = Buffer(pos, pos);
        else
        {
            pos = working_buffer.begin() + nextimpl_working_buffer_offset;
            assert(position() != working_buffer.end());
        }
        nextimpl_working_buffer_offset = 0;

        assert(position() <= working_buffer.end());

        return res;
    }


    inline void nextIfAtEnd()
    {
        if (!hasPendingData())
            next();
    }

    virtual ~ReadBuffer() = default;


    /** Unlike std::istream, it returns true if all data was read
      *  (and not in case there was an attempt to read after the end).
      * If at the moment the position is at the end of the buffer, it calls the next() method.
      * That is, it has a side effect - if the buffer is over, then it updates it and set the position to the beginning.
      *
      * Try to read after the end should throw an exception.
      */
    bool ALWAYS_INLINE eof()
    {
        return !hasPendingData() && !next();
    }

    void ignore()
    {
        if (!eof())
            ++pos;
        else
            throwReadAfterEOF();
    }

    void ignore(size_t n)
    {
        while (n != 0 && !eof())
        {
            size_t bytes_to_ignore = std::min(static_cast<size_t>(working_buffer.end() - pos), n);
            pos += bytes_to_ignore;
            n -= bytes_to_ignore;
        }

        if (n)
            throwReadAfterEOF();
    }

    /// You could call this method `ignore`, and `ignore` call `ignoreStrict`.
    size_t tryIgnore(size_t n)
    {
        size_t bytes_ignored = 0;

        while (bytes_ignored < n && !eof())
        {
            size_t bytes_to_ignore = std::min(static_cast<size_t>(working_buffer.end() - pos), n - bytes_ignored);
            pos += bytes_to_ignore;
            bytes_ignored += bytes_to_ignore;
        }

        return bytes_ignored;
    }

    void ignoreAll()
    {
        tryIgnore(std::numeric_limits<size_t>::max());
    }

    /// Peeks a single byte.
    bool ALWAYS_INLINE peek(char & c)
    {
        if (eof())
            return false;
        c = *pos;
        return true;
    }

    /// Reads a single byte.
    bool ALWAYS_INLINE read(char & c)
    {
        if (peek(c))
        {
            ++pos;
            return true;
        }

        return false;
    }

    void ALWAYS_INLINE readStrict(char & c)
    {
        if (read(c))
            return;
        throwReadAfterEOF();
    }

    /** Reads as many as there are, no more than n bytes. */
    size_t read(char * to, size_t n)
    {
        size_t bytes_copied = 0;

        while (bytes_copied < n && !eof())
        {
            size_t bytes_to_copy = std::min(static_cast<size_t>(working_buffer.end() - pos), n - bytes_copied);
            ::memcpy(to + bytes_copied, pos, bytes_to_copy);
            pos += bytes_to_copy;
            bytes_copied += bytes_to_copy;
        }

        return bytes_copied;
    }

    /** Reads n bytes, if there are less - throws an exception. */
    void readStrict(char * to, size_t n)
    {
        auto read_bytes = read(to, n);
        if (n != read_bytes)
            throw Exception("Cannot read all data. Bytes read: " + std::to_string(read_bytes) + ". Bytes expected: " + std::to_string(n) + ".", ErrorCodes::CANNOT_READ_ALL_DATA);
    }

    /** A method that can be more efficiently implemented in derived classes, in the case of reading large enough blocks.
      * The implementation can read data directly into `to`, without superfluous copying, if in `to` there is enough space for work.
      * For example, a CompressedReadBuffer can decompress the data directly into `to`, if the entire decompressed block fits there.
      * By default - the same as read.
      * Don't use for small reads.
      */
    virtual size_t readBig(char * to, size_t n)
    {
        return read(to, n);
    }

    /** Do something to allow faster subsequent call to 'nextImpl' if possible.
      * It's used for asynchronous readers with double-buffering.
      */
    virtual void prefetch() {}

    /**
     * Set upper bound for read range [..., position).
     * Required for reading from remote filesystem, when it matters how much we read.
     */
    virtual void setReadUntilPosition(size_t /* position */) {}

    virtual void setReadUntilEnd() {}

protected:
    /// The number of bytes to ignore from the initial position of `working_buffer`
    /// buffer. Apparently this is an additional out-parameter for nextImpl(),
    /// not a real field.
    size_t nextimpl_working_buffer_offset = 0;

private:
    /** Read the next data and fill a buffer with it.
      * Return `false` in case of the end, `true` otherwise.
      * Throw an exception if something is wrong.
      */
    virtual bool nextImpl() { return false; }

    [[noreturn]] static void throwReadAfterEOF()
    {
        throw Exception("Attempt to read after eof", ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF);
    }
};


using ReadBufferPtr = std::shared_ptr<ReadBuffer>;

/// Due to inconsistencies in ReadBuffer-family interfaces:
///  - some require to fully wrap underlying buffer and own it,
///  - some just wrap the reference without ownership,
/// we need to be able to wrap reference-only buffers with movable transparent proxy-buffer.
/// The uniqueness of such wraps is responsibility of the code author.
inline std::unique_ptr<ReadBuffer> wrapReadBufferReference(ReadBuffer & buf)
{
    class ReadBufferWrapper : public ReadBuffer
    {
        public:
            explicit ReadBufferWrapper(ReadBuffer & buf_) : ReadBuffer(buf_.position(), 0), buf(buf_)
            {
                working_buffer = Buffer(buf.position(), buf.buffer().end());
            }

        private:
            ReadBuffer & buf;

            bool nextImpl() override
            {
                buf.position() = position();

                if (!buf.next())
                    return false;

                working_buffer = buf.buffer();

                return true;
            }
    };

    return std::make_unique<ReadBufferWrapper>(buf);
}

inline std::unique_ptr<ReadBuffer> wrapReadBufferPointer(ReadBufferPtr ptr)
{
    class ReadBufferWrapper : public ReadBuffer
    {
        public:
            explicit ReadBufferWrapper(ReadBufferPtr ptr_) : ReadBuffer(ptr_->position(), 0), ptr(ptr_)
            {
                working_buffer = Buffer(ptr->position(), ptr->buffer().end());
            }

        private:
            ReadBufferPtr ptr;

            bool nextImpl() override
            {
                ptr->position() = position();

                if (!ptr->next())
                    return false;

                working_buffer = ptr->buffer();

                return true;
            }
    };

    return std::make_unique<ReadBufferWrapper>(ptr);
}

}
