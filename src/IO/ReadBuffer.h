#pragma once

#include <cassert>
#include <cstring>
#include <memory>

#include <Common/Exception.h>
#include <Common/Priority.h>
#include <IO/BufferBase.h>
#include <IO/AsynchronousReader.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int CANNOT_READ_ALL_DATA;
}

static constexpr auto DEFAULT_PREFETCH_PRIORITY = Priority{0};

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

    /** read next data and fill a buffer with it; set position to the beginning of the new data
      * (but not necessarily to the beginning of working_buffer!);
      * return `false` in case of end, `true` otherwise; throw an exception, if something is wrong;
      *
      * if an exception was thrown, is the ReadBuffer left in a usable state? this varies across implementations;
      * can the caller retry next() after an exception, or call other methods? not recommended
      */
    bool next()
    {
        chassert(!hasPendingData());
        chassert(position() <= working_buffer.end());

        bytes += offset();
        bool res = nextImpl();
        if (!res)
        {
            working_buffer = Buffer(pos, pos);
        }
        else
        {
            pos = working_buffer.begin() + std::min(nextimpl_working_buffer_offset, working_buffer.size());
            chassert(position() < working_buffer.end());
        }
        nextimpl_working_buffer_offset = 0;

        chassert(position() <= working_buffer.end());

        return res;
    }


    void nextIfAtEnd()
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
    [[nodiscard]] bool ALWAYS_INLINE read(char & c)
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
    [[nodiscard]] size_t read(char * to, size_t n)
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
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                            "Cannot read all data. Bytes read: {}. Bytes expected: {}.", read_bytes, std::to_string(n));
    }

    /** A method that can be more efficiently implemented in derived classes, in the case of reading large enough blocks.
      * The implementation can read data directly into `to`, without superfluous copying, if in `to` there is enough space for work.
      * For example, a CompressedReadBuffer can decompress the data directly into `to`, if the entire decompressed block fits there.
      * By default - the same as read.
      * Don't use for small reads.
      */
    [[nodiscard]] virtual size_t readBig(char * to, size_t n) { return read(to, n); }

    /** Do something to allow faster subsequent call to 'nextImpl' if possible.
      * It's used for asynchronous readers with double-buffering.
      * `priority` is the `ThreadPool` priority, with which the prefetch task will be scheduled.
      * Lower value means higher priority.
      */
    virtual void prefetch(Priority) {}

    /**
     * Set upper bound for read range [..., position).
     * Useful for reading from remote filesystem, when it matters how much we read.
     * Doesn't affect getFileSize().
     * See also: SeekableReadBuffer::supportsRightBoundedReads().
     *
     * Behavior in weird cases is currently implementation-defined:
     *  - setReadUntilPosition() below current position,
     *  - setReadUntilPosition() above the end of the file,
     *  - seek() to a position above the until position (even if you setReadUntilPosition() to a
     *    higher value right after the seek!),
     *
     * Implementations are recommended to:
     *  - Allow the read-until-position to go below current position, e.g.:
     *      // Read block [300, 400)
     *      setReadUntilPosition(400);
     *      seek(300);
     *      next();
     *      // Read block [100, 200)
     *      setReadUntilPosition(200); // oh oh, this is below the current position, but should be allowed
     *      seek(100); // but now everything's fine again
     *      next();
     *      // (Swapping the order of seek and setReadUntilPosition doesn't help: then it breaks if the order of blocks is reversed.)
     *  - Check if new read-until-position value is equal to the current value and do nothing in this case,
     *    so that the caller doesn't have to.
     *
     * Typical implementations discard any current buffers and connections when the
     * read-until-position changes even by a small (nonzero) amount.
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
        throw Exception(ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF, "Attempt to read after eof");
    }
};


using ReadBufferPtr = std::shared_ptr<ReadBuffer>;

/// Due to inconsistencies in ReadBuffer-family interfaces:
///  - some require to fully wrap underlying buffer and own it,
///  - some just wrap the reference without ownership,
/// we need to be able to wrap reference-only buffers with movable transparent proxy-buffer.
/// The uniqueness of such wraps is responsibility of the code author.
std::unique_ptr<ReadBuffer> wrapReadBufferReference(ReadBuffer & ref);
std::unique_ptr<ReadBuffer> wrapReadBufferPointer(ReadBufferPtr ptr);

}
