#pragma once

#include <Core/Defines.h>
#include <algorithm>


namespace DB
{


/** Base class for ReadBuffer and WriteBuffer.
  * Contains common types, variables, and functions.
  *
  * ReadBuffer and WriteBuffer are similar to istream and ostream, respectively.
  * They have to be used, because using iostreams it is impossible to effectively implement some operations.
  * For example, using istream, you can not quickly read string values from a tab-separated file,
  *  so that after reading, the position remains immediately after the read value.
  * (The only option is to call the std::istream::get() function on each byte, but this slows down due to several virtual calls.)
  *
  * Read/WriteBuffers provide direct access to the internal buffer, so the necessary operations are implemented more efficiently.
  * Only one virtual function nextImpl() is used, which is rarely called:
  * - in the case of ReadBuffer - fill in the buffer with new data from the source;
  * - in the case of WriteBuffer - write data from the buffer into the receiver.
  *
  * Read/WriteBuffer can own or not own an own piece of memory.
  * In the second case, you can effectively read from an already existing piece of memory / std::string without copying it.
  */
class BufferBase
{
public:
    /** Cursor in the buffer. The position of write or read. */
    using Position = char *;

    /** A reference to the range of memory. */
    struct Buffer
    {
        Buffer(Position begin_pos_, Position end_pos_) : begin_pos(begin_pos_), end_pos(end_pos_) {}

        Position begin() const { return begin_pos; }
        Position end() const { return end_pos; }
        size_t size() const { return size_t(end_pos - begin_pos); }
        void resize(size_t size) { end_pos = begin_pos + size; }
        bool empty() const { return size() == 0; }

        void swap(Buffer & other) noexcept
        {
            std::swap(begin_pos, other.begin_pos);
            std::swap(end_pos, other.end_pos);
        }

    private:
        Position begin_pos;
        Position end_pos;        /// 1 byte after the end of the buffer
    };

    /** The constructor takes a range of memory to use for the buffer.
      * offset - the starting point of the cursor. ReadBuffer must set it to the end of the range, and WriteBuffer - to the beginning.
      */
    BufferBase(Position ptr, size_t size, size_t offset)
        : pos(ptr + offset), working_buffer(ptr, ptr + size), internal_buffer(ptr, ptr + size) {}

    /// Assign the buffers and pos.
    /// Be careful when calling this from ReadBuffer::nextImpl() implementations: `offset` is
    /// effectively ignored because ReadBuffer::next() reassigns `pos`.
    void set(Position ptr, size_t size, size_t offset)
    {
        internal_buffer = Buffer(ptr, ptr + size);
        working_buffer = Buffer(ptr, ptr + size);
        pos = ptr + offset;
    }

    /// get buffer
    Buffer & internalBuffer() { return internal_buffer; }

    /// get the part of the buffer from which you can read / write data
    Buffer & buffer() { return working_buffer; }

    /// get (for reading and modifying) the position in the buffer
    Position & position() { return pos; }

    /// offset in bytes of the cursor from the beginning of the buffer
    size_t offset() const { return size_t(pos - working_buffer.begin()); }

    /// How many bytes are available for read/write
    size_t available() const { return size_t(working_buffer.end() - pos); }

    void swap(BufferBase & other) noexcept
    {
        internal_buffer.swap(other.internal_buffer);
        working_buffer.swap(other.working_buffer);
        std::swap(pos, other.pos);
    }

    /** How many bytes have been read/written, counting those that are still in the buffer. */
    size_t count() const { return bytes + offset(); }

    /** Check that there is more bytes in buffer after cursor. */
    bool ALWAYS_INLINE hasPendingData() const { return available() > 0; }

    bool isPadded() const { return padded; }

protected:
    void resetWorkingBuffer()
    {
        /// Move position to the end of buffer to trigger call of 'next' on next reading.
        /// Discard all data in current working buffer to prevent wrong assumptions on content
        /// of buffer, e.g. for optimizations of seeks in seekable buffers.
        working_buffer.resize(0);
        pos = working_buffer.end();
    }

    /// Read/write position.
    Position pos;

    /** How many bytes have been read/written, not counting those that are now in the buffer.
      * (counting those that were already used and "removed" from the buffer)
      */
    size_t bytes = 0;

    /** A piece of memory that you can use.
      * For example, if internal_buffer is 1MB, and from a file for reading it was loaded into the buffer
      *  only 10 bytes, then working_buffer will be 10 bytes in size
      *  (working_buffer.end() will point to the position immediately after the 10 bytes that can be read).
      */
    Buffer working_buffer;

    /// A reference to a piece of memory for the buffer.
    Buffer internal_buffer;

    /// Indicator of 15 bytes pad_right
    bool padded{false};
    bool canceled{false};
};


}
