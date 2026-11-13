#pragma once

#include <algorithm>
#include <concepts>
#include <type_traits>
#include <Core/Defines.h>


namespace DB
{

class MMapReadBufferFromFileDescriptor;
class MMapReadBufferFromFileWithCache;

template <typename Storage>
concept PaddedBufferStorage = requires(const Storage & storage)
{
    { storage.data() };
    { storage.size() } -> std::convertible_to<size_t>;
    requires std::is_pointer_v<decltype(storage.data())>;
    requires sizeof(std::remove_pointer_t<decltype(storage.data())>) == 1;
    requires std::remove_cvref_t<Storage>::pad_right >= PADDING_FOR_SIMD - 1;
};


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

        [[nodiscard]] Position begin() const { return begin_pos; }
        [[nodiscard]] Position end() const { return end_pos; }
        [[nodiscard]] size_t size() const { return size_t(end_pos - begin_pos); }
        void resize(size_t size) { end_pos = begin_pos + size; }
        [[nodiscard]] bool empty() const { return size() == 0; }

        void swap(Buffer & other) noexcept
        {
            std::swap(begin_pos, other.begin_pos);
            std::swap(end_pos, other.end_pos);
        }

    private:
        Position begin_pos;
        Position end_pos;        /// 1 byte after the end of the buffer
    };

    /** A working range together with the promise that SIMD readers may access the padding after it.
      * Constructing or assigning from a raw `Buffer` clears the promise. Copying another
      * `WorkingBuffer` preserves it. The range is only exposed as `const Buffer &`, because mutating it
      * through a `Buffer &` would bypass the padding bookkeeping.
      */
    struct WorkingBuffer
    {
        WorkingBuffer(Position begin_pos_, Position end_pos_)
            : buffer(begin_pos_, end_pos_)
        {
        }

        WorkingBuffer(const Buffer & buffer_) /// NOLINT(google-explicit-constructor)
            : buffer(buffer_)
        {
        }

        WorkingBuffer(const WorkingBuffer &) = default;
        WorkingBuffer & operator=(const WorkingBuffer &) = default;

        WorkingBuffer & operator=(const Buffer & other)
        {
            buffer = other;
            padded_end = nullptr;
            return *this;
        }

        operator const Buffer &() const { return buffer; } /// NOLINT(google-explicit-constructor)

        [[nodiscard]] Position begin() const { return buffer.begin(); }
        [[nodiscard]] Position end() const { return buffer.end(); }
        [[nodiscard]] size_t size() const { return buffer.size(); }
        [[nodiscard]] bool empty() const { return buffer.empty(); }

        void resize(size_t size)
        {
            if (padded_end && size > static_cast<size_t>(padded_end - begin()))
                padded_end = nullptr;
            buffer.resize(size);
        }

        /** Construct a padded range only from storage whose type guarantees enough right padding.
          * The logical range must be contained in the storage's logical size; its allocation
          * supplies the padding after that size.
          */
        template <PaddedBufferStorage Storage>
        static WorkingBuffer fromPaddedStorage(const Storage & storage, size_t storage_offset, size_t size)
        {
            chassert(storage_offset <= storage.size());
            chassert(size <= storage.size() - storage_offset);

            auto * storage_begin = const_cast<Position>(reinterpret_cast<const char *>(storage.data()));
            if (!storage_begin)
            {
                chassert(storage.size() == 0);
                return WorkingBuffer(nullptr, nullptr);
            }

            return WorkingBuffer(
                storage_begin + storage_offset, storage_begin + storage_offset + size, storage_begin + storage.size(), PaddedTag{});
        }

        WorkingBuffer subrange(Position begin_pos_, Position end_pos_) const
        {
            chassert(begin_pos_ >= begin());
            chassert(end_pos_ >= begin_pos_);
            chassert(end_pos_ <= end());

            WorkingBuffer result(begin_pos_, end_pos_);
            result.padded_end = padded_end;
            return result;
        }

        void swap(WorkingBuffer & other) noexcept
        {
            buffer.swap(other.buffer);
            std::swap(padded_end, other.padded_end);
        }

        [[nodiscard]] bool isPadded() const { return padded_end != nullptr; }

    private:
        struct PaddedTag
        {
        };

        WorkingBuffer(Position begin_pos_, Position end_pos_, Position padded_end_, PaddedTag)
            : buffer(begin_pos_, end_pos_)
            , padded_end(padded_end_)
        {
        }

        static WorkingBuffer assumePadded(Position begin_pos_, Position end_pos_)
        {
            return WorkingBuffer(begin_pos_, end_pos_, end_pos_, PaddedTag{});
        }

        Buffer buffer;
        Position padded_end = nullptr;

        friend class BufferBase;
    };

    /** The constructor takes a range of memory to use for the buffer.
      * offset - the starting point of the cursor. ReadBuffer must set it to the end of the range, and WriteBuffer - to the beginning.
      */
    BufferBase(Position ptr, size_t size, size_t offset)
        : pos(ptr + offset)
        , working_buffer(ptr, ptr + size)
        , internal_buffer(ptr, ptr + size)
    {
    }

    /// Assign the buffers and pos.
    /// Be careful when calling this from ReadBuffer::nextImpl() implementations: `offset` is
    /// effectively ignored because ReadBuffer::next() reassigns `pos`.
    void set(Position ptr, size_t size, size_t offset)
    {
        internal_buffer = Buffer(ptr, ptr + size);
        working_buffer = WorkingBuffer(ptr, ptr + size);
        pos = ptr + offset;
    }

    /// get buffer
    [[nodiscard]] const Buffer & internalBuffer() const { return internal_buffer; }

    /// get the part of the buffer from which you can read / write data
    WorkingBuffer & buffer() { return working_buffer; }

    /// get (for reading and modifying) the position in the buffer
    Position & position() { return pos; }

    /// offset in bytes of the cursor from the beginning of the buffer
    [[nodiscard]] size_t offset() const { return size_t(pos - working_buffer.begin()); }

    /// How many bytes are available for read/write
    [[nodiscard]] size_t available() const { return size_t(working_buffer.end() - pos); }

    void swap(BufferBase & other) noexcept
    {
        internal_buffer.swap(other.internal_buffer);
        working_buffer.swap(other.working_buffer);
        std::swap(pos, other.pos);
    }

    /** How many bytes have been read/written, counting those that are still in the buffer. */
    [[nodiscard]] size_t count() const { return bytes + offset(); }

    /** Check that there is more bytes in buffer after cursor. */
    [[nodiscard]] bool ALWAYS_INLINE hasPendingData() const { return available() > 0; }

    [[nodiscard]] bool isPadded() const { return working_buffer.isPadded(); }

protected:
    template <PaddedBufferStorage Storage>
    void setPaddedStorage(const Storage & storage, size_t storage_offset, size_t size, size_t offset)
    {
        working_buffer = WorkingBuffer::fromPaddedStorage(storage, storage_offset, size);
        internal_buffer = working_buffer;
        pos = working_buffer.begin() + offset;
    }

    void setWorkingBuffer(WorkingBuffer buffer, size_t offset)
    {
        working_buffer = buffer;
        internal_buffer = buffer;
        pos = working_buffer.begin() + offset;
    }

    void setInternalBuffer(Buffer buffer) { internal_buffer = buffer; }

    template <PaddedBufferStorage Storage>
    void setPaddedInternalBuffer(const Storage & storage, size_t storage_offset, size_t size)
    {
        auto padded_buffer = WorkingBuffer::fromPaddedStorage(storage, storage_offset, size);
        internal_buffer = padded_buffer;
    }

    void setWorkingBufferFromInternal() { working_buffer = internal_buffer; }

    void clearBufferPadding()
    {
        internal_buffer.padded_end = nullptr;
        working_buffer.padded_end = nullptr;
    }

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
    WorkingBuffer working_buffer;

    /// A reference to a piece of memory for the buffer.
    WorkingBuffer internal_buffer;
    bool canceled{false};

private:
    /// The mmap readers establish padding from page boundaries, which no storage type can express.
    void setAssumePadded(Position ptr, size_t size, size_t offset)
    {
        working_buffer = WorkingBuffer::assumePadded(ptr, ptr + size);
        internal_buffer = working_buffer;
        pos = ptr + offset;
    }

    friend class MMapReadBufferFromFileDescriptor;
    friend class MMapReadBufferFromFileWithCache;
};


}
