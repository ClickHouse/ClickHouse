#pragma once

#include <IO/ReadBuffer.h>
#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


class SeekableReadBuffer : public ReadBuffer
{
public:
    SeekableReadBuffer(Position ptr, size_t size)
        : ReadBuffer(ptr, size) {}
    SeekableReadBuffer(Position ptr, size_t size, size_t offset)
        : ReadBuffer(ptr, size, offset) {}

    /**
     * Shifts buffer current position to given offset.
     * @param off Offset.
     * @param whence Seek mode (@see SEEK_SET, @see SEEK_CUR).
     * @return New position from the beginning of underlying buffer / file.
     */
    virtual off_t seek(off_t off, int whence) = 0;

    /**
     * Keep in mind that seekable buffer may encounter eof() once and the working buffer
     * may get into inconsistent state. Don't forget to reset it on the first nextImpl()
     * after seek().
     */

    /**
     * @return Offset from the begin of the underlying buffer / file corresponds to the buffer current position.
     */
    virtual off_t getPosition() = 0;

    virtual String getInfoForLog() { return ""; }

    virtual size_t getFileOffsetOfBufferEnd() const { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented"); }
};

using SeekableReadBufferPtr = std::shared_ptr<SeekableReadBuffer>;


class SeekableReadBufferWithSize : public SeekableReadBuffer
{
public:
    SeekableReadBufferWithSize(Position ptr, size_t size)
        : SeekableReadBuffer(ptr, size) {}
    SeekableReadBufferWithSize(Position ptr, size_t size, size_t offset)
        : SeekableReadBuffer(ptr, size, offset) {}

    /// set std::nullopt in case it is impossible to find out total size.
    virtual std::optional<size_t> getTotalSize() = 0;

protected:
    std::optional<size_t> file_size;
};

}
