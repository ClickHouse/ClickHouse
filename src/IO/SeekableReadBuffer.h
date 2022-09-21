#pragma once

#include <IO/ReadBuffer.h>
#include <IO/WithFileSize.h>
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

    struct Range
    {
        size_t left;
        std::optional<size_t> right;
    };

    /**
     * Returns a struct, where `left` is current read position in file and `right` is the
     * last included offset for reading according to setReadUntilPosition() or setReadUntilEnd().
     * E.g. next nextImpl() call will read within range [left, right].
     */
    virtual Range getRemainingReadRange() const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getRemainingReadRange() not implemented");
    }

    virtual String getInfoForLog() { return ""; }

    virtual size_t getFileOffsetOfBufferEnd() const { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getFileOffsetOfBufferEnd() not implemented"); }
};

using SeekableReadBufferPtr = std::shared_ptr<SeekableReadBuffer>;

}
