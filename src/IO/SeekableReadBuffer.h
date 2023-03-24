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
     *
     * What happens if you seek above the end of the file? Implementation-defined.
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

    virtual size_t getFileOffsetOfBufferEnd() const { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getFileOffsetOfBufferEnd() not implemented"); }

    // If true, setReadUntilPosition() guarantees that eof will be reported at the given position.
    virtual bool supportsRightBoundedReads() const { return false; }

    virtual bool isIntegratedWithFilesystemCache() const { return false; }
};

// Useful for reading in parallel.
class SeekableReadBufferFactory : public WithFileSize
{
public:
    ~SeekableReadBufferFactory() override = default;

    // We usually call setReadUntilPosition() and seek() on the returned buffer before reading.
    // So it's recommended that the returned implementation be lazy, i.e. don't start reading
    // before the first call to nextImpl().
    virtual std::unique_ptr<SeekableReadBuffer> getReader() = 0;
};

using SeekableReadBufferPtr = std::shared_ptr<SeekableReadBuffer>;

using SeekableReadBufferFactoryPtr = std::unique_ptr<SeekableReadBufferFactory>;

/// Wraps a reference to a SeekableReadBuffer into an unique pointer to SeekableReadBuffer.
/// This function is like wrapReadBufferReference() but for SeekableReadBuffer.
std::unique_ptr<SeekableReadBuffer> wrapSeekableReadBufferReference(SeekableReadBuffer & ref);
std::unique_ptr<SeekableReadBuffer> wrapSeekableReadBufferPointer(SeekableReadBufferPtr ptr);

}
