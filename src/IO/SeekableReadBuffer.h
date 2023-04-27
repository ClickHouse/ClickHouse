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

    /// If true, setReadUntilPosition() guarantees that eof will be reported at the given position.
    virtual bool supportsRightBoundedReads() const { return false; }

    virtual bool isIntegratedWithFilesystemCache() const { return false; }

    /// Returns true if seek() actually works, false if seek() will always throw (or make subsequent
    /// nextImpl() calls throw).
    ///
    /// This is needed because:
    ///  * Sometimes there's no cheap way to know in advance whether the buffer is really seekable.
    ///    Specifically, HTTP read buffer needs to send a request to check whether the server
    ///    supports byte ranges.
    ///  * Sometimes when we create such buffer we don't know in advance whether we'll need it to be
    ///    seekable or not. So we don't want to pay the price for this check in advance.
    virtual bool checkIfActuallySeekable() { return true; }

    /// Unbuffered positional read.
    /// Doesn't affect the buffer state (position, working_buffer, etc).
    ///
    /// Caller needs to be careful:
    ///  * supportsReadAt() must be checked (called and return true) before calling readBigAt().
    ///    Otherwise readBigAt() may crash.
    ///  * Thread safety: multiple readBigAt() calls may be performed in parallel.
    ///    But readBigAt() may not be called in parallel with any other methods
    ///    (e.g. next() or supportsReadAt()).
    ///  * Performance: there's no buffering. Each readBigAt() call typically translates into actual
    ///    IO operation (e.g. HTTP request). Don't use it for small adjacent reads.
    virtual size_t readBigAt(char * /*to*/, size_t /*n*/, size_t /*offset*/)
        { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method readBigAt() not implemented"); }

    /// Checks if readBigAt() is allowed. May be slow, may throw (e.g. it may do an HTTP request or an fstat).
    virtual bool supportsReadAt() { return false; }
};

/// Useful for reading in parallel.
/// The created read buffers may outlive the factory.
///
/// There are 2 ways to use this:
///  (1) Never call seek() or getFileSize(), read the file sequentially.
///      For HTTP, this usually translates to just one HTTP request.
///  (2) Call checkIfActuallySeekable(), then:
///       a. If it returned false, go to (1). seek() and getFileSize() are not available (throw if called).
///       b. If it returned true, seek() and getFileSize() are available, knock yourself out.
///      For HTTP, checkIfActuallySeekable() sends a HEAD request and returns false if the web server
///      doesn't support ranges (or doesn't support HEAD requests).
class SeekableReadBufferFactory : public WithFileSize
{
public:
    ~SeekableReadBufferFactory() override = default;

    // We usually call setReadUntilPosition() and seek() on the returned buffer before reading.
    // So it's recommended that the returned implementation be lazy, i.e. don't start reading
    // before the first call to nextImpl().
    virtual std::unique_ptr<SeekableReadBuffer> getReader() = 0;

    virtual bool checkIfActuallySeekable() { return true; }
};

using SeekableReadBufferPtr = std::shared_ptr<SeekableReadBuffer>;

using SeekableReadBufferFactoryPtr = std::unique_ptr<SeekableReadBufferFactory>;

/// Wraps a reference to a SeekableReadBuffer into an unique pointer to SeekableReadBuffer.
/// This function is like wrapReadBufferReference() but for SeekableReadBuffer.
std::unique_ptr<SeekableReadBuffer> wrapSeekableReadBufferReference(SeekableReadBuffer & ref);
std::unique_ptr<SeekableReadBuffer> wrapSeekableReadBufferPointer(SeekableReadBufferPtr ptr);

}
