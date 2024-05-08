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

    /// NOTE: This method should be thread-safe against seek(), since it can be
    /// used in CachedOnDiskReadBufferFromFile from multiple threads (because
    /// it first releases the buffer, and then do logging, and so other thread
    /// can already call seek() which will lead to data-race).
    virtual size_t getFileOffsetOfBufferEnd() const { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getFileOffsetOfBufferEnd() not implemented"); }

    /// If true, setReadUntilPosition() guarantees that eof will be reported at the given position.
    virtual bool supportsRightBoundedReads() const { return false; }

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
    /// `progress_callback` may be called periodically during the read, reporting that to[0..m-1]
    /// has been filled. If it returns true, reading is stopped, and readBigAt() returns bytes read
    /// so far. Called only from inside readBigAt(), from the same thread, with increasing m.
    ///
    /// Stops either after n bytes, or at end of file, or on exception. Returns number of bytes read.
    /// If offset is past the end of file, may return 0 or throw exception.
    ///
    /// Caller needs to be careful:
    ///  * supportsReadAt() must be checked (called and return true) before calling readBigAt().
    ///    Otherwise readBigAt() may crash.
    ///  * Thread safety: multiple readBigAt() calls may be performed in parallel.
    ///    But readBigAt() may not be called in parallel with any other methods
    ///    (e.g. next() or supportsReadAt()).
    ///  * Performance: there's no buffering. Each readBigAt() call typically translates into actual
    ///    IO operation (e.g. HTTP request). Don't use it for small adjacent reads.
    virtual size_t readBigAt(char * /*to*/, size_t /*n*/, size_t /*offset*/, const std::function<bool(size_t m)> & /*progress_callback*/) const
        { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method readBigAt() not implemented"); }

    /// Checks if readBigAt() is allowed. May be slow, may throw (e.g. it may do an HTTP request or an fstat).
    virtual bool supportsReadAt() { return false; }

    /// We do some tricks to avoid seek cost. E.g we read more data and than ignore it (see remote_read_min_bytes_for_seek).
    /// Sometimes however seek is basically free because underlying read buffer wasn't yet initialised (or re-initialised after reset).
    virtual bool isSeekCheap() { return false; }

    /// For tables that have an external storage (like S3) as their main storage we'd like to distinguish whether we're reading from this storage or from a local cache.
    /// It allows to reuse all the optimisations done for reading from local tables when reading from cache.
    virtual bool isContentCached([[maybe_unused]] size_t offset, [[maybe_unused]] size_t size) { return false; }
};


using SeekableReadBufferPtr = std::shared_ptr<SeekableReadBuffer>;

/// Wraps a reference to a SeekableReadBuffer into an unique pointer to SeekableReadBuffer.
/// This function is like wrapReadBufferReference() but for SeekableReadBuffer.
std::unique_ptr<SeekableReadBuffer> wrapSeekableReadBufferReference(SeekableReadBuffer & ref);
std::unique_ptr<SeekableReadBuffer> wrapSeekableReadBufferPointer(SeekableReadBufferPtr ptr);

/// Helper for implementing readBigAt().
/// Updates *out_bytes_copied after each call to the callback, as well as at the end.
void copyFromIStreamWithProgressCallback(std::istream & istr, char * to, size_t n, const std::function<bool(size_t)> & progress_callback, size_t * out_bytes_copied, bool * out_cancelled = nullptr);

}
