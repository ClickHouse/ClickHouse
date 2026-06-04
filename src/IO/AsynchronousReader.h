#pragma once

#include <Core/Types.h>
#include <optional>
#include <memory>
#include <future>
#include <boost/noncopyable.hpp>
#include <Common/Stopwatch.h>
#include <Common/Priority.h>


namespace DB
{

class PageCacheCell;
using PageCacheCellPtr = std::shared_ptr<PageCacheCell>;

/** Interface for asynchronous reads from file descriptors.
  * It can abstract Linux AIO, io_uring or normal reads from separate thread pool,
  * and also reads from non-local filesystems.
  * The implementation not necessarily to be efficient for large number of small requests,
  * instead it should be ok for moderate number of sufficiently large requests
  * (e.g. read 1 MB of data 50 000 times per seconds; BTW this is normal performance for reading from page cache).
  * For example, this interface may not suffice if you want to serve 10 000 000 of 4 KiB requests per second.
  * This interface is fairly limited.
  */
class IAsynchronousReader : private boost::noncopyable
{
public:
    /// For local filesystems, the file descriptor is simply integer
    /// but it can be arbitrary opaque object for remote filesystems.
    struct IFileDescriptor
    {
        virtual ~IFileDescriptor() = default;
    };

    using FileDescriptorPtr = std::shared_ptr<IFileDescriptor>;

    struct LocalFileDescriptor : public IFileDescriptor
    {
        explicit LocalFileDescriptor(int fd_) : fd(fd_) {}
        int fd;
    };

    /// Read from file descriptor at specified offset up to size bytes into buf.
    /// Some implementations may require alignment and it is responsibility of
    /// the caller to provide conforming requests.
    struct Request
    {
        FileDescriptorPtr descriptor;
        size_t offset = 0;
        size_t size = 0;
        /// If descriptor is a RemoteFSFileDescriptor containing a CachedInMemoryReadBufferFromFile
        /// then `buf` can be nullptr, and PageCacheCell buffer will be used instead (to avoid
        /// copying data out of userspace page cache).
        char * buf = nullptr;
        Priority priority;
        /// Some implementations require ignore < size.
        /// AsynchronousBoundedReadBuffer may set ignore >= size, so it should only be used with
        /// implementations that don't require that.
        size_t ignore = 0;
        bool direct_io = false;
    };

    struct Result
    {
        /// The read data is at [buf + offset, buf + size).
        /// (Notice that `offset` is included in `size`.)
        /// buf is either the buf from Request or inside page_cache_cell's buffer,
        /// or nullptr if size = 0.
        /// Less than requested amount of data can be returned.
        /// If size is zero - the file has ended.
        /// (for example, EINTR must be handled by implementation automatically)
        char * buf = nullptr;
        size_t size = 0;
        size_t offset = 0;

        /// File offset corresponding to `buf + size`. Equal to:
        /// request.offset + request.ignore + (result.size - result.offset)
        size_t file_offset_of_buffer_end = 0;

        /// If not null, `buf` points into it, so this shared_ptr must be kept alive for as long
        /// as `buf` is in use.
        PageCacheCellPtr page_cache_cell = {};

        std::unique_ptr<Stopwatch> execution_watch = {};
    };

    /// Submit request and obtain a handle. This method don't perform any waits.
    /// If this method did not throw, the caller must wait for the result with 'wait' method
    /// or destroy the whole reader before destroying the buffer for request.
    /// The method can be called concurrently from multiple threads.
    virtual std::future<Result> submit(Request request) = 0;
    virtual Result execute(Request request) = 0;

    virtual void wait() = 0;

    /// Destructor must wait for all not completed request and ignore the results.
    /// It may also cancel the requests.
    virtual ~IAsynchronousReader() = default;
};

using AsynchronousReaderPtr = std::shared_ptr<IAsynchronousReader>;

}
