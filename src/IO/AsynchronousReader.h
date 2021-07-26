#pragma once

#include <Core/Types.h>
#include <optional>
#include <memory>


namespace DB
{

/** Interface for asynchronous reads from file descriptors.
  * It can abstract Linux AIO, io_uring or normal reads from separate thread pool,
  * and also reads from non-local filesystems.
  * The implementation not necessarily to be efficient for large number of small requests,
  * instead it should be ok for moderate number of sufficiently large requests
  * (e.g. read 1 MB of data 50 000 times per seconds; BTW this is normal performance for reading from page cache).
  * For example, this interface may not suffice if you want to serve 10 000 000 of 4 KiB requests per second.
  * This interface is fairly limited.
  */
class IAsynchronousReader
{
public:
    using RequestID = UInt64;

    /// For local filesystems, the file descriptor is simply integer
    /// but it can be arbitrary opaque object for remote filesystems.
    struct IFileDescriptor
    {
        virtual ~IFileDescriptor() = default;
    };

    using FileDescriptorPtr = std::shared_ptr<IFileDescriptor>;

    struct LocalFileDescriptor : public IFileDescriptor
    {
        LocalFileDescriptor(int fd_) : fd(fd_) {}
        int fd;
    };

    /// Read from file descriptor at specified offset up to size bytes into buf.
    /// Some implementations may require alignment and it is responsibility of
    /// the caller to provide conforming requests.
    struct Request
    {
        FileDescriptorPtr descriptor;
        size_t offset;
        size_t size;
        char * buf;
        int priority = 0;
    };

    /// Less than requested amount of data can be returned.
    /// Also error can be returned in 'exception'.
    /// If no error, and the size is zero - the file has ended.
    /// (for example, EINTR must be handled automatically)
    struct Result
    {
        size_t size = 0;
        std::exception_ptr exception;
    };

    /// The methods 'submit' and 'wait' can be called concurrently from multiple threads
    /// but only for different requests.

    /// Submit request and obtain a handle. This method don't perform any waits.
    /// If this method did not throw, the caller must wait for the result with 'wait' method
    /// or destroy the whole reader before destroying the buffer for request.
    virtual RequestID submit(Request request) = 0;

    /// Wait for request completion or timeout.
    /// Optional timeout can be specified, otherwise waits until completion.
    /// In case of timeout, nullopt is returned.
    /// In case of completion, Result object is returned. Result may contain exception.
    /// In case of timeout, the caller must call wait again until completion
    /// or destroy the whole reader before destroying the buffer for request.
    virtual std::optional<Result> wait(RequestID id, std::optional<UInt64> microseconds) = 0;

    /// Destructor must wait for all not completed request and ignore the results.
    /// It may also cancel the requests.
    virtual ~IAsynchronousReader() = default;
};

using AsynchronousReaderPtr = std::shared_ptr<IAsynchronousReader>;

}
