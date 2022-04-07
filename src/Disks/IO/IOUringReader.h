#pragma once

#include <Common/ThreadPool.h>
#include <IO/AsynchronousReader.h>
#include <unordered_map>

#if defined(__clang__)
#pragma clang diagnostic push

#pragma clang diagnostic ignored "-Wold-style-cast"
#pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"

#ifdef HAS_RESERVED_IDENTIFIER
#pragma clang diagnostic ignored "-Wreserved-identifier"
#endif

#endif

#include <liburing.h>

#if defined(__clang__)
#pragma clang diagnostic pop
#endif

namespace DB
{

/** Perform reads using the io_uring Linux subsystem.
  *
  * The class sets up a single io_uring that clients submit read requests to, they are
  * placed in a map using the read buffer address as the key and the original request
  * with a promise as the value. A monitor thread continuously polls the completion queue,
  * looks up the completed request and completes the matching promise.
  */
class IOUringReader final : public IAsynchronousReader
{
private:
    std::mutex mutex;
    struct io_uring ring;

    std::atomic<bool> cancelled{false};
    ThreadFromGlobalPool ring_completion_monitor;

    struct EnqueuedRequest
    {
        std::promise<IAsynchronousReader::Result> promise;
        Request request;
        size_t bytes_read; // keep track of bytes already read in case short reads happen
    };

    std::unordered_map<UInt64, EnqueuedRequest> enqueued_requests;

    bool trySubmitRequest(UInt64 request_id, EnqueuedRequest & enqueued, bool resubmitting);
    void monitorRing();

    template <typename T, typename... Args> inline void failPromise(
        std::promise<T> & promise, int code, fmt::format_string<Args...> fmt, Args &&... args)
    {
        promise.set_exception(std::make_exception_ptr(Exception(code, fmt, std::forward<Args>(args)...)));
    }

    template <typename... Args> inline std::future<Result> makeFailedResult(
        int code, fmt::format_string<Args...> fmt, Args &&... args)
    {
        auto promise = std::promise<Result>{};
        failPromise(promise, code, fmt, std::forward<Args>(args)...);
        return promise.get_future();
    }

public:
    IOUringReader(size_t queue_size_);
    std::future<Result> submit(Request request) override;

    virtual ~IOUringReader() override;
};

}
