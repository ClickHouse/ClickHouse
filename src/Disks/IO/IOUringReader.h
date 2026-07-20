#pragma once

#include "config.h"

#if USE_LIBURING

#include <Common/Exception.h>
#include <Common/ThreadPool_fwd.h>
#include <IO/AsynchronousReader.h>
#include <deque>
#include <unordered_map>
#include <liburing.h>

namespace Poco { class Logger; }

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class Exception;

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
    bool is_supported;

    std::mutex mutex;
    struct io_uring ring;
    uint32_t cq_entries;

    std::atomic<bool> cancelled{false};
    std::unique_ptr<ThreadFromGlobalPool> ring_completion_monitor;

    struct EnqueuedRequest
    {
        std::promise<IAsynchronousReader::Result> promise;
        Request request;
        bool resubmitting; // resubmits can happen due to short reads or when io_uring returns -EAGAIN
        size_t bytes_read; // keep track of bytes already read in case short reads happen
    };

    std::deque<EnqueuedRequest> pending_requests;
    std::unordered_map<UInt64, EnqueuedRequest> in_flight_requests;

    int submitToRing(EnqueuedRequest & enqueued);

    using EnqueuedIterator = std::unordered_map<UInt64, EnqueuedRequest>::iterator;

    void failRequest(const EnqueuedIterator & requestIt, const Exception & ex);
    void finalizeRequest(const EnqueuedIterator & requestIt);

    void monitorRing();

    template<typename T> void failPromise(std::promise<T> & promise, const Exception & ex)
    {
        promise.set_exception(std::make_exception_ptr(ex));
    }

    std::future<Result> makeFailedResult(const Exception & ex)
    {
        auto promise = std::promise<Result>{};
        failPromise(promise, ex);
        return promise.get_future();
    }

    const LoggerPtr log;

public:
    explicit IOUringReader(uint32_t entries_);

    bool isSupported() const { return is_supported; }
    std::future<Result> submit(Request request) override;
    Result execute(Request /* request */) override { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method `execute` not implemented for IOUringReader"); }

    void wait() override {}

    ~IOUringReader() override;
};

}
#endif
