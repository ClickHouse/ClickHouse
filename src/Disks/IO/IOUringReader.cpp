#if defined(OS_LINUX)

#include "IOUringReader.h"
#include <base/errnoToString.h>
#include <Common/assert_cast.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>
#include <Common/setThreadName.h>
#include <future>

namespace ProfileEvents
{
    extern const Event ReadBufferFromFileDescriptorRead;
    extern const Event ReadBufferFromFileDescriptorReadFailed;
    extern const Event ReadBufferFromFileDescriptorReadBytes;

    extern const Event IOUringSQEsSubmitted;
    extern const Event IOUringSQEsResubmits;
    extern const Event IOUringShortReads;
}

namespace CurrentMetrics
{
    extern const Metric IOUringPendingEvents;
    extern const Metric IOUringInFlightEvents;
    extern const Metric Read;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int IO_URING_INIT_FAILED;
    extern const int IO_URING_SUBMIT_ERROR;
    extern const int IO_URING_WAIT_ERROR;
}

IOUringReader::IOUringReader(uint32_t entries_)
{
    struct io_uring_probe * probe = io_uring_get_probe();
    if (!probe)
    {
        is_supported = false;
        return;
    }

    is_supported = io_uring_opcode_supported(probe, IORING_OP_READ);
    io_uring_free_probe(probe);

    if (!is_supported)
        return;

    struct io_uring_params params =
    {
        .cq_entries = 0, // filled by the kernel, initializing to silence warning
        .flags = 0,
    };

    int ret = io_uring_queue_init_params(entries_, &ring, &params);
    if (ret < 0)
        throwFromErrno("Failed initializing io_uring", ErrorCodes::IO_URING_INIT_FAILED, -ret);

    cq_entries = params.cq_entries;
    ring_completion_monitor = ThreadFromGlobalPool([this] { monitorRing(); });
}

std::future<IAsynchronousReader::Result> IOUringReader::submit(Request request)
{
    assert(request.size);

    // take lock here because we're modifying containers and submitting to the ring,
    // the monitor thread can also do the same
    std::unique_lock lock{mutex};

    // use the requested read destination address as the request id, the assumption
    // here is that we won't get asked to fill in the same address more than once in parallel
    auto request_id = reinterpret_cast<UInt64>(request.buf);

    std::promise<IAsynchronousReader::Result> promise;
    auto enqueued_request = EnqueuedRequest
    {
        .promise = std::move(promise),
        .request = request,
        .resubmitting = false,
        .bytes_read = 0
    };

    // if there's room in the completion queue submit the request to the ring immediately,
    // otherwise push it to the back of the pending queue
    if (in_flight_requests.size() < cq_entries)
    {
        if (submitToRing(enqueued_request))
        {
            const auto [kv, success] = in_flight_requests.emplace(request_id, std::move(enqueued_request));
            if (!success)
            {
                ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadFailed);
                return makeFailedResult(ErrorCodes::LOGICAL_ERROR, "Tried enqueuing read request for {} that is already submitted", request_id);
            }
            return (kv->second).promise.get_future();
        }
        else
        {
            ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadFailed);
            return makeFailedResult(ErrorCodes::IO_URING_SUBMIT_ERROR, "Failed submitting SQE");
        }
    }
    else
    {
        CurrentMetrics::add(CurrentMetrics::IOUringPendingEvents);
        pending_requests.push_back(std::move(enqueued_request));
        return pending_requests.back().promise.get_future();
    }
}

bool IOUringReader::submitToRing(EnqueuedRequest & enqueued)
{
    struct io_uring_sqe * sqe = io_uring_get_sqe(&ring);
    if (!sqe)
        return false;

    auto request = enqueued.request;
    auto request_id = reinterpret_cast<UInt64>(request.buf);
    int fd = assert_cast<const LocalFileDescriptor &>(*request.descriptor).fd;

    io_uring_sqe_set_data64(sqe, request_id);
    io_uring_prep_read(sqe, fd, request.buf, request.size - enqueued.bytes_read, request.offset + enqueued.bytes_read);
    int submitted = io_uring_submit(&ring);

    if (submitted <= 0)
        return false;

    if (!enqueued.resubmitting)
    {
        ProfileEvents::increment(ProfileEvents::IOUringSQEsSubmitted);
        CurrentMetrics::add(CurrentMetrics::IOUringInFlightEvents);
        CurrentMetrics::add(CurrentMetrics::Read);
    }

    return true;
}

void IOUringReader::failRequest(const EnqueuedIterator & requestIt, int code, const std::string & message)
{
    ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadFailed);
    (requestIt->second).promise.set_exception(std::make_exception_ptr(Exception(code, message)));

    finalizeRequest(requestIt);
}

void IOUringReader::finalizeRequest(const EnqueuedIterator & requestIt)
{
    in_flight_requests.erase(requestIt);

    CurrentMetrics::sub(CurrentMetrics::IOUringInFlightEvents);
    CurrentMetrics::sub(CurrentMetrics::Read);

    // since we just finalized a request there's now room in the completion queue,
    // see if there are any pending requests and submit one from the front of the queue
    if (!pending_requests.empty())
    {
        auto pending_request = std::move(pending_requests.front());
        pending_requests.pop_front();

        if (submitToRing(pending_request))
        {
            auto request_id = reinterpret_cast<UInt64>(pending_request.request.buf);
            if (!in_flight_requests.contains(request_id))
                in_flight_requests.emplace(request_id, std::move(pending_request));
            else
                failPromise(pending_request.promise,
                    ErrorCodes::LOGICAL_ERROR,
                    "Tried enqueuing pending read request for {} that is already submitted", request_id);
        }
        else
        {
            failPromise(pending_request.promise, ErrorCodes::IO_URING_SUBMIT_ERROR, "Failed submitting SQE for pending request");
            ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadFailed);
        }

        CurrentMetrics::sub(CurrentMetrics::IOUringPendingEvents);
    }
}

void IOUringReader::monitorRing()
{
    setThreadName("IOUringMonitor");

    while (!cancelled.load(std::memory_order_relaxed))
    {
        // we can't use wait_cqe_* variants with timeouts as they can
        // submit timeout events in older kernels that do not support IORING_FEAT_EXT_ARG
        // and it is not safe to mix submission and consumption event threads.
        struct io_uring_cqe * cqe;
        int ret = io_uring_wait_cqe(&ring, &cqe);

        if (ret == -EAGAIN)
            continue;

        if (ret < 0)
            throwFromErrno("Failed waiting for io_uring CQEs", ErrorCodes::IO_URING_WAIT_ERROR, -ret);

        // user_data zero means a noop event sent from the destructor meant to interrupt the thread
        if (cancelled.load(std::memory_order_relaxed) || cqe->user_data == 0)
            break;

        // it is safe to re-submit events once we take the lock here
        std::unique_lock lock{mutex};

        auto request_id = cqe->user_data;
        const auto it = in_flight_requests.find(request_id);
        if (it == in_flight_requests.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got a completion event for a request {} that was not submitted", request_id);

        auto & enqueued = it->second;

        if (cqe->res == -EAGAIN)
        {
            enqueued.resubmitting = true;
            ProfileEvents::increment(ProfileEvents::IOUringSQEsResubmits);

            if (!submitToRing(enqueued))
                failRequest(it, ErrorCodes::IO_URING_SUBMIT_ERROR, "Failed re-submitting SQE");

            io_uring_cqe_seen(&ring, cqe);
            continue;
        }

        if (cqe->res < 0)
        {
            int fd = assert_cast<const LocalFileDescriptor &>(*enqueued.request.descriptor).fd;
            failRequest(it, ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, fmt::format("Cannot read from file {}", fd));

            io_uring_cqe_seen(&ring, cqe);
            continue;
        }

        size_t bytes_read = cqe->res;
        size_t total_bytes_read = enqueued.bytes_read + bytes_read;

        if (bytes_read > 0)
        {
            ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorRead);
            ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadBytes, bytes_read);
            if (enqueued.bytes_read > 0) ProfileEvents::increment(ProfileEvents::IOUringShortReads);
        }

        if (bytes_read > 0 && total_bytes_read < enqueued.request.size)
        {
            // potential short read, re-submit
            enqueued.resubmitting = true;
            enqueued.bytes_read += bytes_read;
            if (!submitToRing(enqueued))
                failRequest(it, ErrorCodes::IO_URING_SUBMIT_ERROR, "Failed re-submitting SQE for short read");
        }
        else
        {
            enqueued.promise.set_value(Result{ .size = total_bytes_read, .offset = enqueued.request.ignore });
            finalizeRequest(it);
        }

        io_uring_cqe_seen(&ring, cqe);
    }
}

IOUringReader::~IOUringReader()
{
    cancelled.store(true, std::memory_order_relaxed);

    // interrupt the monitor thread by sending a noop event
    {
        std::unique_lock lock{mutex};

        struct io_uring_sqe * sqe = io_uring_get_sqe(&ring);
        io_uring_prep_nop(sqe);
        io_uring_sqe_set_data(sqe, nullptr);
        io_uring_submit(&ring);
    }

    ring_completion_monitor.join();

    io_uring_queue_exit(&ring);
}

}
#endif
