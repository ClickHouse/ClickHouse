#if defined(OS_LINUX)

#include "IOUringReader.h"
#include <Common/assert_cast.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>
#include <Common/setThreadName.h>
#include <base/errnoToString.h>
#include <future>

namespace ProfileEvents
{
    extern const Event ReadBufferFromFileDescriptorRead;
    extern const Event ReadBufferFromFileDescriptorReadFailed;
    extern const Event ReadBufferFromFileDescriptorReadBytes;

    extern const Event IOUringSQEQueueFullRetries;
    extern const Event IOUringSQEsSubmitted;
    extern const Event IOUringSQEsResubmits;
    extern const Event IOUringShortReads;
}

namespace CurrentMetrics
{
    extern const Metric IOUringEnqueuedEvents;
    extern const Metric Read;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int IO_URING_INIT_FAILED;
    extern const int IO_URING_QUEUE_FULL;
    extern const int IO_URING_WAIT_ERROR;
}

IOUringReader::IOUringReader(size_t queue_size_)
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

    int ret = io_uring_queue_init(queue_size_, &ring, 0);
    if (ret < 0)
        throwFromErrno(fmt::format("Failed initializing io_uring of size {}", queue_size_), ErrorCodes::IO_URING_INIT_FAILED);

    ring_completion_monitor = ThreadFromGlobalPool([this] { monitorRing(); });
}

std::future<IAsynchronousReader::Result> IOUringReader::submit(Request request)
{
    assert(request.size);

    // take lock here because we're modifying an std::map and submitting to the ring,
    // the monitor thread can also modify the map and re-submit events
    std::unique_lock lock{mutex};

    // use the requested read destination address as the request id, the assumption
    // here is that we won't get asked to fill in the same address more than once in parallel
    auto request_id = reinterpret_cast<UInt64>(request.buf);

    const auto [kv, is_newly_inserted] = enqueued_requests.emplace(request_id, EnqueuedRequest{
        .promise = std::promise<IAsynchronousReader::Result>{},
        .request = request,
        .bytes_read = 0
    });

    if (!is_newly_inserted)
        return makeFailedResult(ErrorCodes::LOGICAL_ERROR, "Tried enqueuing read request for {} that is already submitted", request_id);

    EnqueuedRequest & enqueued = kv->second;
    trySubmitRequest(request_id, enqueued, false);

    return enqueued.promise.get_future();
}

bool IOUringReader::trySubmitRequest(UInt64 request_id, EnqueuedRequest & enqueued, bool resubmitting)
{
    auto request = enqueued.request;
    int fd = assert_cast<const LocalFileDescriptor &>(*request.descriptor).fd;

    struct io_uring_sqe * sqe = nullptr;
    for (int i = 0; i < 1000; ++i) // try a few times in case the queue is full.
    {
        sqe = io_uring_get_sqe(&ring);
        if (sqe) break;

        ProfileEvents::increment(ProfileEvents::IOUringSQEQueueFullRetries);
    }

    int submitted = 0;
    if (sqe)
    {
        io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(request_id));
        io_uring_prep_read(sqe, fd, request.buf, request.size - enqueued.bytes_read, request.offset + enqueued.bytes_read);
        submitted = io_uring_submit(&ring);
    }

    if (submitted == 0)
    {
        failPromise(enqueued.promise, ErrorCodes::IO_URING_QUEUE_FULL, "Submission queue is full, failed to get an SQE");
        if (resubmitting) // an existing request failed, decrement counters
        {
            ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadFailed);
            CurrentMetrics::sub(CurrentMetrics::IOUringEnqueuedEvents);
            CurrentMetrics::sub(CurrentMetrics::Read);
        }

        return false;
    }

    if (!resubmitting) // new request, increment counters
    {
        ProfileEvents::increment(ProfileEvents::IOUringSQEsSubmitted);
        CurrentMetrics::add(CurrentMetrics::IOUringEnqueuedEvents);
        CurrentMetrics::add(CurrentMetrics::Read);
    }

    return true;
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

        if (ret < 0)
            throwFromErrno(fmt::format("Failed waiting for io_uring CQEs: {}", ret), ErrorCodes::IO_URING_WAIT_ERROR);

        // user_data zero means a noop event sent from the destructor meant to interrupt the thread
        if (cancelled.load(std::memory_order_relaxed) || cqe->user_data == 0)
            break;

        // it is safe to re-submit events once we take the lock here
        std::unique_lock lock{mutex};

        auto request_id = cqe->user_data;
        const auto it = enqueued_requests.find(request_id);
        if (it == enqueued_requests.end())
            throwFromErrno(
                fmt::format("Got a completion event for a request {} that was not submitted", request_id),
                ErrorCodes::LOGICAL_ERROR);

        auto & enqueued = it->second;

        if (cqe->res < 0)
        {
            if (cqe->res == -EAGAIN)
            {
                ProfileEvents::increment(ProfileEvents::IOUringSQEsResubmits);

                trySubmitRequest(request_id, enqueued, true);
                io_uring_cqe_seen(&ring, cqe);

                continue;
            }

            int fd = assert_cast<const LocalFileDescriptor &>(*enqueued.request.descriptor).fd;

            failPromise(enqueued.promise, ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Cannot read from file {}", fd);
            enqueued_requests.erase(it);

            ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadFailed);
            CurrentMetrics::sub(CurrentMetrics::IOUringEnqueuedEvents);
            CurrentMetrics::sub(CurrentMetrics::Read);

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
            enqueued.bytes_read += bytes_read;
            trySubmitRequest(request_id, enqueued, true);
        }
        else
        {
            enqueued.promise.set_value(Result{ .size = total_bytes_read, .offset = enqueued.request.ignore });
            enqueued_requests.erase(it);

            CurrentMetrics::sub(CurrentMetrics::IOUringEnqueuedEvents);
            CurrentMetrics::sub(CurrentMetrics::Read);
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
