#pragma once

#include <IO/AsynchronousReader.h>
#include <IO/AIO.h>
#include <Common/assert_cast.h>
#include <Common/Exception.h>
#include <Common/MemorySanitizer.h>
#include <common/errnoToString.h>
#include <Poco/Event.h>
#include <unordered_map>
#include <mutex>
#include <list>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/eventfd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int CANNOT_SCHEDULE_TASK;
    extern const int CANNOT_IO_SUBMIT;
    extern const int CANNOT_IO_GETEVENTS;
}

/** Perform reads using Linux AIO. Don't be confused with POSIX AIO.
  * NOTE: it is creating eventfd for every operation - inefficient.
  */
class AIOReader final : public IAsynchronousReader
{
private:
    UInt64 counter = 0;

    struct Context
    {
        Context(size_t max_requests_in_flight)
            : aio_context(max_requests_in_flight) {}

        AIOContext aio_context;
        size_t num_requests_in_flight = 0;
        std::mutex getevents_mutex;
    };

    std::list<Context> contexts;

    struct RequestInfo
    {
        Context * context = nullptr;
        int event_fd = -1;
        Result result;

        RequestInfo(Context * context_) : context(context_)
        {
        }

        ~RequestInfo()
        {
            if (-1 != event_fd && 0 != close(event_fd))
                std::terminate();
        }
    };

    using Requests = std::unordered_map<UInt64, RequestInfo>;
    Requests requests;
    std::mutex mutex;

    const size_t max_requests_in_flight_for_context;
    const size_t max_number_of_aio_contexts;

public:
    AIOReader(size_t max_requests_in_flight_for_context_, size_t max_number_of_aio_contexts_)
        : max_requests_in_flight_for_context(max_requests_in_flight_for_context_),
        max_number_of_aio_contexts(max_number_of_aio_contexts_)
    {
    }

    RequestID submit(Request request) override
    {
        int fd = assert_cast<const LocalFileDescriptor &>(*request.descriptor).fd;

        Requests::iterator it;

        {
            std::lock_guard lock(mutex);

            if (requests.size() >= max_number_of_aio_contexts * max_requests_in_flight_for_context)
                throw Exception("Too many read requests in flight", ErrorCodes::CANNOT_SCHEDULE_TASK);

            ++counter;
            Context * used_context = nullptr;

            size_t size = contexts.size();
            for (auto & context : contexts)
            {
                if (context.num_requests_in_flight < max_requests_in_flight_for_context)
                {
                    used_context = &context;
                    break;
                }
            }

            if (!used_context)
            {
                if (size >= max_number_of_aio_contexts)
                    throw Exception("Too many read requests in flight", ErrorCodes::CANNOT_SCHEDULE_TASK);

                contexts.emplace_back(max_requests_in_flight_for_context);
                used_context = &contexts.back();
            }

            ++used_context->num_requests_in_flight;

            it = requests.emplace(std::piecewise_construct,
                std::forward_as_tuple(counter),
                std::forward_as_tuple(used_context)).first;
        }

        try
        {
            it->second.event_fd = eventfd(0, EFD_CLOEXEC);
            if (-1 == it->second.event_fd)
                throwFromErrno(fmt::format("Cannot create eventfd for asynchronous IO on file {}", fd), ErrorCodes::CANNOT_IO_SUBMIT);

            iocb aio_request{};
            iocb * aio_request_ptr{&aio_request};

            aio_request.aio_lio_opcode = IOCB_CMD_PREAD;
            aio_request.aio_fildes = fd;
            aio_request.aio_buf = reinterpret_cast<UInt64>(request.buf);
            aio_request.aio_nbytes = request.size;
            aio_request.aio_offset = request.offset;
            aio_request.aio_data = reinterpret_cast<UInt64>(&it->second.result);
            aio_request.aio_flags = IOCB_FLAG_RESFD;
            aio_request.aio_resfd = it->second.event_fd;

            while (io_submit(it->second.context->aio_context.ctx, 1, &aio_request_ptr) < 0)
                if (errno != EINTR)
                    throwFromErrno(fmt::format("Cannot submit request for asynchronous IO on file {}", fd), ErrorCodes::CANNOT_IO_SUBMIT);

            return it->first;
        }
        catch (...)
        {
            {
                std::lock_guard lock(mutex);
                --it->second.context->num_requests_in_flight;
                requests.erase(it);
            }
            throw;
        }
    }

    std::optional<Result> wait(RequestID id, std::optional<UInt64> microseconds) override
    {
        Result result;
        Requests::iterator it;

        {
            std::lock_guard lock(mutex);
            it = requests.find(id);
            if (it == requests.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find request by id {}", id);
        }

        RequestInfo & info = it->second;

        if (microseconds)
        {
            struct pollfd poll_fd{};
            poll_fd.fd = info.event_fd;
            poll_fd.events = POLLIN;

            while (true)
            {
                int res = poll(&poll_fd, 1, *microseconds / 1000);

                if (-1 == res)
                {
                    if (EINTR == errno)
                        continue;

                    throwFromErrno(fmt::format("Cannot poll eventfd for asynchronous IO on file {}", info.event_fd),
                        ErrorCodes::CANNOT_IO_GETEVENTS);
                }

                if (res > 1)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Linux kernel returned more ready file descriptors than we asked");

                /// Timeout
                if (0 == res)
                    return {};

                /// Ready
                break;
            }
        }
        else
        {
            /// Each successful read(2) returns an 8-byte integer - man eventfd.
            uint64_t unused_read_value = 0;
            while (true)
            {
                int res = read(info.event_fd, &unused_read_value, sizeof(unused_read_value));

                if (-1 == res)
                {
                    if (EINTR == errno)
                        continue;

                    throwFromErrno(fmt::format("Cannot read eventfd for asynchronous IO on file {}", info.event_fd),
                        ErrorCodes::CANNOT_IO_GETEVENTS);
                }

                if (0 == res)
                    continue;

                if (res != 8)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Linux kernel returned returned non 8 bytes result for eventfd");

                break;
            }
        }

        constexpr int max_events_to_fetch = 16;
        io_event events[max_events_to_fetch]{};
        struct timespec timeout{0, 0};

        {
            std::lock_guard getevents_lock(it->second.context->getevents_mutex);

            while (true)
            {
                int res = io_getevents(it->second.context->aio_context.ctx, 1, max_events_to_fetch, events, &timeout);

                if (res < 0)
                {
                    if (EINTR == errno)
                        continue;

                    throwFromErrno("Failed to wait for asynchronous IO completion on file", ErrorCodes::CANNOT_IO_GETEVENTS);
                }

                for (int i = 0; i < res; ++i)
                {
                    const io_event & event = events[i];

                    /// Unpoison the memory returned from an uninstrumented system function.
                    __msan_unpoison(&event, sizeof(event));

                    --it->second.context->num_requests_in_flight;

                    Result * received_result = reinterpret_cast<Result *>(event.data);
                    if (event.res < 0)
                        received_result->exception = std::make_exception_ptr(ErrnoException(
                            fmt::format("Cannot read from file",
                                errnoToString(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, errno)),
                            ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, errno));
                    else
                        received_result->size = event.res;
                }

                if (res < max_events_to_fetch)
                {
                    /// It's guaranteed that our target event is already fetched.
                    break;
                }

                /// Maybe there are more events to fetch.
            }
        }

        Result res = it->second.result;

        {
            std::lock_guard lock(mutex);
            requests.erase(it);
        }

        return res;
    }

    ~AIOReader() override
    {
    }
};

}



