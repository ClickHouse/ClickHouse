#if defined(__linux__)

#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <Poco/Logger.h>
#include <boost/range/iterator_range.hpp>
#include <errno.h>

#include <IO/AIOContextPool.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_IO_SUBMIT;
    extern const int CANNOT_IO_GETEVENTS;
}


AIOContextPool::~AIOContextPool()
{
    cancelled.store(true, std::memory_order_relaxed);
    io_completion_monitor.join();
}


void AIOContextPool::doMonitor()
{
    /// continue checking for events unless cancelled
    while (!cancelled.load(std::memory_order_relaxed))
        waitForCompletion();

    /// wait until all requests have been completed
    while (!promises.empty())
        waitForCompletion();
}


void AIOContextPool::waitForCompletion()
{
    /// array to hold completion events
    io_event events[max_concurrent_events];

    try
    {
        const auto num_events = getCompletionEvents(events, max_concurrent_events);
        fulfillPromises(events, num_events);
        notifyProducers(num_events);
    }
    catch (...)
    {
        /// there was an error, log it, return to any producer and continue
        reportExceptionToAnyProducer();
        tryLogCurrentException("AIOContextPool::waitForCompletion()");
    }
}


int AIOContextPool::getCompletionEvents(io_event events[], const int max_events)
{
    timespec timeout{timeout_sec, 0};

    auto num_events = 0;

    /// request 1 to `max_events` events
    while ((num_events = io_getevents(aio_context.ctx, 1, max_events, events, &timeout)) < 0)
        if (errno != EINTR)
            throwFromErrno("io_getevents: Failed to wait for asynchronous IO completion", ErrorCodes::CANNOT_IO_GETEVENTS, errno);

    return num_events;
}


void AIOContextPool::fulfillPromises(const io_event events[], const int num_events)
{
    if (num_events == 0)
        return;

    const std::lock_guard<std::mutex> lock{mutex};

    /// look at returned events and find corresponding promise, set result and erase promise from map
    for (const auto & event : boost::make_iterator_range(events, events + num_events))
    {
        /// get id from event
        const auto completed_id = event.data;

        /// set value via promise and release it
        const auto it = promises.find(completed_id);
        if (it == std::end(promises))
        {
            LOG_ERROR(&Poco::Logger::get("AIOcontextPool"), "Found io_event with unknown id " << completed_id);
            continue;
        }

        it->second.set_value(event.res);
        promises.erase(it);
    }
}


void AIOContextPool::notifyProducers(const int num_producers) const
{
    if (num_producers == 0)
        return;

    if (num_producers > 1)
        have_resources.notify_all();
    else
        have_resources.notify_one();
}


void AIOContextPool::reportExceptionToAnyProducer()
{
    const std::lock_guard<std::mutex> lock{mutex};

    const auto any_promise_it = std::begin(promises);
    any_promise_it->second.set_exception(std::current_exception());
}


std::future<AIOContextPool::BytesRead> AIOContextPool::post(struct iocb & iocb)
{
    std::unique_lock<std::mutex> lock{mutex};

    /// get current id and increment it by one
    const auto request_id = next_id;
    ++next_id;

    /// create a promise and put request in "queue"
    promises.emplace(request_id, std::promise<BytesRead>{});
    /// store id in AIO request for further identification
    iocb.aio_data = request_id;

    auto num_requests = 0;
    struct iocb * requests[] { &iocb };

    /// submit a request
    while ((num_requests = io_submit(aio_context.ctx, 1, requests)) < 0)
    {
        if (errno == EAGAIN)
            /// wait until at least one event has been completed (or a spurious wakeup) and try again
            have_resources.wait(lock);
        else if (errno != EINTR)
            throwFromErrno("io_submit: Failed to submit a request for asynchronous IO", ErrorCodes::CANNOT_IO_SUBMIT);
    }

    return promises[request_id].get_future();
}

}

#endif
