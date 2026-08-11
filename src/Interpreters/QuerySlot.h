#pragma once

#include <base/types.h>
#include <boost/core/noncopyable.hpp>

#include <Common/Scheduler/IResourceManager.h>
#include <Common/Scheduler/ResourceLink.h>
#include <Common/Scheduler/ResourceRequest.h>
#include <Common/CurrentMetrics.h>

#include <chrono>
#include <condition_variable>
#include <exception>
#include <functional>
#include <mutex>
#include <memory>
#include <optional>
#include <utility>


namespace DB
{

// Represents a slot for a query execution. Every query that participates in workload scheduling should request one from
// the resource scheduler before query execution and hold it until query is finished.
// Specified link should point to a queue of some workload within the resource created with:
//   CREATE RESOURCE query (QUERY)
class QuerySlot final: private ResourceRequest, public boost::noncopyable
{
public:
    /// Blocks until a query slot is acquired.
    explicit QuerySlot(ResourceLink link_);

    /// Enqueues a query slot request without blocking. Calls on_ready from the resource scheduler
    /// thread after the request is granted or failed. The callback should only wake the consumer;
    /// any exception it throws is captured for wait() and never escapes the scheduler thread.
    QuerySlot(ResourceLink link_, ClassifierPtr classifier_, std::function<void()> on_ready_);

    ~QuerySlot() override;

    /// Waits until an asynchronously requested slot is granted and reports scheduler failures.
    /// Returns immediately for a slot that has already been granted.
    void wait();

    /// Cancels a request that is still waiting in the scheduler queue.
    /// Returns false if it was already granted or failed.
    bool cancel();

private:
    enum class State
    {
        Enqueued,
        Granted,
        Failed,
        Cancelled,
    };

    void enqueue();

    /// Callback to trigger resource consumption.
    void execute() override;

    /// Callback to trigger an error in case if resource is unavailable.
    void failed(const std::exception_ptr & ptr) override;

    void complete(State new_state, const std::exception_ptr & ptr = {});

    ResourceLink link;
    /// Keeps the scheduler nodes referenced by link alive while an asynchronous request is pending.
    ClassifierPtr classifier;
    std::function<void()> on_ready;

    std::mutex mutex;
    std::condition_variable cv;
    State state = State::Enqueued;
    std::exception_ptr exception;
    bool callback_running = false;
    bool wait_accounted = false;
    std::chrono::steady_clock::time_point enqueue_time;
    std::optional<CurrentMetrics::Increment> scheduled_increment;
    std::optional<CurrentMetrics::Increment> acquired_slot_increment;
};

using QuerySlotPtr = std::unique_ptr<QuerySlot>;

}
