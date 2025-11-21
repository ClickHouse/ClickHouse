#pragma once

#include <Common/Scheduler/ISchedulerNode.h>

#include <base/types.h>

#include <boost/noncopyable.hpp>

#include <atomic>
#include <chrono>
#include <deque>
#include <functional>
#include <mutex>
#include <condition_variable>


namespace DB
{

class ISchedulerNode;

/// Simple waitable thread-safe FIFO task queue.
/// Intended to hold postponed events for later handling by scheduler thread.
class EventQueue : public boost::noncopyable
{
public:
    using Task = std::function<void()>;

    static constexpr EventId not_postponed = 0;

    using TimePoint = std::chrono::system_clock::time_point;
    using Duration = std::chrono::system_clock::duration;

    struct Event
    {
        const EventId event_id;
        Task task;

        Event(EventId event_id_, Task && task_)
            : event_id(event_id_)
            , task(std::move(task_))
        {}
    };

    struct Postponed
    {
        TimePoint key;
        EventId event_id; // for canceling
        std::unique_ptr<Task> task;

        Postponed(TimePoint key_, EventId event_id_, Task && task_)
            : key(key_)
            , event_id(event_id_)
            , task(std::make_unique<Task>(std::move(task_)))
        {}

        bool operator<(const Postponed & rhs) const
        {
            return std::tie(key, event_id) > std::tie(rhs.key, rhs.event_id); // reversed for min-heap
        }
    };

    /// Add an `event` to be processed after `until` time point.
    /// Returns a unique event id for canceling.
    [[nodiscard]] EventId postpone(TimePoint until, Task && task);

    /// Cancel a postponed event using its unique id.
    /// NOTE: Only postponed events can be canceled.
    /// NOTE: If you need to cancel enqueued event, consider doing your actions inside another enqueued
    /// NOTE: event instead. This ensures that all previous events are processed.
    bool cancelPostponed(EventId postponed_event_id);

    /// Add an `event` for immediate processing
    void enqueue(Task && task);

    /// Add an activation `event` for immediate processing. Activations use a separate queue for performance reasons.
    void enqueueActivation(ISchedulerNode & node);

    /// Removes an activation from queue
    void cancelActivation(ISchedulerNode & node);

    /// Process single event if it exists
    /// Note that postponing constraint are ignored, use it to empty the queue including postponed events on shutdown
    /// Returns `true` iff event has been processed
    bool forceProcess();

    /// Process single event if it exists and meets postponing constraint
    /// Returns `true` iff event has been processed
    bool tryProcess();

    /// Wait for single event (if not available) and process it
    void process();

    /// Returns current time point
    TimePoint now();

    /// For testing only
    void setManualTime(TimePoint value);

    /// For testing only
    void advanceManualTime(Duration elapsed);

private:
    void wait(std::unique_lock<std::mutex> & lock);
    void waitUntil(std::unique_lock<std::mutex> & lock, TimePoint t);
    void processQueue(std::unique_lock<std::mutex> && lock);
    void processActivation(std::unique_lock<std::mutex> && lock);
    void processEvent(std::unique_lock<std::mutex> && lock);
    void processPostponed(std::unique_lock<std::mutex> && lock);

    std::mutex mutex;
    std::condition_variable pending;

    // `events` and `activations` logically represent one ordered queue. To preserve the common order we use `EventId`
    // Activations are stored in a separate queue for performance reasons (mostly to avoid any allocations)
    std::deque<Event> events;
    ISchedulerNode::ActivationList activations;

    std::vector<Postponed> postponed;
    EventId last_event_id = 0;

    std::atomic<TimePoint> manual_time{TimePoint()}; // for tests only
};

}
