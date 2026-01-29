#include <Common/Scheduler/EventQueue.h>

#include <algorithm>


namespace DB
{

EventId EventQueue::postpone(TimePoint until, Task && task)
{
    std::unique_lock lock{mutex};
    if (postponed.empty() || until < postponed.front().key)
        pending.notify_one();
    auto event_id = ++last_event_id;
    postponed.emplace_back(until, event_id, std::move(task));
    std::push_heap(postponed.begin(), postponed.end());
    return event_id;
}

bool EventQueue::cancelPostponed(EventId postponed_event_id)
{
    if (postponed_event_id == not_postponed)
        return false;
    std::unique_lock lock{mutex};
    for (auto i = postponed.begin(), e = postponed.end(); i != e; ++i)
    {
        if (i->event_id == postponed_event_id)
        {
            postponed.erase(i);
            // It is O(n), but we do not expect neither big heaps nor frequent cancels. So it is fine.
            std::make_heap(postponed.begin(), postponed.end());
            return true;
        }
    }
    return false;
}

void EventQueue::enqueue(Task && task)
{
    std::unique_lock lock{mutex};
    bool was_empty = events.empty() && activations.empty();
    auto event_id = ++last_event_id;
    events.emplace_back(event_id, std::move(task));
    if (was_empty)
        pending.notify_one();
}

void EventQueue::enqueueActivation(ISchedulerNode & node)
{
    std::unique_lock lock{mutex};
    if (node.activation_event_id)
        return; // already scheduled
    bool was_empty = events.empty() && activations.empty();
    node.activation_event_id = ++last_event_id;
    activations.push_back(node);
    if (was_empty)
        pending.notify_one();
}

void EventQueue::cancelActivation(ISchedulerNode & node)
{
    std::unique_lock lock{mutex};
    if (node.activation_hook.is_linked())
        activations.erase(activations.iterator_to(node));
    node.activation_event_id = 0;
}

bool EventQueue::forceProcess()
{
    std::unique_lock lock{mutex};
    if (!events.empty() || !activations.empty())
    {
        processQueue(std::move(lock));
        return true;
    }
    if (!postponed.empty())
    {
        processPostponed(std::move(lock));
        return true;
    }
    return false;
}

bool EventQueue::tryProcess()
{
    std::unique_lock lock{mutex};
    if (!events.empty() || !activations.empty())
    {
        processQueue(std::move(lock));
        return true;
    }
    if (postponed.empty())
        return false;

    if (postponed.front().key <= now())
    {
        processPostponed(std::move(lock));
        return true;
    }
    return false;
}

void EventQueue::process()
{
    std::unique_lock lock{mutex};
    while (true)
    {
        if (!events.empty() || !activations.empty())
        {
            processQueue(std::move(lock));
            return;
        }
        if (postponed.empty())
        {
            wait(lock);
        }
        else
        {
            if (postponed.front().key <= now())
            {
                processPostponed(std::move(lock));
                return;
            }

            waitUntil(lock, postponed.front().key);
        }
    }
}

EventQueue::TimePoint EventQueue::now()
{
    auto result = manual_time.load();
    if (likely(result == TimePoint()))
        return std::chrono::system_clock::now();
    return result;
}

void EventQueue::setManualTime(TimePoint value)
{
    std::unique_lock lock{mutex};
    manual_time.store(value);
    pending.notify_one();
}

void EventQueue::advanceManualTime(Duration elapsed)
{
    std::unique_lock lock{mutex};
    manual_time.store(manual_time.load() + elapsed);
    pending.notify_one();
}

void EventQueue::wait(std::unique_lock<std::mutex> & lock)
{
    pending.wait(lock);
}

void EventQueue::waitUntil(std::unique_lock<std::mutex> & lock, TimePoint t)
{
    if (likely(manual_time.load() == TimePoint()))
        pending.wait_until(lock, t);
    else
        pending.wait(lock);
}

void EventQueue::processQueue(std::unique_lock<std::mutex> && lock)
{
    if (events.empty())
    {
        processActivation(std::move(lock));
        return;
    }
    if (activations.empty())
    {
        processEvent(std::move(lock));
        return;
    }
    if (activations.front().activation_event_id < events.front().event_id)
        processActivation(std::move(lock));
    else
        processEvent(std::move(lock));
}

void EventQueue::processActivation(std::unique_lock<std::mutex> && lock)
{
    ISchedulerNode & node = activations.front();
    activations.pop_front();
    node.activation_event_id = 0;
    lock.unlock(); // do not hold queue mutex while processing events
    node.processActivation();
}

void EventQueue::processEvent(std::unique_lock<std::mutex> && lock)
{
    Task task = std::move(events.front().task);
    events.pop_front();
    lock.unlock(); // do not hold queue mutex while processing events
    task();
}

void EventQueue::processPostponed(std::unique_lock<std::mutex> && lock)
{
    Task task = std::move(*postponed.front().task);
    std::pop_heap(postponed.begin(), postponed.end());
    postponed.pop_back();
    lock.unlock(); // do not hold queue mutex while processing events
    task();
}

}
