#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasEventDispatcher.h>

#include <Common/Exception.h>

#include <utility>

namespace DB::Cas
{

void EventDispatcher::setSink(Sink sink_)
{
    std::lock_guard<std::mutex> lock(mutex);
    sink = std::move(sink_);
    has_sink.store(static_cast<bool>(sink), std::memory_order_release);
}

void EventDispatcher::emit(CasEvent event)
{
    std::unique_lock<std::mutex> lock(mutex);
    /// Enqueue before deciding who drains. A `bad_alloc` here escapes, but `draining` is untouched and
    /// the deque's strong guarantee leaves the queue intact -- the dispatcher is never left wedged.
    queue.push_back(std::move(event));

    /// A drain loop already owns delivery (this call is reentrant from inside the sink, or a concurrent
    /// emitter is draining). Enqueue-and-return: the running loop will deliver what we just pushed.
    if (draining)
        return;

    draining = true;
    while (!queue.empty())
    {
        CasEvent next = std::move(queue.front());
        queue.pop_front();
        /// The sink runs OUTSIDE `mutex`: a reentrant `emit` can take the lock, and a concurrent
        /// emitter can enqueue, neither blocked on this delivery.
        lock.unlock();
        try
        {
            /// `sink` is set pre-traffic and never swapped concurrently with delivery, so reading it
            /// here without `mutex` is race-free.
            if (sink)
                sink(std::move(next));
        }
        catch (...)
        {
            /// Contain the sink failure: dropping one audit event must not abandon the queued
            /// remainder nor leave `draining` stuck true (which would silently mute all future events).
            DB::tryLogCurrentException("CasEventDispatcher",
                "Content-addressed audit event sink threw; the event was dropped");
        }
        lock.lock();
    }
    draining = false;
}

}
