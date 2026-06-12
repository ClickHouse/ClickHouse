#include <IO/LiveConnectionLimit.h>
#include <Common/CurrentMetrics.h>

namespace CurrentMetrics
{
    extern const Metric LiveSourceBuffers;
}

namespace DB
{

LiveConnectionSlot::~LiveConnectionSlot()
{
    release();
}

LiveConnectionSlot::LiveConnectionSlot(LiveConnectionSlot && other) noexcept
    : limit(std::move(other.limit))
    , held(other.held)
{
    other.held = false;
}

LiveConnectionSlot & LiveConnectionSlot::operator=(LiveConnectionSlot && other) noexcept
{
    if (this != &other)
    {
        release();
        limit = std::move(other.limit);
        held = other.held;
        other.held = false;
    }
    return *this;
}

LiveConnectionSlot::LiveConnectionSlot(std::shared_ptr<LiveConnectionLimit> limit_)
    : limit(std::move(limit_))
    , held(true)
{
}

void LiveConnectionSlot::release()
{
    if (held && limit)
    {
        limit->release();
        CurrentMetrics::sub(CurrentMetrics::LiveSourceBuffers);
    }
    held = false;
}


LiveConnectionLimit::LiveConnectionLimit(size_t max_slots_)
    : max_slots(max_slots_)
{
}

LiveConnectionSlot LiveConnectionLimit::tryAcquire(std::shared_ptr<LiveConnectionLimit> self)
{
    /// Claim a unit iff under capacity. A `setCapacity` lowering below the
    /// live count is soft - new acquirers just wait it out.
    size_t cur = count.load(std::memory_order_relaxed);
    while (cur < max_slots.load(std::memory_order_relaxed))
    {
        if (count.compare_exchange_weak(cur, cur + 1, std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            CurrentMetrics::add(CurrentMetrics::LiveSourceBuffers);
            return LiveConnectionSlot(std::move(self));
        }
    }
    return {};
}

void LiveConnectionLimit::setCapacity(size_t new_max_slots)
{
    max_slots.store(new_max_slots, std::memory_order_relaxed);
}

void LiveConnectionLimit::release()
{
    count.fetch_sub(1, std::memory_order_acq_rel);
}

}
