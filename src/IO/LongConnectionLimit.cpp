#include <IO/LongConnectionLimit.h>
#include <Common/CurrentMetrics.h>

namespace CurrentMetrics
{
    extern const Metric ReaderExecutorLongConnections;
}

namespace DB
{

LongConnectionSlot::~LongConnectionSlot()
{
    release();
}

LongConnectionSlot::LongConnectionSlot(LongConnectionSlot && other) noexcept
    : limit(std::move(other.limit))
    , held(other.held)
{
    other.held = false;
}

LongConnectionSlot & LongConnectionSlot::operator=(LongConnectionSlot && other) noexcept
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

LongConnectionSlot::LongConnectionSlot(std::shared_ptr<LongConnectionLimit> limit_) noexcept
    : limit(std::move(limit_))
    , held(true)
{
    /// Account the held slot here so add/sub stay symmetric with `release`.
    CurrentMetrics::add(CurrentMetrics::ReaderExecutorLongConnections);
}

void LongConnectionSlot::release()
{
    if (held && limit)
    {
        limit->release();
        CurrentMetrics::sub(CurrentMetrics::ReaderExecutorLongConnections);
    }
    held = false;
}


LongConnectionLimit::LongConnectionLimit(size_t max_slots_)
    : max_slots(max_slots_)
{
}

LongConnectionSlot LongConnectionLimit::tryAcquire(std::shared_ptr<LongConnectionLimit> self)
{
    /// Claim a unit iff under capacity. A `setCapacity` lowering below the
    /// live count is soft - new acquirers just wait it out.
    size_t cur = count.load(std::memory_order_relaxed);
    while (cur < max_slots.load(std::memory_order_relaxed))
    {
        if (count.compare_exchange_weak(cur, cur + 1, std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            return LongConnectionSlot(std::move(self));
        }
    }
    return {};
}

void LongConnectionLimit::setCapacity(size_t new_max_slots)
{
    max_slots.store(new_max_slots, std::memory_order_relaxed);
}

void LongConnectionLimit::release()
{
    count.fetch_sub(1, std::memory_order_acq_rel);
}

}
