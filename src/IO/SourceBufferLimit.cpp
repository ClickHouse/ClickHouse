#include <IO/SourceBufferLimit.h>
#include <Common/logger_useful.h>

namespace DB
{

SourceBufferSlot::SourceBufferSlot(SourceBufferLimit * limit_, size_t slot_id_)
    : limit(limit_)
    , slot_id(slot_id_)
{
}

SourceBufferSlot::~SourceBufferSlot()
{
    if (limit)
        limit->release(slot_id);
}

SourceBufferSlot::SourceBufferSlot(SourceBufferSlot && other) noexcept
    : limit(other.limit)
    , slot_id(other.slot_id)
{
    other.limit = nullptr;
}

SourceBufferSlot & SourceBufferSlot::operator=(SourceBufferSlot && other) noexcept
{
    if (this != &other)
    {
        if (limit)
            limit->release(slot_id);
        limit = other.limit;
        slot_id = other.slot_id;
        other.limit = nullptr;
    }
    return *this;
}

void SourceBufferSlot::updatePosition(size_t new_position)
{
    if (limit)
        limit->updatePosition(slot_id, new_position);
}


SourceBufferLimit::SourceBufferLimit(size_t max_slots_)
    : max_slots(max_slots_)
{
}

std::optional<SourceBufferSlot> SourceBufferLimit::tryAcquire(const String & object_path)
{
    /// Optimistic try-increment.
    size_t current = used_slots.load(std::memory_order_relaxed);
    while (true)
    {
        if (current >= max_slots)
        {
            LOG_TRACE(log, "tryAcquire: at capacity ({}/{}), falling back to stateless read for {}",
                current, max_slots, object_path);
            return std::nullopt;
        }
        if (used_slots.compare_exchange_weak(current, current + 1, std::memory_order_acq_rel))
            break;
    }

    size_t id;
    {
        std::lock_guard lock(registry_mutex);
        id = next_slot_id++;
        active_registry[id] = ActiveBufferInfo{
            .object_path = object_path,
            .position = 0,
            .acquired_time = std::chrono::steady_clock::now()};
    }

    LOG_TRACE(log, "tryAcquire: got slot {} for {} ({}/{})", id, object_path, used_slots.load(), max_slots);
    return SourceBufferSlot(this, id);
}

void SourceBufferLimit::release(size_t slot_id)
{
    {
        std::lock_guard lock(registry_mutex);
        active_registry.erase(slot_id);
    }
    used_slots.fetch_sub(1, std::memory_order_acq_rel);
    LOG_TRACE(log, "release: slot {} freed ({}/{})", slot_id, used_slots.load(), max_slots);
}

void SourceBufferLimit::updatePosition(size_t slot_id, size_t new_position)
{
    std::lock_guard lock(registry_mutex);
    if (auto it = active_registry.find(slot_id); it != active_registry.end())
        it->second.position = new_position;
}

std::vector<ActiveBufferInfo> SourceBufferLimit::getActive() const
{
    std::lock_guard lock(registry_mutex);
    std::vector<ActiveBufferInfo> result;
    result.reserve(active_registry.size());
    for (const auto & [_, info] : active_registry)
        result.push_back(info);
    return result;
}

}
