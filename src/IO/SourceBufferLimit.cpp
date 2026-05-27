#include <IO/SourceBufferLimit.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/VectorWithMemoryTracking.h>

namespace CurrentMetrics
{
    extern const Metric LiveSourceBuffers;
}

namespace DB
{

SourceBufferSlot::SourceBufferSlot(std::shared_ptr<SourceBufferLimit> limit_, size_t slot_id_, const ActiveBufferInfo * info_)
    : limit(std::move(limit_))
    , slot_id(slot_id_)
    , info(info_)
{
}

SourceBufferSlot::SourceBufferSlot(SourceBufferSlot && other) noexcept
    : limit(std::move(other.limit))
    , slot_id(other.slot_id)
    , info(other.info)
{
    other.slot_id = 0;
    other.info = nullptr;
}

SourceBufferSlot & SourceBufferSlot::operator=(SourceBufferSlot && other) noexcept
{
    if (this != &other)
    {
        if (limit)
            limit->release(slot_id);
        limit = std::move(other.limit);
        slot_id = other.slot_id;
        info = other.info;
        other.slot_id = 0;
        other.info = nullptr;
    }
    return *this;
}

SourceBufferSlot::~SourceBufferSlot()
{
    if (limit)
        limit->release(slot_id);
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

void SourceBufferLimit::setCapacity(size_t new_max_slots)
{
    std::lock_guard lock(mutex);
    LOG_DEBUG(log, "setCapacity: {} -> {}", max_slots, new_max_slots);
    max_slots = new_max_slots;
}

size_t SourceBufferLimit::getCapacity() const
{
    std::lock_guard lock(mutex);
    return max_slots;
}

std::optional<SourceBufferSlot> SourceBufferLimit::tryAcquire(
    std::shared_ptr<SourceBufferLimit> self,
    const String & object_path,
    const String & local_path,
    const String & query_id)
{
    std::lock_guard lock(mutex);

    if (registry.size() >= max_slots)
    {
        LOG_TRACE(log, "tryAcquire: at capacity ({}/{}), falling back to stateless read for {}",
            registry.size(), max_slots, object_path);
        return std::nullopt;
    }

    size_t id = next_id++;
    auto [it, inserted] = registry.emplace(id, ActiveBufferInfo{
        .object_path = object_path,
        .local_path = local_path,
        .query_id = query_id,
        .position = 0,
        .acquired_time = std::chrono::steady_clock::now()});
    chassert(inserted);

    CurrentMetrics::add(CurrentMetrics::LiveSourceBuffers);
    LOG_TRACE(log, "tryAcquire: got slot {} for {} ({}/{})", id, object_path, registry.size(), max_slots);
    /// `&it->second` is a stable pointer for the registry entry's lifetime
    /// (unordered_map guarantees pointer stability across rehash; the entry
    /// is erased only by `release`, which is called from this slot's dtor).
    return SourceBufferSlot(std::move(self), id, &it->second);
}

void SourceBufferLimit::release(size_t slot_id)
{
    std::lock_guard lock(mutex);
    registry.erase(slot_id);
    CurrentMetrics::sub(CurrentMetrics::LiveSourceBuffers);
    LOG_TRACE(log, "release: slot {} freed ({}/{})", slot_id, registry.size(), max_slots);
}

void SourceBufferLimit::updatePosition(size_t slot_id, size_t new_position)
{
    std::lock_guard lock(mutex);
    if (auto it = registry.find(slot_id); it != registry.end())
        it->second.position = new_position;
}

VectorWithMemoryTracking<ActiveBufferInfo> SourceBufferLimit::getActive() const
{
    std::lock_guard lock(mutex);
    VectorWithMemoryTracking<ActiveBufferInfo> result;
    result.reserve(registry.size());
    for (const auto & [_, info] : registry)
        result.push_back(info);
    return result;
}

}
