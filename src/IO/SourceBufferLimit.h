#pragma once

#include <base/types.h>
#include <Common/Logger.h>

#include <chrono>
#include <mutex>
#include <optional>
#include <vector>

namespace DB
{

class SourceBufferLimit;

/// RAII slot — releasing it returns capacity to the global limit
/// and removes the entry from the active registry.
class SourceBufferSlot
{
public:
    ~SourceBufferSlot();

    SourceBufferSlot(const SourceBufferSlot &) = delete;
    SourceBufferSlot & operator=(const SourceBufferSlot &) = delete;
    SourceBufferSlot(SourceBufferSlot && other) noexcept;
    SourceBufferSlot & operator=(SourceBufferSlot && other) noexcept;

    /// Update the position tracked in the registry (for observability).
    void updatePosition(size_t new_position);

private:
    friend class SourceBufferLimit;
    SourceBufferSlot(SourceBufferLimit * limit, size_t slot_id);

    SourceBufferLimit * limit = nullptr;
    size_t slot_id = 0;
};

/// Observability: one entry per active live buffer.
struct ActiveBufferInfo
{
    String object_path;
    size_t position = 0;
    std::chrono::steady_clock::time_point acquired_time;
};

/// Global limit on the number of live source buffers (open connections).
/// Thread-safe. Created once in Context.
class SourceBufferLimit
{
public:
    explicit SourceBufferLimit(size_t max_slots);

    /// Try to acquire a slot. Returns nullopt if at capacity.
    std::optional<SourceBufferSlot> tryAcquire(const String & object_path);

    /// Observability: snapshot of all active slots.
    std::vector<ActiveBufferInfo> getActive() const;

    size_t getCapacity() const;

private:
    friend class SourceBufferSlot;
    void release(size_t slot_id);
    void updatePosition(size_t slot_id, size_t new_position);

    size_t max_slots;

    mutable std::mutex mutex;
    size_t next_id = 0;
    std::unordered_map<size_t, ActiveBufferInfo> registry;

    LoggerPtr log = getLogger("SourceBufferLimit");
};

}
