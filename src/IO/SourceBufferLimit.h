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
    /// Releases the currently-held slot (if any) BEFORE taking ownership of
    /// `other`'s slot. The defaulted move-assignment that this replaces just
    /// overwrote `limit` / `slot_id`, leaking the previous slot's registry
    /// entry and permanently consuming one unit of capacity.
    SourceBufferSlot & operator=(SourceBufferSlot && other) noexcept;

    /// Update the position tracked in the registry (for observability).
    void updatePosition(size_t new_position);

private:
    friend class SourceBufferLimit;
    SourceBufferSlot(std::shared_ptr<SourceBufferLimit> limit, size_t slot_id);

    std::shared_ptr<SourceBufferLimit> limit;
    size_t slot_id = 0;
};

/// Observability: one entry per active live buffer.
struct ActiveBufferInfo
{
    String object_path;
    String query_id;
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
    /// self must be the shared_ptr owning this instance (kept alive by the slot).
    std::optional<SourceBufferSlot> tryAcquire(std::shared_ptr<SourceBufferLimit> self, const String & object_path, const String & query_id = {});

    /// Update capacity at runtime (called from config reload in Server.cpp).
    /// Existing slots beyond the new limit are not forcibly closed.
    void setCapacity(size_t new_max_slots);
    size_t getCapacity() const;

    /// Observability: snapshot of all active slots.
    std::vector<ActiveBufferInfo> getActive() const;

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
