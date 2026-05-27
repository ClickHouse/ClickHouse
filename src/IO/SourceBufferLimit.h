#pragma once

#include <base/types.h>
#include <Common/Logger.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>

#include <chrono>
#include <mutex>
#include <optional>
#include <vector>

namespace DB
{

class SourceBufferLimit;

/// Observability: one entry per active live buffer. The slot stores a
/// stable pointer to its registry entry so `SourceBufferSlot::objectPath`
/// can be served lock-free.
///
/// Carries both the remote `object_path` (S3 / Azure / local blob name)
/// and the `local_path` (the data part file the read maps to), so
/// `system.remote_read_connections` can show "table-relative" identity
/// without the consumer having to join against any external table.
struct ActiveBufferInfo
{
    String object_path;
    String local_path;
    String query_id;
    size_t position = 0;
    std::chrono::steady_clock::time_point acquired_time;
};

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

    /// The `object_path` this slot was acquired for. Read directly from the
    /// registry entry — `ActiveBufferInfo::object_path` is the single source
    /// of truth, immutable for the slot's lifetime, and `unordered_map`
    /// guarantees pointer stability to mapped values across insert / rehash
    /// (only the erased element's pointer is invalidated, and the slot's
    /// destructor is what triggers the erase). No mutex needed.
    const String & objectPath() const { return info->object_path; }

private:
    friend class SourceBufferLimit;
    SourceBufferSlot(std::shared_ptr<SourceBufferLimit> limit, size_t slot_id, const ActiveBufferInfo * info);

    std::shared_ptr<SourceBufferLimit> limit;
    size_t slot_id = 0;
    /// Pointer (not iterator) into the limit's `registry`, which is
    /// `std::unordered_map<size_t, ActiveBufferInfo>`. The container
    /// guarantees pointers to mapped values survive rehash — nodes are
    /// individually heap-allocated and never moved, only the bucket array
    /// is replaced. Iterators do not have this guarantee: rehash may
    /// invalidate them. The pointer is set under the limit's mutex at
    /// `tryAcquire` and used unlocked from any thread while the slot is
    /// alive; the registry entry is erased only by the slot's destructor.
    const ActiveBufferInfo * info = nullptr;
};

/// Global limit on the number of live source buffers (open connections).
/// Thread-safe. Created once in Context.
class SourceBufferLimit
{
public:
    explicit SourceBufferLimit(size_t max_slots);

    /// Try to acquire a slot. Returns nullopt if at capacity.
    /// self must be the shared_ptr owning this instance (kept alive by the slot).
    std::optional<SourceBufferSlot> tryAcquire(
        std::shared_ptr<SourceBufferLimit> self,
        const String & object_path,
        const String & local_path = {},
        const String & query_id = {});

    /// Update capacity at runtime (called from config reload in Server.cpp).
    /// Existing slots beyond the new limit are not forcibly closed.
    void setCapacity(size_t new_max_slots);
    size_t getCapacity() const;

    /// Observability: snapshot of all active slots.
    VectorWithMemoryTracking<ActiveBufferInfo> getActive() const;

private:
    friend class SourceBufferSlot;
    void release(size_t slot_id);
    void updatePosition(size_t slot_id, size_t new_position);

    size_t max_slots;

    mutable std::mutex mutex;
    size_t next_id = 0;
    UnorderedMapWithMemoryTracking<size_t, ActiveBufferInfo> registry;

    LoggerPtr log = getLogger("SourceBufferLimit");
};

}
