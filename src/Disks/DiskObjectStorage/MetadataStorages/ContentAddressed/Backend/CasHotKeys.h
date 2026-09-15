#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRetry.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasWriteResult.h>
#include <Common/CacheBase.h>
#include <base/types.h>

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>

namespace DB::Cas
{

class CasOperation;

/// One conditional write in flight per key from the operations that write through here, in arrival
/// order, plus the last object this pool knows per key so the next write needs no read.
///
/// Compare-and-swap is needed only against other servers; inside one server every writer of a shared
/// key used to race every other, and each lost race cost a read, a refused write, a resolve read and a
/// growing sleep. `submit` takes a FIFO ticket for the key, waits its turn re-checking the caller's
/// own fence, lease and deadline, obtains a base (the cache's object, else the engine's own read),
/// runs the caller's `decide` on it, lands the candidate through the engine's `replace` or `create`
/// on the caller's own operation and thread, and returns the engine's result unchanged. The caller
/// keeps the retry loop: a `Conflict` is a lost race against another server, and the caller submits
/// again after `Retry::conflictBackoff`.
///
/// The cache is a hint and never a source of truth: every write against it is conditional on its
/// etag, so a stale entry costs one 412 and one resolve read; and a verdict a `decide` renders on it
/// (a refusal by exception, or "nothing to write") is never delivered, because a refusal without a
/// write is the one thing a 412 cannot correct -- the entry is dropped, the key is read, and the
/// `decide` runs again on the read. The store's answer to a write decided on a hint is delivered
/// whatever it is: it is a fact about the store, and the caller's ordinary retry learns the rest.
///
/// One instance per pool, shared by its three request planes; a `CasRequests` built without one owns
/// a private instance with no cache. Every callback (`decide`, a `Liveness` closure, a backend hook)
/// runs with `mutex` released: the mutex is a leaf that calls nothing.
class CasHotKeys
{
public:
    /// `cache_budget_bytes` bounds the remembered objects; 0 disables the cache, and every hold reads.
    explicit CasHotKeys(uint64_t cache_budget_bytes);
    ~CasHotKeys();
    CasHotKeys(const CasHotKeys &) = delete;
    CasHotKeys & operator=(const CasHotKeys &) = delete;

    /// The caller's mutation of `key`, the engine's own decide shape: the candidate bytes to write over
    /// `base`, nothing (`Declined`), or a refusal by exception. `base` is absent when the key does not
    /// exist; a caller that refuses to bootstrap throws there. A `decide` may be run twice, and a
    /// later run on a fresh read is the decision that counts; it issues no write through this lane;
    /// its reads go through `op`; it reads `base->bytes` and never `base->etag`.
    using Decide = std::function<std::optional<String>(const std::optional<Object> &)>;

    /// One hold on `key`: wait for the turn, obtain a base, run `decide`, one engine write, remember.
    /// `policy` is frozen at entry, so time in the queue spends the caller's window. Returns the
    /// engine's own result for that write, in class and content, and propagates a `decide`'s
    /// exception as `readModifyWrite` does, never from a cached base.
    WriteResult submit(const String & key, CasOperation & op, const Retry & policy, const Decide & decide);

    /// Items in the key's queue, the holder included; 0 for a key with no lane.
    size_t queueDepthForTest(const String & key) const;
    /// Lanes in existence: a lane lives while its queue holds an item.
    size_t laneCountForTest() const;
    /// Entries the cache holds.
    size_t cacheEntriesForTest() const;

    /// TEST SEAM: runs under `mutex` after the key's lane was found or created and before the item
    /// is queued; a throw here is the enqueue's allocation failing.
    std::function<void()> enter_after_lane_hook_for_test;
    /// TEST SEAM: runs inside `remember` before the cache is filled; a throw here is the fill failing.
    std::function<void()> cache_fill_hook_for_test;

private:
    /// One queued submission, on its caller's stack: the caller's own guard is the only thing that
    /// removes it, so nothing here outlives the stack it lives on.
    struct Item
    {
        uint64_t ticket;
    };
    /// One key. Created on first use, erased when its queue empties; referenced only by threads whose
    /// item is in its queue.
    struct Lane
    {
        std::deque<Item *> queue;                    /// guarded by `mutex`; the front is the holder
        std::optional<uint64_t> holder_since_ms;     /// guarded by `mutex`; for the log line
        std::condition_variable cv;
    };
    /// A remembered object together with its precomputed weight: `Etag::render` allocates and can
    /// throw, and the cache's own accounting calls the weight after it has already subtracted the
    /// entry being replaced, so the weight itself must never throw.
    struct Remembered
    {
        Object object;
        size_t weight;
    };
    struct RememberedWeight
    {
        size_t operator()(const Remembered & remembered) const noexcept { return remembered.weight; }
    };
    using Cache = CacheBase<String, Remembered, std::hash<String>, RememberedWeight>;

    WriteResult hold(const String & key, CasOperation & op, const Retry & policy, const Retry::Bound & bound,
                     const Decide & decide);
    void leave(const String & key, Item & item, bool entered_hold, uint64_t entered_ms, CasOperation & op) noexcept;

    std::optional<Object> cached(const String & key) const;
    void remember(const String & key, Object object) noexcept;
    void forget(const String & key) noexcept;

    mutable std::mutex mutex;                        /// `mutable` for the const test seams
    uint64_t next_ticket = 0;                        /// guarded by `mutex`; the holder's identity in the log line
    std::unordered_map<String, Lane> lanes;          /// guarded by `mutex`
    const uint64_t cache_budget_bytes;
    /// The last known object per key. Its synchronization is its own; it is read and written with
    /// `mutex` released. Null when the budget is 0.
    std::unique_ptr<Cache> cache;
};

}
