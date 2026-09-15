#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasHotKeys.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <base/defines.h>

#include <algorithm>
#include <chrono>

namespace ProfileEvents
{
    extern const Event CASHotKeyQueueWaitMicroseconds;
    extern const Event CASHotKeyCacheStarts;
    extern const Event CASHotKeyReadStarts;
    extern const Event CASHotKeyCacheVerdictsReread;
}

namespace CurrentMetrics
{
    extern const Metric CASHotKeyCacheBytes;
    extern const Metric CASHotKeyCacheEntries;
}

namespace DB::Cas
{

namespace
{

GaveUp::Source sourceFor(const Retry::Bound & bound)
{
    return bound.lease_bound ? GaveUp::Source::Lease : GaveUp::Source::Policy;
}

/// The wait slice: how late, at most, a waiter notices its own fence or deadline. A handover wakes it
/// at once through the lane's condition variable.
constexpr auto kWaitSlice = std::chrono::milliseconds(200);

}

CasHotKeys::CasHotKeys(uint64_t cache_budget_bytes_)
    : cache_budget_bytes(cache_budget_bytes_)
    , cache(cache_budget_bytes_ == 0
            ? nullptr
            /// No count cap: the weight above is never zero, so the byte budget alone already bounds
            /// how many entries the cache can hold. `size_ratio` is unused by the LRU policy.
            : std::make_unique<Cache>("LRU", CurrentMetrics::CASHotKeyCacheBytes, CurrentMetrics::CASHotKeyCacheEntries,
                                      cache_budget_bytes_, Cache::NO_MAX_COUNT, Cache::DEFAULT_SIZE_RATIO))
{
}

CasHotKeys::~CasHotKeys()
{
    /// A lane surviving the pool that owns it means some caller's stack still points into it: a
    /// lifetime error, not a state this destructor could ever see in a correct program.
    chassert(lanes.empty());
}

WriteResult CasHotKeys::submit(const String & key, CasOperation & op, const Retry & policy, const Decide & decide)
{
    /// Frozen here so that the queue wait and the write share one deadline; a policy already frozen
    /// by the caller's loop is returned unchanged.
    const Retry frozen = op.freeze(policy);
    const Retry::Bound bound = frozen.bind(op.owner.now_ms());
    const uint64_t entered_ms = op.owner.now_ms();

    Item item{};
    {
        std::lock_guard lock(mutex);
        auto [it, inserted] = lanes.try_emplace(key);
        try
        {
            if (enter_after_lane_hook_for_test)
                enter_after_lane_hook_for_test();
            item.ticket = ++next_ticket;
            it->second.queue.push_back(&item);
        }
        catch (...)
        {
            /// A lane that holds nothing is nobody's; erasing it puts the map back as it was.
            if (inserted)
                lanes.erase(it);
            throw;
        }
    }

    /// From here the item is in the queue, and this guard is the only thing that removes it: on every
    /// exit, normal or by unwinding, it runs the leave step, which allocates nothing and cannot throw.
    bool entered_hold = false;
    struct Leave
    {
        CasHotKeys & owner;
        const String & key;
        Item & item;
        const bool & entered_hold;
        uint64_t entered_ms;
        CasOperation & op;
        ~Leave() noexcept { owner.leave(key, item, entered_hold, entered_ms, op); }
    } guard{*this, key, item, entered_hold, entered_ms, op};

    for (;;)
    {
        /// Outside the mutex: the engine's own admission in the engine's order (the fence generation,
        /// the lease budget, then the caller's liveness, which the engine reports as a lost fence),
        /// then the caller's bound. A lost fence is therefore never reported as a policy deadline, and
        /// an exhausted lease is reported as the lease.
        std::optional<WriteResult> leaving;
        CasOperation::WriteState nothing_sent;
        switch (op.gate(0))
        {
            case CasOperation::Gate::FenceLost:
                leaving = op.gaveUp(GaveUp::Why::FenceLost, sourceFor(bound), nothing_sent);
                break;
            case CasOperation::Gate::NoBudget:
                leaving = op.gaveUp(GaveUp::Why::Deadline, GaveUp::Source::Lease, nothing_sent);
                break;
            case CasOperation::Gate::Ok:
                break;
        }
        if (!leaving && !op.fits(0, bound))
            leaving = op.gaveUp(GaveUp::Why::Deadline, sourceFor(bound), nothing_sent);

        /// Read outside the mutex, as `leave` already does: the mutex is a leaf that calls nothing,
        /// and `now_ms` is a closure the pool injects, not a fixed clock.
        const uint64_t now = op.owner.now_ms();
        std::unique_lock lock(mutex);
        if (leaving)
            return std::move(*leaving);   /// the lock is released before the guard erases the item
        Lane & lane = lanes.at(key);
        if (lane.queue.front() == &item)
        {
            lane.holder_since_ms = now;
            entered_hold = true;
            break;
        }
        lane.cv.wait_for(lock, kWaitSlice);
    }
    ProfileEvents::increment(ProfileEvents::CASHotKeyQueueWaitMicroseconds, (op.owner.now_ms() - entered_ms) * 1000);
    return hold(key, op, frozen, bound, decide);
}

void CasHotKeys::leave(const String & key, Item & item, bool entered_hold, uint64_t entered_ms, CasOperation & op) noexcept
{
    /// The front item's ticket and how long it has held, when this caller left at its own bound behind
    /// it: what makes a stuck holder visible while it is stuck.
    std::optional<std::pair<uint64_t, uint64_t>> stuck_behind;
    {
        std::lock_guard lock(mutex);
        auto it = lanes.find(key);
        chassert(it != lanes.end());
        Lane & lane = it->second;
        auto found = std::find(lane.queue.begin(), lane.queue.end(), &item);
        chassert(found != lane.queue.end());
        lane.queue.erase(found);
        if (entered_hold)
            lane.holder_since_ms.reset();
        else if (!lane.queue.empty() && lane.holder_since_ms)
        {
            /// `now_ms` is the pool's injected clock and can throw; the fallback is no snapshot, never
            /// a throw out of this locked section, whose sole duty -- removing the item -- must complete
            /// regardless.
            try
            {
                const uint64_t now = op.owner.now_ms();
                stuck_behind = std::pair{lane.queue.front()->ticket, now - *lane.holder_since_ms};
            }
            catch (...)
            {
            }
        }
        if (lane.queue.empty())
            lanes.erase(it);
        else
            lane.cv.notify_all();
    }
    try
    {
        if (!entered_hold)
        {
            const uint64_t now = op.owner.now_ms();
            ProfileEvents::increment(ProfileEvents::CASHotKeyQueueWaitMicroseconds, (now - entered_ms) * 1000);
        }
        if (stuck_behind)
            LOG_WARNING(getLogger("CasHotKeys"),
                "hot key '{}': a writer left at its own bound while ticket {} has held the key for {} ms",
                key, stuck_behind->first, stuck_behind->second);
    }
    catch (...)
    {
        tryLogCurrentException("CasHotKeys");
    }
}

WriteResult CasHotKeys::hold(const String & key, CasOperation & op, const Retry & policy, const Retry::Bound & bound,
                             const Decide & decide)
{
    CasOperation::WriteState state;
    std::optional<Object> base;
    bool from_cache = false;
    /// A single-attempt submission never starts from a hint: its one attempt is on fresh state, as
    /// the engine's own verb reads before it.
    if (!policy.single_attempt)
    {
        if (auto remembered = cached(key))
        {
            base = std::move(remembered);
            from_cache = true;
            ProfileEvents::increment(ProfileEvents::CASHotKeyCacheStarts);
        }
    }
    const auto read_base = [&]() -> std::optional<WriteResult>
    {
        ProfileEvents::increment(ProfileEvents::CASHotKeyReadStarts);
        CasOperation::Resolved resolved = op.observe(key, policy, bound);
        state.last_seen = resolved.seen;
        base.reset();
        if (const auto * object = std::get_if<Object>(&state.last_seen))
            base = *object;
        else if (!std::holds_alternative<ProvenAbsent>(state.last_seen))
            return op.gaveUpAfterFailedObservation(resolved.stop, state, bound);
        return std::nullopt;
    };
    if (!from_cache)
        if (auto refused = read_base())
            return *refused;

    std::optional<String> candidate;
    bool verdict_on_hint = false;
    try
    {
        candidate = decide(base);
        verdict_on_hint = from_cache && !candidate;
    }
    catch (...)
    {
        if (!from_cache)
            throw;
        verdict_on_hint = true;
    }
    if (verdict_on_hint)
    {
        /// A verdict rendered on a hint proves only that a hint is not a proof: the entry is dropped,
        /// the key is read, and what the `decide` does on the read is the result. A read the caller's
        /// own gate refuses is that give-up, as for any caller that cannot read.
        forget(key);
        ProfileEvents::increment(ProfileEvents::CASHotKeyCacheVerdictsReread);
        if (auto refused = read_base())
            return *refused;
        from_cache = false;
        candidate = decide(base);
    }
    if (!candidate)
    {
        if (base)
            remember(key, *base);
        return Declined{state.last_seen};
    }

    WriteResult result = [&]
    {
        try
        {
            return base ? op.replace(key, *candidate, base->etag, policy) : op.create(key, *candidate, policy);
        }
        catch (...)
        {
            forget(key);
            throw;
        }
    }();
    std::visit(detail::Overload{
        [&](const Committed & committed) { remember(key, Object{*candidate, committed.etag}); },
        [&](const Conflict & conflict)
        {
            if (const auto * object = std::get_if<Object>(&conflict.seen))
                remember(key, *object);
            else
                forget(key);
        },
        [&](const Refused &) { forget(key); },
        [&](const GaveUp & gave_up) { if (gave_up.sent_any) forget(key); },
        [&](const Declined &) {}}, result);
    return result;
}

std::optional<Object> CasHotKeys::cached(const String & key) const
{
    if (!cache)
        return std::nullopt;
    if (auto hit = cache->get(key))
        return hit->object;
    return std::nullopt;
}

void CasHotKeys::remember(const String & key, Object object) noexcept
{
    if (!cache)
        return;
    try
    {
        if (cache_fill_hook_for_test)
            cache_fill_hook_for_test();
        /// Computed here, where a throw (allocation, `Etag::render`) is still just a failed fill: never
        /// zero, since the key, the incarnation and the containers weigh something even when the object
        /// is empty, so the byte budget bounds the entry count as well as the bytes.
        const size_t weight = key.size() + object.bytes.size() + object.etag.render().size() + 64;
        /// An object above the budget is not a hint worth evicting everything else for.
        if (weight > cache_budget_bytes)
        {
            forget(key);
            return;
        }
        auto remembered = std::make_shared<Remembered>(Remembered{std::move(object), weight});
        cache->set(key, remembered);
    }
    catch (...)
    {
        /// A hint that could not be stored is no hint: the next hold reads. The result that led here
        /// stands; a failed fill must never replace it.
        tryLogCurrentException("CasHotKeys");
        forget(key);
    }
}

void CasHotKeys::forget(const String & key) noexcept
{
    if (!cache)
        return;
    try
    {
        cache->remove(key);
    }
    catch (...)
    {
        tryLogCurrentException("CasHotKeys");
    }
}

size_t CasHotKeys::queueDepthForTest(const String & key) const
{
    std::lock_guard lock(mutex);
    const auto it = lanes.find(key);
    return it == lanes.end() ? 0 : it->second.queue.size();
}

size_t CasHotKeys::laneCountForTest() const
{
    std::lock_guard lock(mutex);
    return lanes.size();
}

size_t CasHotKeys::cacheEntriesForTest() const
{
    return cache ? cache->count() : 0;
}

}
