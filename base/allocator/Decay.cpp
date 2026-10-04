#include <allocator/Decay.h>

#include <allocator/Prng.h>

#include <cstring>

namespace jemalloc
{

void Decay::deadlineInit()
{
    deadline.copy(epoch);
    deadline.add(interval);
    if (msRead() > 0)
    {
        NsTime jitter;
        jitter.init(prngRangeU64(jitter_state, interval.ns()));
        deadline.add(jitter);
    }
}

void Decay::reinit(const NsTime & cur_time, ssize_t decay_ms)
{
    time_ms.store(decay_ms, std::memory_order_relaxed);
    if (decay_ms > 0)
    {
        interval.init(static_cast<uint64_t>(decay_ms) * 1000000ULL);
        interval.idivide(SMOOTHSTEP_NSTEPS);
    }

    epoch.copy(cur_time);
    /// The jitter stream is seeded with the address of the object, exactly like jemalloc.
    jitter_state = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(this));
    deadlineInit();
    nunpurged = 0;
    std::memset(backlog, 0, SMOOTHSTEP_NSTEPS * sizeof(size_t));
}

bool Decay::init(const NsTime & cur_time, ssize_t decay_ms)
{
    if constexpr (config::debug)
    {
        /// jemalloc checks that the whole `decay_t` is zeroed; the mutex is checked by its own initialization (its
        /// initial state is not all-zero bytes on every platform).
        JE_ASSERT(!purging);
        JE_ASSERT(time_ms.load(std::memory_order_relaxed) == 0);
        JE_ASSERT(interval.ns() == 0 && epoch.ns() == 0 && deadline.ns() == 0);
        JE_ASSERT(jitter_state == 0 && npages_limit == 0 && nunpurged == 0 && ceil_npages == 0);
        for (size_t i = 0; i < SMOOTHSTEP_NSTEPS; ++i)
            JE_ASSERT(backlog[i] == 0);
        ceil_npages = 0;
    }
    if (mtx.init("decay", MutexRank::DECAY, MutexLockOrder::RankExclusive))
        return true;
    purging = false;
    reinit(cur_time, decay_ms);
    return false;
}

bool Decay::msValid(ssize_t decay_ms)
{
    if (decay_ms < -1)
        return false;
    if (decay_ms == -1 || static_cast<uint64_t>(decay_ms) <= NSTIME_SEC_MAX * 1000ULL)
        return true;
    return false;
}

void Decay::maybeUpdateTime(const NsTime & new_time)
{
    if (JE_UNLIKELY(!NsTime::isMonotonic() && epoch.compare(new_time) > 0))
    {
        /// Time went backwards. Move the epoch back in time and generate a new deadline, with the expectation that
        /// time typically flows forward for long enough periods of time that epochs complete. Unfortunately, this
        /// strategy is susceptible to clock jitter triggering premature epoch advances, but clock jitter estimation
        /// and compensation isn't feasible here because calls into this code are event-driven.
        epoch.copy(new_time);
        deadlineInit();
    }
    else
    {
        /// Verify that time does not go backwards.
        JE_ASSERT(epoch.compare(new_time) <= 0);
    }
}

size_t Decay::backlogNpagesLimit() const
{
    /// For each element of the backlog, multiply by the corresponding fixed-point smoothstep decay factor. Sum the
    /// products, then divide to round down to the nearest whole number of pages.
    uint64_t sum = 0;
    for (unsigned i = 0; i < SMOOTHSTEP_NSTEPS; ++i)
        sum += backlog[i] * smoothstep_h_steps[i];
    size_t npages_limit_backlog = static_cast<size_t>(sum >> SMOOTHSTEP_BFP);

    return npages_limit_backlog;
}

void Decay::backlogUpdate(uint64_t nadvance_u64, size_t current_npages)
{
    if (nadvance_u64 >= SMOOTHSTEP_NSTEPS)
    {
        std::memset(backlog, 0, (SMOOTHSTEP_NSTEPS - 1) * sizeof(size_t));
    }
    else
    {
        size_t nadvance_z = static_cast<size_t>(nadvance_u64);

        JE_ASSERT(static_cast<uint64_t>(nadvance_z) == nadvance_u64);

        std::memmove(backlog, &backlog[nadvance_z], (SMOOTHSTEP_NSTEPS - nadvance_z) * sizeof(size_t));
        if (nadvance_z > 1)
            std::memset(&backlog[SMOOTHSTEP_NSTEPS - nadvance_z], 0, (nadvance_z - 1) * sizeof(size_t));
    }

    size_t npages_delta = (current_npages > nunpurged) ? current_npages - nunpurged : 0;
    backlog[SMOOTHSTEP_NSTEPS - 1] = npages_delta;

    if constexpr (config::debug)
    {
        if (current_npages > ceil_npages)
            ceil_npages = current_npages;
        size_t limit = backlogNpagesLimit();
        JE_ASSERT(ceil_npages >= limit);
        if (ceil_npages > limit)
            ceil_npages = limit;
    }
}

uint64_t Decay::npagesPurgeIn(const NsTime & time, size_t npages_new) const
{
    uint64_t decay_interval_ns = epochDurationNs();
    JE_ASSERT(decay_interval_ns != 0);
    size_t n_epoch = static_cast<size_t>(time.ns() / decay_interval_ns);

    uint64_t npages_purge;
    if (n_epoch >= SMOOTHSTEP_NSTEPS)
    {
        npages_purge = npages_new;
    }
    else
    {
        uint64_t h_steps_max = smoothstep_h_steps[SMOOTHSTEP_NSTEPS - 1];
        JE_ASSERT(h_steps_max >= smoothstep_h_steps[SMOOTHSTEP_NSTEPS - 1 - n_epoch]);
        npages_purge = npages_new * (h_steps_max - smoothstep_h_steps[SMOOTHSTEP_NSTEPS - 1 - n_epoch]);
        npages_purge >>= SMOOTHSTEP_BFP;
    }
    return npages_purge;
}

bool Decay::maybeAdvanceEpoch(const NsTime & new_time, size_t npages_current)
{
    /// Handle possible non-monotonicity of time.
    maybeUpdateTime(new_time);

    if (!deadlineReached(new_time))
        return false;
    NsTime delta;
    delta.copy(new_time);
    delta.subtract(epoch);

    uint64_t nadvance_u64 = delta.divide(interval);
    JE_ASSERT(nadvance_u64 > 0);

    /// Add nadvance_u64 decay intervals to epoch.
    delta.copy(interval);
    delta.imultiply(nadvance_u64);
    epoch.add(delta);

    /// Set a new deadline.
    deadlineInit();

    /// Update the backlog.
    backlogUpdate(nadvance_u64, npages_current);

    npages_limit = backlogNpagesLimit();
    nunpurged = (npages_limit > npages_current) ? npages_limit : npages_current;

    return true;
}

/// First, calculate how many pages should remain at the moment, then subtract the number of pages that should remain
/// after `interval_epochs`. The difference is how many pages should be purged until then.
///
/// The number of pages that should remain at a specific moment is calculated like this:
/// pages(now) = sum(backlog[i] * h_steps[i]). After `interval_epochs` passes, backlog would shift `interval_epochs`
/// positions to the left and sigmoid curve would be applied starting with backlog[interval_epochs].
///
/// The implementation doesn't directly map to the description, but it's essentially the same calculation, optimized
/// to avoid iterating over [interval_epochs..SMOOTHSTEP_NSTEPS) twice.
size_t Decay::npurgeAfterInterval(size_t interval_epochs) const
{
    size_t i;
    uint64_t sum = 0;
    for (i = 0; i < interval_epochs; ++i)
        sum += backlog[i] * smoothstep_h_steps[i];
    for (; i < SMOOTHSTEP_NSTEPS; ++i)
        sum += backlog[i] * (smoothstep_h_steps[i] - smoothstep_h_steps[i - interval_epochs]);

    return static_cast<size_t>(sum >> SMOOTHSTEP_BFP);
}

uint64_t Decay::nsUntilPurge(size_t npages_current, uint64_t npages_threshold) const
{
    if (!gradually())
        return DECAY_UNBOUNDED_TIME_TO_PURGE;
    uint64_t decay_interval_ns = epochDurationNs();
    JE_ASSERT(decay_interval_ns > 0);
    if (npages_current == 0)
    {
        unsigned i;
        for (i = 0; i < SMOOTHSTEP_NSTEPS; ++i)
        {
            if (backlog[i] > 0)
                break;
        }
        if (i == SMOOTHSTEP_NSTEPS)
        {
            /// No dirty pages recorded. Sleep indefinitely.
            return DECAY_UNBOUNDED_TIME_TO_PURGE;
        }
    }
    if (npages_current <= npages_threshold)
    {
        /// Use max interval.
        return decay_interval_ns * SMOOTHSTEP_NSTEPS;
    }

    /// Minimal 2 intervals to ensure reaching next epoch deadline.
    size_t lb = 2;
    size_t ub = SMOOTHSTEP_NSTEPS;

    size_t npurge_lb = npurgeAfterInterval(lb);
    if (npurge_lb > npages_threshold)
        return decay_interval_ns * lb;
    size_t npurge_ub = npurgeAfterInterval(ub);
    if (npurge_ub < npages_threshold)
        return decay_interval_ns * ub;

    [[maybe_unused]] unsigned n_search = 0;
    while ((npurge_lb + npages_threshold < npurge_ub) && (lb + 2 < ub))
    {
        size_t target = (lb + ub) / 2;
        size_t npurge = npurgeAfterInterval(target);
        if (npurge > npages_threshold)
        {
            ub = target;
            npurge_ub = npurge;
        }
        else
        {
            lb = target;
            npurge_lb = npurge;
        }
        JE_ASSERT(n_search < lgFloor(SMOOTHSTEP_NSTEPS) + 1);
        ++n_search;
    }
    return decay_interval_ns * (ub + lb) / 2;
}

}
