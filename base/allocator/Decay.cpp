#include <allocator/Decay.h>

#include <allocator/PRNG.h>

#include <cstring>

namespace jemalloc
{

void Decay::deadlineInit()
{
    deadline.copy(epoch);
    deadline.add(interval);
    if (msRead() > 0)
    {
        Nanoseconds jitter;
        jitter.init(prngRangeU64(jitter_state, interval.ns()));
        deadline.add(jitter);
    }
}

void Decay::reinit(const Nanoseconds & current_time, ssize_t decay_ms)
{
    time_ms.store(decay_ms, std::memory_order_relaxed);
    if (decay_ms > 0)
    {
        interval.init(static_cast<uint64_t>(decay_ms) * 1000000ULL);
        interval.divideBy(SMOOTHSTEP_NUM_STEPS);
    }

    epoch.copy(current_time);
    /// The jitter stream is seeded with the address of the object, exactly like jemalloc.
    jitter_state = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(this));
    deadlineInit();
    num_unpurged = 0;
    std::memset(backlog, 0, SMOOTHSTEP_NUM_STEPS * sizeof(size_t));
}

bool Decay::init(const Nanoseconds & current_time, ssize_t decay_ms)
{
    if constexpr (config::debug)
    {
        /// jemalloc checks that the whole `decay_t` is zeroed; the mutex is checked by its own initialization (its
        /// initial state is not all-zero bytes on every platform).
        ALLOCATOR_ASSERT(!purging);
        ALLOCATOR_ASSERT(time_ms.load(std::memory_order_relaxed) == 0);
        ALLOCATOR_ASSERT(interval.ns() == 0 && epoch.ns() == 0 && deadline.ns() == 0);
        ALLOCATOR_ASSERT(jitter_state == 0 && num_pages_limit == 0 && num_unpurged == 0 && ceil_num_pages == 0);
        for (size_t i = 0; i < SMOOTHSTEP_NUM_STEPS; ++i)
            ALLOCATOR_ASSERT(backlog[i] == 0);
        ceil_num_pages = 0;
    }
    if (mutex.init("decay", MutexRank::DECAY, MutexLockOrder::RankExclusive))
        return true;
    purging = false;
    reinit(current_time, decay_ms);
    return false;
}

bool Decay::msValid(ssize_t decay_ms)
{
    if (decay_ms < -1)
        return false;
    if (decay_ms == -1 || static_cast<uint64_t>(decay_ms) <= NANOSECONDS_MAX_SECONDS * 1000ULL)
        return true;
    return false;
}

void Decay::maybeUpdateTime(const Nanoseconds & new_time)
{
    if (ALLOCATOR_UNLIKELY(!Nanoseconds::isMonotonic() && epoch.compare(new_time) > 0))
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
        ALLOCATOR_ASSERT(epoch.compare(new_time) <= 0);
    }
}

size_t Decay::backlogNumPagesLimit() const
{
    /// For each element of the backlog, multiply by the corresponding fixed-point smoothstep decay factor. Sum the
    /// products, then divide to round down to the nearest whole number of pages.
    uint64_t sum = 0;
    for (unsigned i = 0; i < SMOOTHSTEP_NUM_STEPS; ++i)
        sum += backlog[i] * smoothstep_h_steps[i];
    size_t num_pages_limit_backlog = static_cast<size_t>(sum >> SMOOTHSTEP_BINARY_FIXED_POINT);

    return num_pages_limit_backlog;
}

void Decay::backlogUpdate(uint64_t num_advance_u64, size_t current_num_pages)
{
    if (num_advance_u64 >= SMOOTHSTEP_NUM_STEPS)
    {
        std::memset(backlog, 0, (SMOOTHSTEP_NUM_STEPS - 1) * sizeof(size_t));
    }
    else
    {
        size_t num_advance_size = static_cast<size_t>(num_advance_u64);

        ALLOCATOR_ASSERT(static_cast<uint64_t>(num_advance_size) == num_advance_u64);

        std::memmove(backlog, &backlog[num_advance_size], (SMOOTHSTEP_NUM_STEPS - num_advance_size) * sizeof(size_t));
        if (num_advance_size > 1)
            std::memset(&backlog[SMOOTHSTEP_NUM_STEPS - num_advance_size], 0, (num_advance_size - 1) * sizeof(size_t));
    }

    size_t num_pages_delta = (current_num_pages > num_unpurged) ? current_num_pages - num_unpurged : 0;
    backlog[SMOOTHSTEP_NUM_STEPS - 1] = num_pages_delta;

    if constexpr (config::debug)
    {
        if (current_num_pages > ceil_num_pages)
            ceil_num_pages = current_num_pages;
        size_t limit = backlogNumPagesLimit();
        ALLOCATOR_ASSERT(ceil_num_pages >= limit);
        if (ceil_num_pages > limit)
            ceil_num_pages = limit;
    }
}

uint64_t Decay::numPagesPurgeIn(const Nanoseconds & time, size_t num_pages_new) const
{
    uint64_t decay_interval_ns = epochDurationNs();
    ALLOCATOR_ASSERT(decay_interval_ns != 0);
    size_t num_epoch = static_cast<size_t>(time.ns() / decay_interval_ns);

    uint64_t num_pages_purge;
    if (num_epoch >= SMOOTHSTEP_NUM_STEPS)
    {
        num_pages_purge = num_pages_new;
    }
    else
    {
        uint64_t h_steps_max = smoothstep_h_steps[SMOOTHSTEP_NUM_STEPS - 1];
        ALLOCATOR_ASSERT(h_steps_max >= smoothstep_h_steps[SMOOTHSTEP_NUM_STEPS - 1 - num_epoch]);
        num_pages_purge = num_pages_new * (h_steps_max - smoothstep_h_steps[SMOOTHSTEP_NUM_STEPS - 1 - num_epoch]);
        num_pages_purge >>= SMOOTHSTEP_BINARY_FIXED_POINT;
    }
    return num_pages_purge;
}

bool Decay::maybeAdvanceEpoch(const Nanoseconds & new_time, size_t num_pages_current)
{
    /// Handle possible non-monotonicity of time.
    maybeUpdateTime(new_time);

    if (!deadlineReached(new_time))
        return false;
    Nanoseconds delta;
    delta.copy(new_time);
    delta.subtract(epoch);

    uint64_t num_advance_u64 = delta.divide(interval);
    ALLOCATOR_ASSERT(num_advance_u64 > 0);

    /// Add nadvance_u64 decay intervals to epoch.
    delta.copy(interval);
    delta.multiplyBy(num_advance_u64);
    epoch.add(delta);

    /// Set a new deadline.
    deadlineInit();

    /// Update the backlog.
    backlogUpdate(num_advance_u64, num_pages_current);

    num_pages_limit = backlogNumPagesLimit();
    num_unpurged = (num_pages_limit > num_pages_current) ? num_pages_limit : num_pages_current;

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
/// to avoid iterating over [interval_epochs..SMOOTHSTEP_NUM_STEPS) twice.
size_t Decay::numPurgeAfterInterval(size_t interval_epochs) const
{
    size_t i;
    uint64_t sum = 0;
    for (i = 0; i < interval_epochs; ++i)
        sum += backlog[i] * smoothstep_h_steps[i];
    for (; i < SMOOTHSTEP_NUM_STEPS; ++i)
        sum += backlog[i] * (smoothstep_h_steps[i] - smoothstep_h_steps[i - interval_epochs]);

    return static_cast<size_t>(sum >> SMOOTHSTEP_BINARY_FIXED_POINT);
}

uint64_t Decay::nsUntilPurge(size_t num_pages_current, uint64_t num_pages_threshold) const
{
    if (!gradually())
        return DECAY_UNBOUNDED_TIME_TO_PURGE;
    uint64_t decay_interval_ns = epochDurationNs();
    ALLOCATOR_ASSERT(decay_interval_ns > 0);
    if (num_pages_current == 0)
    {
        unsigned i;
        for (i = 0; i < SMOOTHSTEP_NUM_STEPS; ++i)
        {
            if (backlog[i] > 0)
                break;
        }
        if (i == SMOOTHSTEP_NUM_STEPS)
        {
            /// No dirty pages recorded. Sleep indefinitely.
            return DECAY_UNBOUNDED_TIME_TO_PURGE;
        }
    }
    if (num_pages_current <= num_pages_threshold)
    {
        /// Use max interval.
        return decay_interval_ns * SMOOTHSTEP_NUM_STEPS;
    }

    /// Minimal 2 intervals to ensure reaching next epoch deadline.
    size_t lower_bound = 2;
    size_t upper_bound = SMOOTHSTEP_NUM_STEPS;

    size_t num_purge_lower_bound = numPurgeAfterInterval(lower_bound);
    if (num_purge_lower_bound > num_pages_threshold)
        return decay_interval_ns * lower_bound;
    size_t num_purge_upper_bound = numPurgeAfterInterval(upper_bound);
    if (num_purge_upper_bound < num_pages_threshold)
        return decay_interval_ns * upper_bound;

    [[maybe_unused]] unsigned num_search = 0;
    while ((num_purge_lower_bound + num_pages_threshold < num_purge_upper_bound) && (lower_bound + 2 < upper_bound))
    {
        size_t target = (lower_bound + upper_bound) / 2;
        size_t num_purge = numPurgeAfterInterval(target);
        if (num_purge > num_pages_threshold)
        {
            upper_bound = target;
            num_purge_upper_bound = num_purge;
        }
        else
        {
            lower_bound = target;
            num_purge_lower_bound = num_purge;
        }
        ALLOCATOR_ASSERT(num_search < log2Floor(SMOOTHSTEP_NUM_STEPS) + 1);
        ++num_search;
    }
    return decay_interval_ns * (upper_bound + lower_bound) / 2;
}

}
