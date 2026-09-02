#pragma once

#include <Common/Stopwatch.h>

#include <algorithm>

namespace DB
{

/**
 * The purpose of this helper class is to provide a good value for prefetch look ahead (how distant row we should prefetch on the given iteration)
 * based on the latency of a single iteration of the given cycle.
 *
 * Assumed usage pattern is the following:
 *
 * PrefetchingHelper prefetching; /// When object is created, it starts a watch to measure iteration latency.
 * size_t prefetch_look_ahead = prefetching.getInitialLookAheadValue(); /// Initially it provides you with some reasonable default value.
 *
 * for (size_t i = 0; i < end; ++i)
 * {
 *     if (i == prefetching.iterationsToMeasure()) /// When enough iterations passed, we are able to make a fairly accurate estimation of a single iteration latency.
 *         prefetch_look_ahead = prefetching.calcPrefetchLookAhead(); /// Based on this estimation we can choose a good value for prefetch_look_ahead.
 *
 *     ... main loop body ...
 * }
 *
 */
class PrefetchingHelper
{
public:
    size_t calcPrefetchLookAhead()
    {
        static constexpr auto assumed_load_latency_ns = 100;
        static constexpr auto just_coefficient = 4;
        static constexpr auto numerator = just_coefficient * assumed_load_latency_ns * iterations_to_measure;
        size_t elapsed_ns = watch.elapsedNanoseconds();
        if (unlikely(elapsed_ns == 0))
            return max_look_ahead_value;
        size_t look_ahead = (numerator + elapsed_ns - 1) / elapsed_ns;
        return std::clamp<size_t>(look_ahead, min_look_ahead_value, max_look_ahead_value);
    }

    static constexpr size_t getInitialLookAheadValue() { return min_look_ahead_value; }

    static constexpr size_t iterationsToMeasure() { return iterations_to_measure; }

private:
    static constexpr size_t iterations_to_measure = 100;
    static constexpr size_t min_look_ahead_value = 4;
    static constexpr size_t max_look_ahead_value = 32;

    Stopwatch watch;
};

/// `min_bytes_for_prefetch` marks where a hash table whose cells carry an aggregate-state pointer
/// stops fitting in caches. What decides how many lookups miss is the number of cells - one random
/// probe each - not how wide they are, so a table of key-only cells has to start prefetching at the
/// same number of cells, which for it means fewer bytes. Scale the threshold by the ratio of the
/// cell to its mapped counterpart; for a table that does carry the pointer the ratio is 1.
/// Both carriers of the threshold go through here: `Aggregator`'s own prefetch gates and the
/// self-prefetching `HashMethodSerialized`.
template <typename Data, bool has_mapped>
size_t minBytesForPrefetch(size_t min_bytes_for_prefetch)
{
    if constexpr (has_mapped)
        return min_bytes_for_prefetch;
    else
    {
        using Cell = typename Data::cell_type;
        /// The mapped counterpart of this cell is the same cell plus an aligned pointer to the aggregate state.
        static constexpr size_t alignment = alignof(void *);
        static constexpr size_t mapped_cell_size = ((sizeof(Cell) + sizeof(void *) + alignment - 1) / alignment) * alignment;

        return min_bytes_for_prefetch * sizeof(Cell) / mapped_cell_size;
    }
}

}
