#pragma once

#include <Interpreters/HashJoin/KeyGetter.h>
#include <Interpreters/PartitionedHashJoin/SharedJoinTable.h>
#include <base/defines.h>
#include <Common/ColumnsHashing.h>

#include <array>
#include <bit>
#include <limits>

namespace DB
{

/** AMAC (asynchronous memory access chaining): a ring of in-flight rows where each visit takes
  * exactly one memory-dependent step and prefetches the address its next visit will dereference, so
  * the data-dependent misses of several rows overlap instead of serializing.
  *
  * Two pieces: a policy owns the per-row state as parallel arrays and the seed/step bodies over the
  * shared table's cells; `amacRun` drives them. The table never grows, so there is no cancellation
  * point: a build ring's in-flight rows all belong to the partition the worker holds, and the only
  * thing a visit can do besides claiming, appending or advancing is hand a row that reached the range
  * end to the overflow buffer.
  *
  * The correctness invariant of a build policy's `step`: the cell read and the mutation it implies
  * have to be one indivisible visit. Read a batch of cells and mutate afterwards, and two in-flight
  * rows with the same key - or two keys colliding on one cell - can both see it empty and both claim
  * it, silently dropping a build row. Fused, the in-flight rows are exactly a sequential insert with
  * the rows reordered, which an unordered join does not care about.
  */

/// The string-key cells compare a saved hash as a prefilter, and recomputing a string hash per visit
/// is expensive, so those rings carry the hash in the slot. Every other cell ignores the hash
/// argument, which keeps their slots at 16 bytes.
template <typename Cell>
constexpr bool cell_stores_hash = requires(const Cell & cell) { cell.saved_hash; };

/// Inactive sentinel of a build ring's row array - the probe ring marks inactivity in its
/// cell-pointer array instead - and so also the driver's row-count bound.
constexpr UInt32 amac_inactive_row = std::numeric_limits<UInt32>::max();

/// 8-10 in-flight rows already saturate a core's L1-D miss handling, and past 32 the ring starts
/// thrashing the TLB (Kocberber et al., PVLDB 2015).
constexpr size_t amac_ring_size = 32;

/// Below this the ring's prime and drain phases cost more than the overlap wins.
constexpr size_t amac_min_rows = 256;

enum class AmacStepResult : UInt8
{
    Advance, /// collision: the cursor advanced and prefetched the next cell; revisit later
    Done, /// the row completed; the slot can be recycled
};

/// The compile-time gate of the AMAC path. Two getters stay on the plain loop: the LowCardinality
/// one deduplicates lookups per dictionary index through its own cache, which a ring bypasses rather
/// than accelerates (the same reason it disables the look-ahead prefetch), and the `hashed` fallback
/// recomputes a 128-bit serialized-key hash on every key-holder fetch, which a ring pays per visit.
/// `FixedHashMap` (`key8`/`key16`) has no collision chain to pipeline.
template <typename T>
inline constexpr bool is_low_cardinality_join_key_getter = false;
template <typename BaseMethod, typename Mapped>
inline constexpr bool is_low_cardinality_join_key_getter<LowCardinalityKeyGetterForJoin<BaseMethod, Mapped>> = true;

template <typename T>
inline constexpr bool is_hashed_join_key_getter = false;
template <typename Value, typename Mapped, bool use_cache, bool need_offset>
inline constexpr bool is_hashed_join_key_getter<ColumnsHashing::HashMethodHashed<Value, Mapped, use_cache, need_offset>> = true;

template <typename KeyGetter, typename Map>
constexpr bool amac_join_supported
    = is_shared_join_table<std::remove_const_t<Map>> && !is_low_cardinality_join_key_getter<KeyGetter> && !is_hashed_join_key_getter<KeyGetter>;

/** The ring driver. A policy supplies `Ring<ring_size>` - the per-row state, value-initialized to
  * all-inactive, with `isActive` / `deactivate` / `rowAt` - plus `start(ring, s, row)` (seed the slot
  * and prefetch; false means the row was handled synchronously and the slot stays free) and
  * `step(ring, s)`.
  *
  * The ring state is a struct of parallel arrays, not an array of slot structs, so a wide field - a
  * 16- or 32-byte stored key - cannot misalign every other field against cache lines. Keeping it
  * minimal is what makes the ring work at all: fat slot state spills to the stack and costs more
  * than the overlap wins, so anything recomputable from the row index is recomputed, and a policy
  * carries resolved address material only where the steady step would otherwise re-resolve it per
  * visit.
  *
  * Steady/drain split: while rows remain and every refill has succeeded, every slot is provably
  * active, so the steady phase can sweep with a plain `for` - no active check, no modulo. The first
  * failed refill drops into the drain loop, which checks.
  */
template <typename Policy, size_t ring_size = amac_ring_size>
void amacRun(Policy & policy_arg, size_t rows)
{
    static_assert(std::has_single_bit(ring_size));
    chassert(rows < amac_inactive_row);

    /// A policy whose fields are per-run invariants can opt into a frame-local copy. The copy's
    /// address never escapes - every policy call inlines - so its fields become SSA values that
    /// stores through the result arrays cannot alias; behind the caller's reference the compiler
    /// reloads them per visit instead. A policy with mutable aggregates opts in too by providing
    /// `writeBackTo`. An exception mid-run skips the write-back, which matches by-reference
    /// semantics: nothing reads the aggregates until the run has finished.
    static constexpr bool run_on_copy = requires { requires Policy::copy_into_frame; };
    std::conditional_t<run_on_copy, Policy, Policy &> policy = policy_arg;

    typename Policy::template Ring<ring_size> ring{};
    size_t next = 0;
    size_t active = 0;

    /// Pull rows into a slot until one enters the ring or the rows run out. Force-inlined because
    /// clang otherwise outlines it for the multi-column fixed-key policies, which leaks the policy
    /// copy's address and undoes the SSA promotion above - reintroducing a per-visit reload of every
    /// invariant in the steady loop, plus a call per completed row.
    auto refill = [&](size_t s) ALWAYS_INLINE
    {
        while (next < rows)
        {
            const size_t row = next;
            ++next;
            if (policy.start(ring, s, row))
                break;
        }
    };

    /// After priming, either every slot is active or the rows are exhausted.
    for (size_t s = 0; s < ring_size; ++s)
    {
        refill(s);
        active += ring.isActive(s);
    }

    if (active == ring_size)
    {
        bool full = true;
        while (full && next < rows)
        {
            for (size_t s = 0; s < ring_size; ++s)
            {
                if (policy.step(ring, s) == AmacStepResult::Advance)
                    continue;
                ring.deactivate(s);
                refill(s);
                if (!ring.isActive(s))
                {
                    --active;
                    full = false;
                }
            }
        }
    }

    /// Drain: no refills left; finish the in-flight rows.
    while (active > 0)
    {
        for (size_t s = 0; s < ring_size; ++s)
        {
            if (!ring.isActive(s))
                continue;
            if (policy.step(ring, s) == AmacStepResult::Advance)
                continue;
            ring.deactivate(s);
            --active;
        }
    }

    if constexpr (run_on_copy)
    {
        if constexpr (requires { policy.writeBackTo(policy_arg); })
            policy.writeBackTo(policy_arg);
    }
}

}
