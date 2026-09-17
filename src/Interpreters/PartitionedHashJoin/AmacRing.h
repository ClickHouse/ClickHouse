#pragma once

#include <Interpreters/HashJoin/KeyGetter.h>
#include <Interpreters/PartitionedHashJoin/HashJoinTable.h>
#include <base/defines.h>
#include <Common/ColumnsHashing.h>

#include <bit>
#include <limits>

namespace DB
{

/** AMAC (asynchronous memory access chaining): a ring of in-flight rows. Each visit takes
  * exactly one memory-dependent step and prefetches the address its next visit will dereference.
  * The data-dependent misses of several rows overlap instead of serializing.
  *
  * Two pieces: a policy owns the per-row state as parallel arrays and the seed/step bodies over the
  * `HashJoinTable` cells; `amacRun` drives them. The ring has no cancellation point. The table grows
  * only on wrapping inserts (the single-partition build and the overflow drain) or between waves.
  * Neither uses the ring, so no ring is ever in flight across a resize. A build ring's in-flight rows
  * all belong to the partition its worker holds. Besides claiming, appending or advancing, a visit
  * can only hand a row that reached its range end to the overflow buffer.
  *
  * The correctness invariant of a build policy's `step`: the cell read and the mutation it implies
  * have to be one indivisible visit. If a step read a batch of cells and mutated afterwards, two
  * in-flight rows with the same key - or two keys colliding on one cell - could both see it empty
  * and both claim it, silently dropping a build row. Fused, the in-flight rows are exactly a
  * sequential insert with the rows reordered. An unordered join does not care about that order.
  */

/// The string-key cells compare a saved hash as a prefilter. Recomputing a string hash per visit is
/// expensive, so those rings carry the hash in the slot. Every other cell ignores the hash argument.
/// That keeps their slots at 16 bytes.
template <typename Cell>
constexpr bool cell_stores_hash = requires(const Cell & cell) { cell.saved_hash; };

/// The inactive sentinel of a build ring's row array, and therefore the driver's upper bound on rows
/// per run. The probe ring marks inactivity in its cell-pointer array instead.
constexpr UInt32 amac_inactive_row = std::numeric_limits<UInt32>::max();

/// 8-10 in-flight rows already saturate a core's L1-D miss handling. Past 32 the ring starts
/// thrashing the TLB (Kocberber et al., PVLDB 2015).
constexpr size_t amac_ring_size = 32;

/// Below this the ring's prime and drain phases cost more than the overlap wins.
constexpr size_t amac_min_rows = 256;

enum class AmacStepResult : UInt8
{
    Advance, /// collision: the cursor advanced and prefetched the next cell; revisit later
    Done, /// the row completed; the slot can be recycled
};

/// The compile-time gate of the AMAC path. Two key getters stay on the plain loop. The LowCardinality
/// getter deduplicates lookups per dictionary index through its own cache. A ring would bypass that
/// cache (the same reason it disables the look-ahead prefetch). The `hashed` getter recomputes a 128-bit
/// hash of the serialized key on every key-holder fetch. A ring would pay that per visit. `FixedHashMap`
/// (`key8`/`key16`) has no collision chain to pipeline.
template <typename T>
inline constexpr bool is_low_cardinality_join_key_getter = false;
template <typename BaseMethod, typename Mapped, bool use_offset>
inline constexpr bool is_low_cardinality_join_key_getter<LowCardinalityKeyGetterForJoin<BaseMethod, Mapped, use_offset>> = true;

template <typename T>
inline constexpr bool is_hashed_join_key_getter = false;
template <typename Value, typename Mapped, bool use_cache, bool need_offset>
inline constexpr bool is_hashed_join_key_getter<ColumnsHashing::HashMethodHashed<Value, Mapped, use_cache, need_offset>> = true;

template <typename KeyGetter, typename Map>
constexpr bool amac_join_supported
    = is_hash_join_table<std::remove_const_t<Map>> && !is_low_cardinality_join_key_getter<KeyGetter> && !is_hashed_join_key_getter<KeyGetter>;

/** The ring driver. A policy supplies `Ring<ring_size>`: the per-row state, value-initialized to
  * all-inactive, with `isActive` / `deactivate`. It also supplies `start(ring, s, row)` (seed the slot
  * and prefetch; false means the row was handled synchronously and the slot stays free) and
  * `step(ring, s)`.
  *
  * The ring state is a struct of parallel arrays, not an array of slot structs. A wide field - a
  * 16- or 32-byte stored key - therefore cannot misalign every other field against cache lines. Keeping
  * the slot state minimal is what makes the ring pay off. Fat slot state spills to the stack and costs
  * more than the overlap wins. Anything recomputable from the row index is recomputed. A policy stores
  * a resolved address only where the steady step would otherwise re-resolve it on every visit.
  *
  * Steady/drain split: while rows remain and every refill has succeeded, every slot is provably
  * active. The steady phase can sweep with a plain `for` - no active check, no modulo. The first
  * failed refill drops into the drain loop, which checks.
  */
template <typename Policy, size_t ring_size = amac_ring_size>
void amacRun(Policy & policy_arg, size_t rows)
{
    static_assert(std::has_single_bit(ring_size));
    chassert(rows < amac_inactive_row);

    /// A policy whose fields are per-run invariants can opt into a frame-local copy. The copy's
    /// address never escapes: every policy call inlines. Its fields therefore become SSA values that
    /// stores through the result arrays cannot alias. Behind the caller's reference the compiler reloads
    /// them per visit instead.
    static constexpr bool run_on_copy = requires { requires Policy::copy_into_frame; };
    std::conditional_t<run_on_copy, Policy, Policy &> policy = policy_arg;

    typename Policy::template Ring<ring_size> ring{};
    size_t next = 0;
    size_t active = 0;

    /// Pull rows into a slot until one enters the ring or the rows run out. Force-inlined: for the
    /// multi-column fixed-key policies clang otherwise outlines this lambda. That leaks the address of
    /// the policy copy and undoes the SSA promotion above. Every invariant is reloaded per visit in
    /// the steady loop, plus one call per completed row.
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
}

}
