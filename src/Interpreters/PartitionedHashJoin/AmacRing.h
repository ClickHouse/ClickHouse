#pragma once

#include <Interpreters/HashJoin/KeyGetter.h>
#include <base/defines.h>
#include <Common/ColumnsHashing.h>
#include <Common/HashTable/HashMap.h>

#include <algorithm>
#include <array>
#include <bit>
#include <limits>

namespace DB
{

/** AMAC (asynchronous memory access chaining): a ring of in-flight rows where each visit takes
  * exactly one memory-dependent step and prefetches the address its next visit will dereference, so
  * the data-dependent misses of several rows overlap instead of serializing.
  *
  * Three pieces: `ResumableHashMap` decomposes the map's emplace/find into a seed and a one-cell
  * step; a policy owns the per-row state as parallel arrays and the seed/step bodies; `amacRun`
  * drives them.
  *
  * The correctness invariant of a build policy's `step`: the cell read and the mutation it implies
  * have to be one indivisible visit. Read a batch of cells and mutate afterwards, and two in-flight
  * rows with the same key - or two keys colliding on one cell - can both see it empty and both claim
  * it, silently dropping a build row. Fused, the in-flight rows are exactly a sequential insert with
  * the rows reordered, which an unordered join does not care about.
  */

/** The standard join hash map with a resumable-cursor API on top. Derived rather than wrapped so
  * that the cells, hash, grower and public interface stay exactly the standard ones - the key
  * getters, the probe machinery and the non-joined iteration see an unchanged map - while the cursor
  * methods reach the protected `buf`, `grower` and `m_size` the decomposed emplace/find needs.
  */
template <typename Base>
struct ResumableHashMap : public Base
{
    using Base::Base;
    using Cell = Base::cell_type;
    using Key = Base::key_type;

    /// Keys equal to the zero sentinel live in the map's dedicated zero-value cell, not in the
    /// buffer; a policy handles them synchronously through the standard `emplace`/`find`.
    bool isZeroKey(const Key & key) const { return Cell::isZero(key, *this); }

    /// Seed: the home cell of a hash value.
    size_t cursorPlace(size_t hash_value) const { return this->grower.place(hash_value); }

    /// Step: the next cell of the collision resolution chain.
    size_t cursorNext(size_t place) const { return this->grower.next(place); }

    /// The mask the flat leaf descriptors carry, for lookups that never touch the map object.
    size_t cursorMask() const { return this->grower.bufSize() - 1; }

    Cell * cursorCell(size_t place) { return &this->buf[place]; }
    const Cell * cursorCell(size_t place) const { return &this->buf[place]; }

    /// The cell buffer base, for policies that cache it in a field instead of re-resolving it
    /// through the map per visit. Invalidated by `cursorGrow`.
    Cell * cursorCells() { return this->buf; }

    bool cursorCellIsEmpty(const Cell * cell) const { return cell->isZero(*this); }

    bool cursorKeyEquals(const Cell * cell, const Key & key, size_t hash_value) const { return cell->keyEquals(key, hash_value, *this); }

    /** Claim an empty cell for a new key: exactly what `emplaceNonZeroImpl` does up to the
      * mapped-value write, which the caller performs in the same fused step. The caller passes
      * the cell pointer it already computed for the empty check. Returns whether the insert
      * overflowed the grower - the caller must then drain its ring and call `cursorGrow`
      * (the standard path resizes at the same point).
      */
    template <typename KeyHolder>
    ALWAYS_INLINE bool cursorClaim(Cell * cell, KeyHolder && key_holder, size_t hash_value)
    {
        keyHolderPersistKey(key_holder);
        const auto & key = keyHolderGetKey(key_holder);
        new (cell) Cell(key, *this);
        cell->setHash(hash_value);
        ++this->m_size;
        return this->grower.overflow(this->m_size);
    }

    /// Growth is a ring cancellation point: in-flight positions index the old buffer, so the
    /// driver drains the ring first, resizes here, and re-seeds the collected rows.
    void cursorGrow() { this->resize(); }
};

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
    DoneNeedsGrow /// the row completed by a claim that overflowed the grower; drain + grow
};

/// The compile-time gate of the AMAC path. Two getters stay on the plain loop: the LowCardinality
/// one deduplicates lookups per dictionary index through its own cache, which a ring bypasses rather
/// than accelerates (the same reason it disables the look-ahead prefetch), and the `hashed` fallback
/// recomputes a 128-bit serialized-key hash on every key-holder fetch, which a ring pays per visit.
/// `FixedHashMap` (`key8`/`key16`) has no collision chain to pipeline and no cursor API.
template <typename Map>
concept AmacResumableMap = requires(std::remove_const_t<Map> & map, const std::remove_const_t<Map> & const_map, size_t place)
{
    { const_map.cursorPlace(place) } -> std::same_as<size_t>;
    { const_map.cursorNext(place) } -> std::same_as<size_t>;
    { map.cursorCell(place) };
    { map.cursorGrow() };
};

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
    = AmacResumableMap<Map> && !is_low_cardinality_join_key_getter<KeyGetter> && !is_hashed_join_key_getter<KeyGetter>;

/** Growth cancellation: collect the other in-flight rows, deactivate them, let the policy grow the
  * map (slot `skip`'s row is already fully inserted and the resize rehashes it), then re-admit the
  * collected rows through `reseed` - a `start` without the synchronous cases, which rows in the ring
  * never had. Two ordering rules are load-bearing:
  *
  *  - Rows go back into exactly the slots they came from. Filling "the first pending-count free
  *    slots" instead would move the active set whenever a slot was already inactive - possible in
  *    the steady phase, where a failed refill drops `full` but the sweep still finishes - and the
  *    rest of that sweep would then step a slot the re-seed just emptied.
  *  - Rows are re-admitted in row order, not collection order. A mid-sweep refill can put a later
  *    row in a lower slot than an earlier in-flight row, and since a sweep acts on lower slots
  *    first - the growth having erased the earlier row's visit-count lead - collection order could
  *    let a later duplicate claim a cell before an earlier one, which first-wins `RowRef` maps
  *    expose. Ring rows are source row indexes and every caller's selector is monotonic, so sorting
  *    restores the sequential loop's insert order.
  *
  * Force-inlined because it is called from inside the steady loop: out of line it would capture the
  * policy and ring addresses and force conservative per-visit reloads of the policy invariants.
  */
template <size_t ring_size, typename Policy, typename Ring>
ALWAYS_INLINE void amacDrainAndGrow(Policy & policy, Ring & ring, size_t skip)
{
    static_assert(ring_size <= std::numeric_limits<UInt8>::max() + 1, "slot indexes are collected as bytes");
    std::array<UInt32, ring_size> pending_rows{};
    std::array<UInt8, ring_size> pending_slots{};
    size_t pending_count = 0;
    for (size_t j = 0; j < ring_size; ++j)
    {
        if (j == skip || !ring.isActive(j))
            continue;
        pending_rows[pending_count] = ring.rowAt(j);
        pending_slots[pending_count] = static_cast<UInt8>(j);
        ++pending_count;
        ring.deactivate(j);
    }

    policy.grow();

    std::sort(pending_rows.begin(), pending_rows.begin() + pending_count);

    for (size_t k = 0; k < pending_count; ++k)
        policy.reseed(ring, pending_slots[k], pending_rows[k]);
}

/** The ring driver. A policy supplies `Ring<ring_size>` - the per-row state, value-initialized to
  * all-inactive, with `isActive` / `deactivate` / `rowAt` - plus `may_grow`, `start(ring, s, row)`
  * (seed the slot and prefetch; false means the row was handled synchronously and the slot stays
  * free), `step(ring, s)`, and optionally `reseed`.
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
                const AmacStepResult result = policy.step(ring, s);
                if (result == AmacStepResult::Advance)
                    continue;
                if constexpr (Policy::may_grow)
                {
                    if (result == AmacStepResult::DoneNeedsGrow)
                        amacDrainAndGrow<ring_size>(policy, ring, s);
                }
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
            const AmacStepResult result = policy.step(ring, s);
            if (result == AmacStepResult::Advance)
                continue;
            if constexpr (Policy::may_grow)
            {
                if (result == AmacStepResult::DoneNeedsGrow)
                    amacDrainAndGrow<ring_size>(policy, ring, s);
            }
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
