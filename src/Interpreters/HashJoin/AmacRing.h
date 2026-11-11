#pragma once

#include <base/defines.h>
#include <base/types.h>

namespace DB
{

/** AMAC (asynchronous memory access chaining): a ring of in-flight rows. Each visit of a row takes
  * exactly one memory-dependent step and prefetches the address that its next visit will dereference.
  * So the data-dependent cache misses of several rows overlap instead of coming one after another.
  *
  * `amacRun` drives a policy, which holds the state of the rows and takes their steps over the cells of
  * a hash table. A run cannot stop before all its rows are done. The cells must not move while a ring
  * is in flight, so nothing may insert into the table during a run.
  */

/// Measured with 100 million keys on one x86 thread: the misses in flight rise with the ring size up to
/// 32 slots and then stay flat. 16 slots cost 14% more per row than 32 slots, and 64 slots gain nothing
/// over 32. Kocberber et al. (PVLDB 2015) found that 8-10 rows in flight saturate the L1-D miss handling
/// of a core, and that past 32 the ring thrashes the TLB.
constexpr size_t amac_ring_size = 32;

/// With fewer rows than this, filling and draining the ring cost more than the overlap of the misses saves.
constexpr size_t amac_min_rows = 256;

enum class AmacStepResult : UInt8
{
    Advance, /// the row is not done: the step prefetched what the next visit of the row reads, such as the next cell
    Done, /// the row completed; the slot can be recycled
};

/** The ring driver. A policy supplies `Ring`: the state of `amac_ring_size` slots, value-initialized to
  * all-inactive, with `isActive` / `deactivate`. It also supplies `start(ring, s, row)` (seed the slot
  * and prefetch; false means the row was handled synchronously and the slot stays free) and
  * `step(ring, s)`. `amacRun` works on a copy of the policy, so a policy must keep its results behind
  * pointers or references.
  *
  * The ring state is a struct of parallel arrays, so that a wide stored key does not misalign the
  * other fields against cache lines. Keep it minimal: slot state that spills to the stack costs more
  * than the overlap wins. Recompute what the row index gives, and store a resolved address only where
  * the steady loop would resolve it again on every visit.
  */
template <typename Policy>
void amacRun(Policy & policy_arg, size_t rows)
{
    /// Every call of the policy is inlined, so the address of this copy never escapes. Its fields then
    /// become SSA values, which the stores through the result arrays cannot alias. Through the caller's
    /// reference, the compiler would reload them at every visit.
    Policy policy = policy_arg;

    typename Policy::Ring ring{};
    size_t next = 0;
    size_t active = 0;

    /// Pull rows into a slot until one enters the ring or the rows run out. Force-inlined: for the
    /// policies of the multi-column fixed keys, clang otherwise outlines this lambda. That leaks the
    /// address of the policy copy and undoes the SSA promotion above. Then the steady loop would reload
    /// every invariant at every visit, and each completed row would cost a call.
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

    /// After the ring is first filled, either every slot is active or the rows have run out.
    for (size_t s = 0; s < amac_ring_size; ++s)
    {
        refill(s);
        active += ring.isActive(s);
    }

    /// While rows remain and every refill has succeeded, every slot is active, so the steady loop has no
    /// active check. The first failed refill drops into the drain loop, which checks.
    if (active == amac_ring_size)
    {
        bool full = true;
        while (full && next < rows)
        {
            for (size_t s = 0; s < amac_ring_size; ++s)
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
        for (size_t s = 0; s < amac_ring_size; ++s)
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
