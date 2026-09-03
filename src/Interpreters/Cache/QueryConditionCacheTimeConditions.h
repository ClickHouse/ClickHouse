#pragma once

#include <Interpreters/ActionsDAG.h>

#include <optional>

namespace DB
{

/// Deriving deterministic conditions from conditions involving the current time, for the query
/// condition cache (issue #115504).
///
/// A condition like
///
///     time >= now() - INTERVAL 10 DAY
///
/// reaches the storage layer with the current-time expression folded into a constant that is marked
/// non-deterministic. Hashing it yields a different value for every query (the constant changes with
/// the clock), so such conditions never benefit from the query condition cache.
///
/// However, a deterministic condition can be derived from it by rounding the non-deterministic
/// constant onto a time grid:
///
///     time >= '2026-08-09 12:34:56'  ~~~>  time >= '2026-08-09 00:00:00'  (rounded down)
///                                          time >= '2026-08-10 00:00:00'  (rounded up)
///
/// The grid step is proportional to the distance between the constant and the current time
/// (10 days above), multiplied by a configured factor and capped at one day. All queries whose
/// constants land in the same grid cell derive the same condition and therefore the same cache key,
/// which balances cache usefulness against staleness of the boundary: the derived boundary lags the
/// real one by at most one grid step, and the key naturally rotates once per grid step.
///
/// The rounding direction makes the derived condition comparable to the original one by implication,
/// which is what makes using it as a cache key sound (see TimeConditionRounding).
enum class TimeConditionRounding : uint8_t
{
    /// The derived condition is implied by the original condition: every row that matches the
    /// original condition also matches the derived one. Therefore "no rows of this granule match
    /// the derived condition" implies "no rows match the original condition", and it is sound to
    /// *read* cache entries stored under the derived condition and skip the corresponding granules.
    Weaken,

    /// The derived condition implies the original condition: every row that matches the derived
    /// condition also matches the original one. Therefore "no rows of this granule match the
    /// original condition" (which is what query execution observes) implies "no rows match the
    /// derived condition", and it is sound to *write* cache entries under the derived condition.
    Strengthen,
};

/// The two directions meet as follows. When the folded constant is already aligned to the grid
/// (e.g. `today() - 10` or `toStartOfDay(now())`), rounding up and down both return the constant
/// unchanged, so reads and writes share the key immediately: the cache is effective for the whole
/// grid cell (e.g. the whole day). When the constant is not aligned (e.g. `now() - INTERVAL 10 DAY`
/// with second precision), writes go to the grid point above the constant and reads probe the grid
/// point below it, so entries written during one grid cell are consumed during the next one (e.g.
/// yesterday's queries prime the cache for today's).
struct DeterministicTimeCondition
{
    UInt64 hash;      /// Hash of the derived condition, to be used as the condition hash in the query condition cache.
    String condition; /// Human-readable rendering of the derived condition, for logs and (in debug builds) system.query_condition_cache.
};

/// Try to derive a deterministic condition from `condition` by rounding non-deterministic constants
/// of date/time types onto a time grid, in the direction given by `rounding`.
///
/// Returns std::nullopt if
///   * the condition is already deterministic (no derivation is needed - use the condition itself), or
///   * the condition contains non-determinism that cannot be soundly rounded away: non-deterministic
///     constants of non-temporal types, non-deterministic constants outside of monotone comparison
///     positions (only AND, OR, NOT and comparisons of a deterministic expression with a constant
///     are understood), or non-deterministic functions, or
///   * `grid_factor` is not positive, or the resulting grid step would be below one second.
///
/// `current_time` is only used to choose the grid step; it does not affect soundness, only the
/// likelihood that independent derivations (e.g. the write side of one query and the read side of a
/// later query) pick the same grid and therefore the same cache key.
///
/// `allow_top_k_filter` treats the internal `__topKFilter` function - which TopK dynamic filtering
/// folds into the storage filter as `and(__topKFilter(...), <predicate>)` - as an opaque
/// deterministic leaf, mirroring `isDeterministicAllowingTopKFilter` in `updateQueryConditionCache`
/// and `ReadFromMergeTree`. Without it, a TopK read of a current-time condition would derive nothing
/// and bypass the cache entirely. Only pass `true` where the cache key is additionally partitioned by
/// the TopK plan parameters, because granule exclusions produced under a running `__topKFilter`
/// threshold may only be reused under the same TopK plan.
std::optional<DeterministicTimeCondition> deriveDeterministicTimeCondition(
    const ActionsDAG::Node * condition,
    TimeConditionRounding rounding,
    double grid_factor,
    time_t current_time,
    bool allow_top_k_filter);

}
