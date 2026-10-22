#pragma once

#include <Core/Field.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context_fwd.h>

#include <optional>
#include <vector>

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
/// (10 days above), multiplied by a configured factor and clamped to the range from one second to
/// one day. All queries whose
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
///
/// Reusing an entry requires `original condition of the reader` => `derived condition` =>
/// `original condition of the writer`. A sliding window bounded by the current time on both sides
/// (`time >= now() - INTERVAL 10 DAY AND time < now()`) shifts both bounds forward, so the later window
/// is never contained in the earlier one: no choice of grid lets such conditions share a key unless
/// both constants are aligned to the grid (`time >= today() - 10 AND time < today()`). A grid shared
/// by all constants of a condition would only make the derived bounds staler, so the grid step is
/// chosen per constant.
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
///   * `grid_factor` is not positive (which disables the derivation).
///
/// `current_time` is only used to choose the grid step; it does not affect soundness, only the
/// likelihood that independent derivations (e.g. the write side of one query and the read side of a
/// later query) pick the same grid and therefore the same cache key.
///
/// The internal `__topKFilter` function is non-deterministic, so a condition containing it derives
/// nothing. This is intended: granules dropped by a TopK read depend on the running threshold, i.e.
/// on the whole set of rows matching the condition. Two derivations that share a key may stand for
/// different row sets (a later query's condition can match a subset of an earlier query's rows), and
/// the top N of a subset may lie in granules that the earlier query excluded. So a TopK read must not
/// write or consult entries under a derived condition partitioned by the TopK plan; it may only
/// consult entries written by plain reads of the predicate.
std::optional<DeterministicTimeCondition> deriveDeterministicTimeCondition(
    const ActionsDAG::Node * condition,
    TimeConditionRounding rounding,
    double grid_factor,
    time_t current_time);

/// Reusing cache entries across changes of time bounds (issue #78502).
///
/// Dashboards repeat a query with time bounds given as literals that move a little with every refresh:
///
///     WHERE time >= fromUnixTimestamp64Nano(1743410504651000000) AND time <= fromUnixTimestamp64Nano(1743493304654000000)
///         AND message LIKE '%error%'
///
/// Every variant is a deterministic condition with a hash of its own, so a refresh never finds the
/// entries written by the previous one. They can still be used: if no row of a granule matches the
/// earlier variant, then a row matching the new variant must be admitted by some time comparison of the
/// new variant but not by the same comparison of the earlier variant (e.g. a row with `time` between
/// the earlier and the new upper bound). Call the set of such rows the band. If the primary key shows
/// that a granule has no rows in the band, it has no rows matching the new variant either.
///
/// This holds for any condition that is monotone in each of its time comparisons: AND, OR and NOT
/// (which flips the direction) over comparisons of a deterministic expression with a constant of a
/// date/time type, and over arbitrary deterministic subconditions, which are kept as they are.
struct TimeBoundsCondition
{
    struct Bound
    {
        /// The compared deterministic expression, in the DAG of the condition.
        const ActionsDAG::Node * expression = nullptr;
        /// `less`, `lessOrEquals`, `greater` or `greaterOrEquals`, with `expression` as the first argument.
        String function_name;
        /// The constant.
        DataTypePtr type;
        Field value;
        /// The comparison is under an odd number of NOTs, i.e. the condition is antitone in it.
        bool negated = false;
    };

    /// Hash of the condition with the values of the constants of `bounds` left out. Variants of the
    /// condition that differ only in these values have the same hash.
    UInt64 structure_hash = 0;
    std::vector<Bound> bounds;

    std::vector<Field> values() const;
};

/// Returns std::nullopt if `condition` is not deterministic, or has no time comparisons outside of the
/// subconditions which are kept as they are.
std::optional<TimeBoundsCondition> abstractTimeBounds(const ActionsDAG::Node * condition);

/// Build a DAG with a single output that is true for the rows in the band between `condition` and its
/// variant with the values `earlier_values` (see `TimeBoundsCondition`): the rows that may match
/// `condition` but not the earlier variant. Returns std::nullopt if there are no such rows, i.e. the
/// earlier variant is implied by `condition`.
std::optional<ActionsDAG> buildTimeBoundsBand(
    const TimeBoundsCondition & condition, const std::vector<Field> & earlier_values, const ContextPtr & context);

}
