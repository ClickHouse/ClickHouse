#pragma once

#include <Core/Field.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context_fwd.h>

#include <optional>
#include <vector>

namespace DB
{

/// Conditions with time bounds in the query condition cache (issues #78502 and #115504).
///
/// Dashboards repeat a query with time bounds that move a little with every refresh, given either as
/// literals or relative to the current time:
///
///     WHERE time >= fromUnixTimestamp64Nano(1743410504651000000) AND time <= fromUnixTimestamp64Nano(1743493304654000000)
///         AND message LIKE '%error%'
///
///     WHERE time >= now() - INTERVAL 1 HOUR AND message LIKE '%error%'
///
/// In the latter, `now()` reaches the storage layer folded into a constant that is marked
/// non-deterministic. Such a condition is still cached under its own hash, which includes the value of
/// the constant: entries written for it state that no rows of a granule match the condition with this
/// particular value, which stays true. Other non-determinism (e.g. `rand()`) is not allowed.
///
/// Every variant of such a condition has a hash of its own, so a refresh never finds the entries
/// written by the previous one. They can still be used: if no row of a granule matches the
/// earlier variant, then a row matching the new variant must be admitted by some time comparison of the
/// new variant but not by the same comparison of the earlier variant (e.g. a row with `time` between
/// the earlier and the new upper bound). Call the set of such rows the band. If the primary key shows
/// that a granule has no rows in the band, it has no rows matching the new variant either.
///
/// This holds for any condition that is monotone in each of its time comparisons: AND, OR and NOT
/// (which flips the direction) over comparisons of a deterministic expression with a constant of a
/// date/time type, and over arbitrary deterministic subconditions, which are kept as they are.
/// Deterministic and non-deterministic constants are treated the same, so a variant with `now()` can
/// use the entries of a variant with a literal and vice versa.
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

/// Returns std::nullopt if `condition` has no time comparisons outside of the subconditions which are
/// kept as they are, or if it is non-deterministic in another way than by folded constants of the time
/// comparisons. Such a constant has to be of a date/time type, so that its value is part of the hash.
std::optional<TimeBoundsCondition> abstractTimeBounds(const ActionsDAG::Node * condition);

/// Whether `condition` is not deterministic but may be cached under its own hash anyway, because its
/// only non-deterministic parts are the folded constants of time comparisons (see `abstractTimeBounds`).
bool isDeterministicExceptTimeBounds(const ActionsDAG::Node * condition);

/// Build a DAG with a single output that is true for the rows in the band between `condition` and its
/// variant with the values `earlier_values` (see `TimeBoundsCondition`): the rows that may match
/// `condition` but not the earlier variant. Returns std::nullopt if there are no such rows, i.e. the
/// earlier variant is implied by `condition`.
std::optional<ActionsDAG> buildTimeBoundsBand(
    const TimeBoundsCondition & condition, const std::vector<Field> & earlier_values, const ContextPtr & context);

}
