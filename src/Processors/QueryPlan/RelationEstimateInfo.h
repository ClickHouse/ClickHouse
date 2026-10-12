#pragma once

#include <optional>
#include <string_view>
#include <base/types.h>
#include <fmt/format.h>

namespace DB
{

/// Where the row count estimate used by join reordering came from.
enum class RowEstimateSource : UInt8
{
    /// The origin of the estimate was not tracked (e.g. it was produced by an already-optimized sub-plan).
    NoSource,
    /// Real column statistics (or an exact row count).
    Statistics,
    /// Estimated from the primary index because column statistics are missing.
    PrimaryIndex,
    /// No estimate could be derived at all while column statistics are missing.
    NoStatistics,
    /// Synthetic estimate from the `_internal_join_table_stat_hints` query parameter (testing only).
    Hint,
    /// Randomized estimate produced for join-reordering stress testing (testing only).
    Randomized,
    /// Measured row count reused from a previous run's hash table.
    HashTableCache,
};

/// Imprecise specifically because column statistics are missing (excludes the synthetic test sources).
constexpr bool isMissingStatisticsSource(RowEstimateSource source)
{
    return source == RowEstimateSource::PrimaryIndex
        || source == RowEstimateSource::NoStatistics;
}

/// EXPLAIN prefix for the row count, e.g. `no_stats` in `a[no_stats~1000]`; empty for precise sources.
constexpr std::string_view rowEstimateSourceTag(RowEstimateSource source)
{
    switch (source)
    {
        case RowEstimateSource::PrimaryIndex:
        case RowEstimateSource::NoStatistics:
            return "no_stats";
        case RowEstimateSource::Hint:
            return "hint";
        case RowEstimateSource::Randomized:
            return "random";
        case RowEstimateSource::HashTableCache:
            return "cache";
        case RowEstimateSource::NoSource:
        case RowEstimateSource::Statistics:
            return "";
    }
    return "";
}

/// One join input relation as shown in EXPLAIN: the display name plus the row estimate and its
/// origin. Kept as structured data rather than a pre-formatted string so the rendering can be
/// changed independently of the join-reordering code that produces the values.
struct RelationEstimateInfo
{
    /// Table name or alias; for a relation that is itself a join, a readable chain of its inputs.
    String name;
    std::optional<UInt64> estimated_rows = {};
    RowEstimateSource source = RowEstimateSource::NoSource;
    bool imprecise_estimate = false;
    /// A relation composed of sub-joins is rendered as the chain only, without its own estimate.
    bool composite = false;

    /// `name[source~rows]`, e.g. `a[no_stats~1000]`, `b[100]`, `c[hint~?]`.
    String displayName() const
    {
        if (composite)
            return name;

        std::string_view tag = rowEstimateSourceTag(source);
        /// The estimate origin is not tracked through sub-plans; label imprecise ones with the generic tag.
        if (tag.empty() && imprecise_estimate)
            tag = "no_stats";

        if (estimated_rows)
            return fmt::format("{}[{}{}{}]", name, tag, tag.empty() ? "" : "~", *estimated_rows);
        if (!tag.empty())
            return fmt::format("{}[{}~?]", name, tag);
        return name;
    }
};

}
