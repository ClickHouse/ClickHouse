#pragma once

#include <cmath>
#include <limits>
#include <optional>
#include <string_view>
#include <base/types.h>
#include <fmt/format.h>

namespace DB
{

/// A row or byte count from floating point arithmetic, rounded to the nearest integer: NaN and
/// negative values are 0, values at or above 2^64 saturate at the largest integer. A conversion
/// through a signed `llround` would fold the upper half of the range into 2^63.
inline UInt64 roundToRowCount(Float64 value)
{
    if (std::isnan(value) || value <= 0)
        return 0;
    const Float64 rounded = std::round(value);
    if (rounded >= Float64(std::numeric_limits<UInt64>::max()))
        return std::numeric_limits<UInt64>::max();
    return static_cast<UInt64>(rounded);
}

/// The same conversion rounded up, for a quantity that must not understate (bytes against a budget).
inline UInt64 ceilToRowCount(Float64 value)
{
    if (std::isnan(value) || value <= 0)
        return 0;
    const Float64 rounded = std::ceil(value);
    if (rounded >= Float64(std::numeric_limits<UInt64>::max()))
        return std::numeric_limits<UInt64>::max();
    return static_cast<UInt64>(rounded);
}

/// Row arithmetic that saturates at the largest integer instead of wrapping.
inline UInt64 saturatingAdd(UInt64 a, UInt64 b)
{
    UInt64 result = 0;
    return __builtin_add_overflow(a, b, &result) ? std::numeric_limits<UInt64>::max() : result;
}

inline UInt64 saturatingMul(UInt64 a, UInt64 b)
{
    UInt64 result = 0;
    return __builtin_mul_overflow(a, b, &result) ? std::numeric_limits<UInt64>::max() : result;
}

/// Where the row count estimate used by join reordering came from. `NoSource` stays first:
/// `CostEstimationInfo.h` declares the enum opaque with zero as its default.
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
    /// Row count from data lake metadata (Iceberg manifest files).
    DataLakeMetadata,
};

/// A serialized plan carries the source as its underlying value; `DataLakeMetadata` is the last enumerator.
constexpr bool isRowEstimateSource(UInt8 value)
{
    return value <= static_cast<UInt8>(RowEstimateSource::DataLakeMetadata);
}

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
        /// Labelled like MergeTree: no tag when exact, `no_stats` from `displayName` when imprecise.
        case RowEstimateSource::DataLakeMetadata:
            return "";
    }
    return "";
}

/// Name of the estimate origin for `system.processors_profile_log`; empty for `NoSource`.
constexpr std::string_view rowEstimateSourceName(RowEstimateSource source)
{
    switch (source)
    {
        case RowEstimateSource::NoSource:
            return "";
        case RowEstimateSource::Statistics:
            return "statistics";
        case RowEstimateSource::PrimaryIndex:
            return "primary_index";
        case RowEstimateSource::NoStatistics:
            return "no_statistics";
        case RowEstimateSource::Hint:
            return "hint";
        case RowEstimateSource::Randomized:
            return "randomized";
        case RowEstimateSource::HashTableCache:
            return "hash_table_cache";
        case RowEstimateSource::DataLakeMetadata:
            return "data_lake_metadata";
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
    /// Rows the relation cannot exceed; shown when there is no estimate.
    std::optional<UInt64> max_rows = {};
    RowEstimateSource source = RowEstimateSource::NoSource;
    bool imprecise_estimate = false;
    /// A relation composed of sub-joins is rendered as the chain only, without its own estimate.
    bool composite = false;

    /// `name[source~rows]`, e.g. `a[no_stats~1000]`, `b[100]`; without an estimate the bound,
    /// `c[no_stats<=1000]`, or `c[hint~?]` when there is none.
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
        if (max_rows)
            return fmt::format("{}[{}<={}]", name, tag, *max_rows);
        if (!tag.empty())
            return fmt::format("{}[{}~?]", name, tag);
        return name;
    }
};

}
