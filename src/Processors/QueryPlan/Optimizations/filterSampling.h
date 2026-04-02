#pragma once

#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Common/SipHash.h>
#include <base/defines.h>
#include <base/types.h>

#include <mutex>
#include <optional>
#include <unordered_map>

namespace DB
{

/// Per-query cache for filter selectivity estimates computed by sampling granules.
/// Avoids redundant I/O when the same table + filter combination appears multiple
/// times in the plan (e.g., due to correlated subquery decorrelation which copies
/// the outer plan subtree).
///
/// Thread-safe: guarded by an internal mutex.
class FilterSelectivityCache
{
public:
    /// Build a cache key from the storage identity and the filter column names.
    /// The key captures "which table" and "which filter expression" without
    /// depending on the physical plan node identity.
    static UInt128 makeKey(
        const StorageID & storage_id,
        const Names & required_columns,
        const String & filter_column_name,
        const String & prewhere_column_name)
    {
        SipHash hash;
        hash.update(storage_id.database_name);
        hash.update(storage_id.table_name);
        hash.update(storage_id.uuid.toUnderType());
        hash.update(filter_column_name);
        hash.update(prewhere_column_name);
        for (const auto & col : required_columns)
            hash.update(col);
        return hash.get128();
    }

    /// Look up a cached selectivity. Returns nullopt on cache miss or if the
    /// granule count diverges more than 2x from the cached entry (safeguard
    /// against reusing an estimate from a very different set of part ranges).
    std::optional<Float64> get(UInt128 key, size_t current_total_granules) const
    {
        std::lock_guard lock(mutex);
        auto it = cache.find(key);
        if (it == cache.end())
            return std::nullopt;
        const auto & entry = it->second;
        if (current_total_granules > 2 * entry.total_granules
            || entry.total_granules > 2 * current_total_granules)
            return std::nullopt;
        return entry.selectivity;
    }

    void put(UInt128 key, Float64 selectivity, size_t total_granules)
    {
        std::lock_guard lock(mutex);
        cache.insert_or_assign(key, Entry{selectivity, total_granules});
    }

private:
    struct Entry
    {
        Float64 selectivity;
        size_t total_granules;
    };

    struct UInt128Hash
    {
        size_t operator()(UInt128 x) const
        {
            return CityHash_v1_0_2::Hash128to64({x.items[UInt128::_impl::little(0)], x.items[UInt128::_impl::little(1)]});
        }
    };

    mutable std::mutex mutex;
    std::unordered_map<UInt128, Entry, UInt128Hash> cache TSA_GUARDED_BY(mutex);
};

/// Estimate filter selectivity by sampling evenly-spaced granules across all parts
/// and ranges, evaluating the filter on each, and returning the median selectivity.
/// The median provides robustness against outlier granules.
/// Deterministic positioning ensures reproducible query plans.
///
/// Handles both a parent FilterStep expression (`filter_dag`/`filter_column_name`) and a PREWHERE
/// condition on the ReadFromMergeTree step. When both are present, both are evaluated and their
/// combined (AND) selectivity is returned.
///
/// If `analyzed_result` is provided, it is reused instead of calling `selectRangesToRead` again.
std::optional<Float64> estimateFilterSelectivity(
    const ReadFromMergeTree & read_step,
    const ActionsDAG * filter_dag = nullptr,
    const String * filter_column_name = nullptr,
    const ReadFromMergeTree::AnalysisResultPtr & analyzed_result = nullptr);

}
