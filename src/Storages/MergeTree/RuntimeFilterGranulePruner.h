#pragma once

#include <atomic>
#include <mutex>
#include <vector>

#include <Core/Range.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Set.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Common/logger_useful.h>

namespace DB
{

struct MergeTreeReadTask;

/// Describes a single runtime filter that can be used for PK granule pruning.
struct RuntimeFilterPKInfo
{
    String filter_name;
    size_t key_index_in_pk; /// position of this column in the primary key
};

/// Prunes mark ranges at execution time using runtime filter exact sets.
///
/// When a hash join builds a runtime filter (exact set of key values from the right side),
/// this pruner uses those values to skip granules on the left side that cannot contain
/// matching rows, based on the primary key index.
///
/// The pruner is created during query plan optimization with metadata about which runtime
/// filters map to which PK columns. The actual pruning happens lazily during execution
/// (`MergeTreeSelectProcessor::read`), when the runtime filter is guaranteed to be ready
/// (right side of hash join completes before left side starts reading).
class RuntimeFilterGranulePruner
{
public:
    RuntimeFilterGranulePruner(
        std::vector<RuntimeFilterPKInfo> filters_,
        RuntimeFilterLookupPtr runtime_filter_lookup_,
        DataTypes pk_data_types_);

    /// Check if ALL mark ranges in the given task can be skipped.
    /// Returns true if the runtime filter proves that no rows in any of the
    /// task's mark ranges can match the join condition.
    /// Thread-safe (uses internal locking for lazy initialization).
    bool shouldSkipTask(const MergeTreeReadTask & task) const;

private:
    struct InitializedFilter
    {
        std::shared_ptr<MergeTreeSetIndex> set_index;
        size_t key_index_in_pk;
    };

    void tryInitialize() const;

    std::vector<RuntimeFilterPKInfo> filters;
    RuntimeFilterLookupPtr runtime_filter_lookup;
    DataTypes pk_data_types;

    mutable std::atomic<bool> is_initialized{false};
    mutable std::mutex init_mutex;
    mutable std::vector<InitializedFilter> initialized_filters;

    LoggerPtr log = getLogger("RuntimeFilterGranulePruner");
};

using RuntimeFilterGranulePrunerPtr = std::shared_ptr<const RuntimeFilterGranulePruner>;

}
