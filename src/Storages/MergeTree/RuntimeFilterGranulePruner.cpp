#include <Storages/MergeTree/RuntimeFilterGranulePruner.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Common/Exception.h>

namespace DB
{

RuntimeFilterGranulePruner::RuntimeFilterGranulePruner(
    std::vector<RuntimeFilterPKInfo> filters_,
    RuntimeFilterLookupPtr runtime_filter_lookup_,
    DataTypes pk_data_types_)
    : filters(std::move(filters_))
    , runtime_filter_lookup(std::move(runtime_filter_lookup_))
    , pk_data_types(std::move(pk_data_types_))
{
}

void RuntimeFilterGranulePruner::tryInitialize() const
{
    if (is_initialized.load(std::memory_order_acquire))
        return;

    std::lock_guard lock(init_mutex);
    if (is_initialized.load(std::memory_order_relaxed))
        return;

    bool all_filters_resolved = true;
    std::vector<InitializedFilter> new_filters;

    for (const auto & filter_info : filters)
    {
        auto runtime_filter = runtime_filter_lookup->find(filter_info.filter_name);
        if (!runtime_filter)
        {
            all_filters_resolved = false;
            continue;
        }

        auto exact_column = runtime_filter->getExactSetColumn();
        if (!exact_column)
        {
            if (!runtime_filter->insertsAreFinished())
            {
                /// Inserts not finished yet — retry later.
                all_filters_resolved = false;
            }
            /// Otherwise inserts are finished but exact set is unavailable
            /// (e.g., set overflowed or negated filter). This is permanent — skip this filter.
            continue;
        }

        if (exact_column->empty())
            continue;

        /// Build `MergeTreeSetIndex` from the exact set values.
        /// The set has a single "tuple element" (the filter key column)
        /// that maps to a specific position in the primary key.
        MergeTreeSetIndex::KeyTuplePositionMapping mapping;
        mapping.tuple_index = 0;
        mapping.key_index = filter_info.key_index_in_pk;

        try
        {
            auto set_index = std::make_shared<MergeTreeSetIndex>(
                Columns{exact_column},
                std::vector<MergeTreeSetIndex::KeyTuplePositionMapping>{mapping});

            new_filters.push_back({std::move(set_index), filter_info.key_index_in_pk});
        }
        catch (const Exception & e)
        {
            /// If we can't build the set index (e.g., incompatible types),
            /// just skip this filter. `PREWHERE` will still do row-level filtering.
            LOG_DEBUG(log, "Could not build MergeTreeSetIndex for runtime filter '{}': {}",
                filter_info.filter_name, e.message());
        }
    }

    /// Only publish `initialized_filters` and mark as initialized once all filters
    /// have been resolved. This avoids data races: `shouldSkipTask` only reads
    /// `initialized_filters` after seeing `is_initialized == true` (acquire/release).
    if (all_filters_resolved)
    {
        initialized_filters = std::move(new_filters);
        is_initialized.store(true, std::memory_order_release);
    }
}

bool RuntimeFilterGranulePruner::shouldSkipTask(const MergeTreeReadTask & task) const
{
    tryInitialize();

    if (!is_initialized.load(std::memory_order_acquire))
        return false;

    if (initialized_filters.empty())
        return false;

    const auto & info = task.getInfo();
    const auto & part = info.data_part;
    const auto & mark_ranges = task.getMarkRanges();

    if (mark_ranges.empty())
        return true;

    const auto index = part->getIndex();
    if (!index || index->empty())
        return false;

    const size_t pk_size = pk_data_types.size();

    /// Pre-build a base key range template with whole-universe for all PK columns.
    /// Only the filter's column gets overwritten per mark range.
    std::vector<Range> key_ranges(pk_size, Range::createWholeUniverse());

    /// For each initialized filter, check if ANY mark range could contain matching values.
    /// If any single filter proves "no marks match", we can skip the entire task
    /// (AND semantics: runtime filters are always conjunctive, see `joinRuntimeFilter.cpp`).
    for (const auto & filter : initialized_filters)
    {
        if (filter.key_index_in_pk >= index->size())
            continue;

        const auto & index_column = (*index)[filter.key_index_in_pk];
        const size_t index_size = index_column->size();

        bool any_range_may_match = false;

        for (const auto & range : mark_ranges)
        {
            if (range.begin >= index_size)
                continue;

            /// Fill in the actual PK index bounds for this mark range.
            /// Uses the same boundary convention as `MergeTreeDataSelectExecutor::markRangesFilterPK`:
            /// left = index value at `range.begin`, right = index value at `range.end`
            /// (which is the first mark of the next range, i.e. exclusive),
            /// or `POSITIVE_INFINITY` when `range.end` reaches the end of the index.
            Field left_val;
            index_column->get(range.begin, left_val);
            if (left_val.isNull())
                left_val = NEGATIVE_INFINITY;

            Field right_val = POSITIVE_INFINITY;
            if (range.end < index_size)
            {
                index_column->get(range.end, right_val);
                if (right_val.isNull())
                    right_val = POSITIVE_INFINITY;
            }

            key_ranges[filter.key_index_in_pk] = Range(left_val, true, right_val, true);

            auto result = filter.set_index->checkInRange(key_ranges, pk_data_types);
            if (result.can_be_true)
            {
                any_range_may_match = true;
                break;
            }
        }

        /// Restore whole-universe for the next filter iteration.
        key_ranges[filter.key_index_in_pk] = Range::createWholeUniverse();

        if (!any_range_may_match)
            return true;
    }

    return false;
}

}
