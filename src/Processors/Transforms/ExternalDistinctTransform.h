#pragma once

#include <Processors/Transforms/DistinctSetFilter.h>
#include <Processors/Transforms/ExternalGroupLimitTransform.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event ExternalDistinctMerge;
    extern const Event ExternalDistinctTailSpilledRows;
    extern const Event ExternalDistinctTailKeptRows;
}

namespace DB
{

/// The parts of external `DISTINCT` that differ from external `LIMIT BY`: a set keeps the first row of each
/// key, and the processors and messages name `DISTINCT`.
struct ExternalDistinctTraits
{
    using Filter = DistinctSetFilter;

    static constexpr const char * transform_name = "ExternalDistinctTransform";
    static constexpr const char * merge_transform_name = "DistinctSortedTransform";
    static constexpr const char * operator_name = "DISTINCT";

    static Filter createFilter(const Block & header, const GroupLimitParams & params, const IProcessor & /*owner*/)
    {
        return DistinctSetFilter(header, params.key_columns, params.size_limits);
    }

    static bool isLimitReached(const Filter & filter) { return filter.isLimitReached(); }

    static ProfileEvents::Event mergeEvent() { return ProfileEvents::ExternalDistinctMerge; }
    static ProfileEvents::Event tailSpilledRowsEvent() { return ProfileEvents::ExternalDistinctTailSpilledRows; }
    static ProfileEvents::Event tailKeptRowsEvent() { return ProfileEvents::ExternalDistinctTailKeptRows; }
};

extern template class ExternalGroupLimitTransform<ExternalDistinctTraits>;

/// The final hash-based `DISTINCT` that spills to disk: the window `[0, 1)` of each group.
using ExternalDistinctTransform = ExternalGroupLimitTransform<ExternalDistinctTraits>;

}
