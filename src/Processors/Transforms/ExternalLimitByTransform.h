#pragma once

#include <Processors/Transforms/ExternalGroupLimitTransform.h>
#include <Processors/Transforms/LimitByGroupFilter.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event ExternalLimitByMerge;
    extern const Event ExternalLimitByTailSpilledRows;
    extern const Event ExternalLimitByTailKeptRows;
}

namespace DB
{

/// The parts of external `LIMIT BY` that differ from external `DISTINCT`: the groups count their rows, and the
/// processors and messages name `LIMIT BY`.
struct ExternalLimitByTraits
{
    using Filter = LimitByGroupFilter;

    static constexpr const char * transform_name = "ExternalLimitByTransform";
    static constexpr const char * merge_transform_name = "LimitBySortedTransform";
    static constexpr const char * operator_name = "LIMIT BY";

    static Filter createFilter(const Block & header, const GroupLimitParams & params, const IProcessor & owner)
    {
        return LimitByGroupFilter(
            header, params.key_columns, params.group_offset, params.group_limit_end, owner, /*keep_fingerprints_=*/ true);
    }

    /// `LIMIT BY` has no size limits on its grouping state.
    static bool isLimitReached(const Filter &) { return false; }

    static ProfileEvents::Event mergeEvent() { return ProfileEvents::ExternalLimitByMerge; }
    static ProfileEvents::Event tailSpilledRowsEvent() { return ProfileEvents::ExternalLimitByTailSpilledRows; }
    static ProfileEvents::Event tailKeptRowsEvent() { return ProfileEvents::ExternalLimitByTailKeptRows; }
};

extern template class ExternalGroupLimitTransform<ExternalLimitByTraits>;

/// The hash-based `LIMIT n OFFSET m BY` that spills to disk: the window `[m, m + n)` of each group.
using ExternalLimitByTransform = ExternalGroupLimitTransform<ExternalLimitByTraits>;

}
