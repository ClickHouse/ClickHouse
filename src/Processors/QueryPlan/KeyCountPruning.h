#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Common/Logger_fwd.h>

#include <optional>

namespace DB
{

/// The selected ranges of `parts` without the granules in which no value of the first primary key column can have
/// `min_count` rows of the read, or `std::nullopt` if no granule is skipped.
std::optional<RangesInDataParts> pruneRangesByKeyCountLowerBound(
    const RangesInDataParts & parts, UInt64 min_count, const ContextPtr & context, const LoggerPtr & log);

}
