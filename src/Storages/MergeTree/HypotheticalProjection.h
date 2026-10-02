#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/ProjectionsDescription.h>

#include <optional>
#include <unordered_map>

namespace DB
{

/// a projection for `EXPLAIN WHATIF` that exists only as in-memory parts
/// the projection optimization weighs it as a materialized projection and records the result, but it never reads it
struct HypotheticalProjection
{
    explicit HypotheticalProjection(ProjectionDescription projection_) : projection(std::move(projection_)) {}

    ProjectionDescription projection;
    /// parent part name -> in-memory projection part
    std::unordered_map<String, MergeTreeDataPartPtr> parts;

    struct Outcome
    {
        /// the marks and the rows of the projection read, set after the analysis of the projection
        std::optional<UInt64> marks;
        UInt64 rows = 0;
        bool chosen = false;
        /// chosen only because `force_optimize_projection` or `prefer_optimize_projection` cancels a rejection
        bool forced = false;
        /// the projection order serves the ORDER BY of the query
        bool serves_order = false;
        /// the query has no filter and no ORDER BY that a projection can serve
        bool nothing_to_serve = false;
        String reason;
        /// parent part name -> the granules that the projection read takes from the projection part
        std::unordered_map<String, MarkRanges> ranges;
    };
    Outcome outcome;

    MergeTreeDataPartPtr findPart(const String & parent_part_name) const
    {
        auto it = parts.find(parent_part_name);
        return it == parts.end() ? nullptr : it->second;
    }
};

using HypotheticalProjectionPtr = std::shared_ptr<HypotheticalProjection>;

}
