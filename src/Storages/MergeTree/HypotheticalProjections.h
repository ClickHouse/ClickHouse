#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/ProjectionsDescription.h>

#include <optional>
#include <unordered_map>

namespace DB
{

/// projections for `EXPLAIN WHATIF` that exist only as in-memory parts
/// the projection optimization weighs them as materialized projections and records the result, but it never reads them
struct HypotheticalProjections
{
    explicit HypotheticalProjections(ProjectionDescription projection_) : projection(std::move(projection_)) {}

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
    };
    Outcome outcome;

    MergeTreeDataPartPtr findPart(const String & parent_part_name) const
    {
        auto it = parts.find(parent_part_name);
        return it == parts.end() ? nullptr : it->second;
    }
};

using HypotheticalProjectionsPtr = std::shared_ptr<HypotheticalProjections>;

}
