#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/ProjectionsDescription.h>

#include <list>
#include <optional>
#include <unordered_map>

namespace DB
{

/// Projections that exist only as in-memory parts, for `EXPLAIN WHATIF`. The projection optimization
/// weighs them like materialized ones and records how each fared, but never reads one.
struct HypotheticalProjections
{
    std::list<ProjectionDescription> projections;
    /// parent part name -> projection name -> in-memory projection part
    std::unordered_map<String, std::unordered_map<String, MergeTreeDataPartPtr>> parts;

    struct Outcome
    {
        /// marks the projection read would take, set once the projection was analyzed
        std::optional<UInt64> marks;
        bool chosen = false;
        /// chosen only because `force_optimize_projection` or `prefer_optimize_projection` lifted a rejection
        bool forced = false;
        String reason;
    };
    std::unordered_map<String, Outcome> outcomes;

    bool contains(const ProjectionDescription * projection) const
    {
        for (const auto & own : projections)
            if (&own == projection)
                return true;
        return false;
    }

    MergeTreeDataPartPtr findPart(const String & parent_part_name, const String & projection_name) const
    {
        auto part_it = parts.find(parent_part_name);
        if (part_it == parts.end())
            return nullptr;
        auto it = part_it->second.find(projection_name);
        return it == part_it->second.end() ? nullptr : it->second;
    }
};

using HypotheticalProjectionsPtr = std::shared_ptr<HypotheticalProjections>;

}
