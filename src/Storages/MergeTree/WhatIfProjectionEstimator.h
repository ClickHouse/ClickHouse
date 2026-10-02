#pragma once

#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/WhatIfResult.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <functional>
#include <optional>

namespace DB
{

class MergeTreeData;
struct ProjectionDescription;
struct WhatIfSettings;
struct HypotheticalProjections;

/// plans the query again so that the optimizer weighs these hypothetical projections and records their results
using WeighHypotheticalProjections = std::function<void(const std::shared_ptr<HypotheticalProjections> &)>;

/// re-validate a stored definition, empty with a reason if it no longer fits
std::optional<ProjectionDescription> refreshHypotheticalProjection(
    const ProjectionDescription & stored,
    const MergeTreeData & data,
    const StorageMetadataPtr & metadata,
    const ContextPtr & context,
    String & reason);

/// like evaluateIndex, for a hypothetical projection
/// `force_requested` makes the verdict name `force_optimize_projection`, which the statement plans as `prefer_optimize_projection`
WhatIfCandidateResult evaluateProjection(
    const ProjectionDescription & stored_projection,
    ReadFromMergeTree * read_step,
    const ReadFromMergeTree::AnalysisResult & analysis,
    const RangesInDataParts & baseline_parts,
    const WhatIfSettings & settings,
    bool force_requested,
    const WeighHypotheticalProjections & weigh,
    ContextPtr context);

}
