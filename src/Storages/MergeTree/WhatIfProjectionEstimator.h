#pragma once

#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/WhatIfResult.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <optional>

namespace DB
{

class MergeTreeData;
struct ProjectionDescription;
struct WhatIfSettings;

/// re-validate a stored definition, empty with a reason if it no longer fits
std::optional<ProjectionDescription> refreshHypotheticalProjection(
    const ProjectionDescription & stored,
    const MergeTreeData & data,
    const StorageMetadataPtr & metadata,
    const ContextPtr & context,
    String & reason);

/// like evaluateIndex, for a hypothetical projection
WhatIfCandidateResult evaluateProjection(
    const ProjectionDescription & stored_projection,
    ReadFromMergeTree * read_step,
    const ReadFromMergeTree::AnalysisResult & analysis,
    const RangesInDataParts & baseline_parts,
    const WhatIfSettings & settings,
    QueryPlan::Node * plan_root,
    ContextPtr context);

}
