#pragma once

#include <Interpreters/Context_fwd.h>
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

/// the stored definition rebuilt against the current table, or empty with `reason` set when an
/// ALTER since CREATE means it can no longer be added
std::optional<ProjectionDescription> refreshHypotheticalProjection(
    const ProjectionDescription & stored,
    const MergeTreeData & data,
    const StorageMetadataPtr & metadata,
    const ContextPtr & context,
    String & reason);

/// Estimate the marks a hypothetical normal projection would read for the query behind `read_step`.
/// Mirrors `evaluateIndex`: an unusable candidate becomes `not_applicable` with a reason
WhatIfCandidateResult evaluateProjection(
    const ProjectionDescription & stored_projection,
    ReadFromMergeTree * read_step,
    const ReadFromMergeTree::AnalysisResult & analysis,
    const RangesInDataParts & baseline_parts,
    const WhatIfSettings & settings,
    ContextPtr context);

}
