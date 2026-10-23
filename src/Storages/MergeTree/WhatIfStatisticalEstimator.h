#pragma once

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/WhatIfResult.h>

namespace DB
{

struct ProjectionDescription;

/// Estimate skip ratio from column statistics (row-level selectivity as upper bound)
bool tryEstimateWithStatistics(
    WhatIfCandidateResult & result,
    const MergeTreeIndexPtr & index_helper,
    ReadFromMergeTree * read_step,
    const ReadFromMergeTree::AnalysisResult & analysis,
    const RangesInDataParts & parts,
    const ActionsDAG::Node * filter_node,
    ContextPtr context);

/// estimate the marks and rows of a normal projection from the column statistics of the first column of its key, with no verdict
bool tryEstimateProjectionWithStatistics(
    WhatIfCandidateResult & result,
    const ProjectionDescription & projection,
    ReadFromMergeTree * read_step,
    const RangesInDataParts & parts,
    ContextPtr context);

}
