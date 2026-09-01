#pragma once

#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/WhatIfIndexEstimator.h>

namespace DB
{

/// Build the candidate index in memory over the baseline marks and check each granule
bool tryEstimateEmpirical(
    WhatIfIndexEstimator::IndexResult & result,
    const MergeTreeIndexPtr & index_helper,
    const MergeTreeIndexConditionPtr & condition,
    ReadFromMergeTree * read_step,
    const ReadFromMergeTree::AnalysisResult & analysis,
    const RangesInDataParts & saved_parts,
    std::vector<UInt8> * surviving_marks,
    ContextPtr context);

}
