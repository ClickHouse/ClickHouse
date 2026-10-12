#pragma once

#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <memory>

namespace DB
{

/// Shared state between LazyFinalKeyAnalysisTransform and LazyReadReplacingFinalSource.
/// The transform builds the ReadFromMergeTree step (with IN-set filter and index analysis)
/// and stores it here. The source retrieves and uses it to build the internal pipeline.
struct LazyFinalSharedState
{
    /// The pre-built ReadFromMergeTree step with index analysis applied.
    /// Written by LazyFinalKeyAnalysisTransform::work(), read by LazyReadReplacingFinalSource::work().
    std::unique_ptr<ReadFromMergeTree> reading_step;
};

using LazyFinalSharedStatePtr = std::shared_ptr<LazyFinalSharedState>;

}
