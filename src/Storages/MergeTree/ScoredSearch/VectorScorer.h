#pragma once
#include "config.h"

#if USE_USEARCH

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/ScoredSearch/IScorer.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

/// `RowScorer` for vector search.
/// `scorePart` publishes distances from the vector index (currently only from USearch).
class VectorScorer final : public RowScorer
{
public:
    VectorScorer(MergeTreeIndexPtr vector_index_, VectorWithMemoryTracking<Float64> reference_vector_, size_t top_k_);

protected:
    ScoreDirection getSortDirection() const override;

    /// Checks the reference vector (dimension, `NaN`/`Inf`) against the index
    /// metadata and the search settings, without touching any part's data.
    void validate(const ContextPtr & context) const override;

    std::vector<MergeTreeIndexPtr> getIndexes() const override { return {vector_index}; }

    /// Runs a (filtered) USearch search over the part's single index granule.
    /// Output is sorted by `(distance, row_id)` according to `getSortDirection`.
    ScoreResult scorePart(const MergeTreeData::DataPartPtr & part, const Prefilter & prefilter, ContextPtr context) override;

private:
    MergeTreeIndexPtr vector_index;
    VectorWithMemoryTracking<Float64> reference_vector;
};

}

#endif
