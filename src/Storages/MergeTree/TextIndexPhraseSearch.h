#pragma once

#include <Common/PODArray.h>

#include <span>
#include <vector>

namespace DB
{

/// Candidate-driven phrase matching over blocked positions.
struct TextIndexPhraseSearch
{
    /// Appends to `matching` the candidates where some position p starts the phrase (term k at
    /// p + k for all k). `candidates`: ascending rows containing all terms. Per unique token u,
    /// per_token_lanes[u] holds the candidates' position lists (first position, then gaps): candidate i's
    /// non-empty list is [per_token_bounds[u][2 i], per_token_bounds[u][2 i + 1]).
    /// term_to_unique maps each phrase term to its token.
    static void matchCandidatePositions(
        std::span<const UInt32> candidates,
        const std::vector<PaddedPODArray<UInt64>> & per_token_bounds,
        const std::vector<PaddedPODArray<UInt32>> & per_token_lanes,
        const std::vector<size_t> & term_to_unique,
        PaddedPODArray<UInt32> & matching);
};

}
