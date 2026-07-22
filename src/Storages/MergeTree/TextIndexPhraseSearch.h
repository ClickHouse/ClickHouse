#pragma once

#include <Common/PODArray.h>

#include <vector>

namespace DB
{

/// Candidate-driven phrase matching over blocked positions.
struct TextIndexPhraseSearch
{
    /// Returns the candidates where some position p starts the phrase (term k at p + k for all k).
    /// `candidates`: ascending rows containing all terms. Per unique token u, per_token_positions[u]
    /// concatenates candidates' position lists, delimited by per_token_offsets[u] (candidate i:
    /// [i ? offsets[i - 1] : 0, offsets[i])). term_to_unique maps each phrase term to its token.
    static PaddedPODArray<UInt32> matchCandidatePositions(
        const PaddedPODArray<UInt32> & candidates,
        const std::vector<PaddedPODArray<UInt32>> & per_token_offsets,
        const std::vector<PaddedPODArray<UInt32>> & per_token_positions,
        const std::vector<size_t> & term_to_unique);
};

}
