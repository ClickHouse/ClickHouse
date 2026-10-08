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
    /// per_token_positions[u] concatenates the candidates' position lists, delimited by
    /// per_token_offsets[u] with a leading zero (candidate i: [offsets[i], offsets[i + 1])).
    /// term_to_unique maps each phrase term to its token.
    static void matchCandidatePositions(
        std::span<const UInt32> candidates,
        const std::vector<PaddedPODArray<UInt32>> & per_token_offsets,
        const std::vector<PaddedPODArray<UInt32>> & per_token_positions,
        const std::vector<size_t> & term_to_unique,
        PaddedPODArray<UInt32> & matching);
};

}
