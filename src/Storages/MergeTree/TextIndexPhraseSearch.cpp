#include <Storages/MergeTree/TextIndexPhraseSearch.h>

namespace DB
{

void TextIndexPhraseSearch::matchCandidatePositions(
    std::span<const UInt32> candidates,
    const std::vector<PaddedPODArray<UInt32>> & per_token_offsets,
    const std::vector<PaddedPODArray<UInt32>> & per_token_positions,
    const std::vector<size_t> & term_to_unique,
    PaddedPODArray<UInt32> & matching)
{
    if (candidates.empty() || term_to_unique.empty())
        return;

    /// Per candidate: keep the positions of term k that continue a phrase started k terms back,
    /// advancing with a two-pointer over each next term's sorted positions. A candidate matches
    /// at the first position that survives to the last term.
    std::vector<UInt32> chain;
    std::vector<UInt32> next;
    for (size_t i = 0; i < candidates.size(); ++i)
    {
        const size_t u0 = term_to_unique[0];
        chain.assign(
            per_token_positions[u0].data() + per_token_offsets[u0][i],
            per_token_positions[u0].data() + per_token_offsets[u0][i + 1]);

        bool matched = term_to_unique.size() == 1 && !chain.empty();
        for (size_t k = 1; k < term_to_unique.size(); ++k)
        {
            const size_t u = term_to_unique[k];
            const UInt32 * next_position = per_token_positions[u].data() + per_token_offsets[u][i];
            const UInt32 * const end = per_token_positions[u].data() + per_token_offsets[u][i + 1];
            const bool last = k + 1 == term_to_unique.size();
            next.clear();
            for (UInt32 position : chain)
            {
                while (next_position != end && *next_position < position + 1)
                    ++next_position;
                if (next_position == end)
                    break;
                if (*next_position == position + 1)
                {
                    if (last)
                    {
                        matched = true;
                        break;
                    }
                    next.push_back(position + 1);
                }
            }
            if (last)
                break;
            chain.swap(next);
            if (chain.empty())
                break;
        }

        if (matched)
            matching.push_back(candidates[i]);
    }
}

}
