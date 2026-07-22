#include <Storages/MergeTree/TextIndexPhraseSearch.h>

#include <utility>

namespace DB
{

PaddedPODArray<UInt32> TextIndexPhraseSearch::matchCandidatePositions(
    const PaddedPODArray<UInt32> & candidates,
    const std::vector<PaddedPODArray<UInt32>> & per_token_offsets,
    const std::vector<PaddedPODArray<UInt32>> & per_token_positions,
    const std::vector<size_t> & term_to_unique)
{
    PaddedPODArray<UInt32> matching;
    if (candidates.empty() || term_to_unique.empty())
        return matching;

    auto candidate_slice = [&](size_t unique_idx, size_t candidate_idx) -> std::pair<const UInt32 *, const UInt32 *>
    {
        const auto & offsets = per_token_offsets[unique_idx];
        const auto & positions = per_token_positions[unique_idx];
        const UInt32 begin = candidate_idx == 0 ? 0 : offsets[candidate_idx - 1];
        return {positions.data() + begin, positions.data() + offsets[candidate_idx]};
    };

    /// Per candidate: keep the positions of term k that continue a phrase started k terms back,
    /// advancing with a two-pointer over each next term's sorted positions.
    std::vector<UInt32> chain;
    std::vector<UInt32> next;
    for (size_t i = 0; i < candidates.size(); ++i)
    {
        const auto [first_begin, first_end] = candidate_slice(term_to_unique[0], i);
        chain.assign(first_begin, first_end);

        for (size_t k = 1; k < term_to_unique.size() && !chain.empty(); ++k)
        {
            const auto [begin, end] = candidate_slice(term_to_unique[k], i);
            const UInt32 * next_position = begin;
            next.clear();
            for (UInt32 position : chain)
            {
                while (next_position != end && *next_position < position + 1)
                    ++next_position;
                if (next_position == end)
                    break;
                if (*next_position == position + 1)
                    next.push_back(*next_position);
            }
            chain.swap(next);
        }

        if (!chain.empty())
            matching.push_back(candidates[i]);
    }

    return matching;
}

}
