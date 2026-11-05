#include <Storages/MergeTree/TextIndexPhraseSearch.h>

#include <Common/Exception.h>

#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

namespace
{

/// Reads a position list stored as its first position followed by the gaps.
class PositionsCursor
{
public:
    PositionsCursor(const UInt32 * begin, const UInt32 * end_)
        : next_gap(begin + 1)
        , end(end_)
        , position(*begin)
    {
    }

    UInt32 value() const { return position; }

    bool advance()
    {
        if (next_gap == end)
            return false;

        /// The writer emits strictly increasing positions, so a zero gap or a wrap means corruption.
        const UInt32 gap = *next_gap++;
        if ((gap == 0) || (position > std::numeric_limits<UInt32>::max() - gap))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupt text index positions: positions do not increase within a document");

        position += gap;
        return true;
    }

private:
    const UInt32 * next_gap;
    const UInt32 * end;
    UInt32 position;
};

}

void TextIndexPhraseSearch::matchCandidatePositions(
    std::span<const UInt32> candidates,
    const std::vector<PaddedPODArray<UInt64>> & per_token_bounds,
    const std::vector<PaddedPODArray<UInt32>> & per_token_lanes,
    const std::vector<size_t> & term_to_unique,
    PaddedPODArray<UInt32> & matching)
{
    if (candidates.empty() || term_to_unique.empty())
        return;

    const auto open = [&](size_t term, size_t candidate)
    {
        const size_t u = term_to_unique[term];
        const UInt32 * lane = per_token_lanes[u].data();
        return PositionsCursor(lane + per_token_bounds[u][2 * candidate], lane + per_token_bounds[u][2 * candidate + 1]);
    };

    /// A single-term phrase matches every candidate.
    if (term_to_unique.size() == 1)
    {
        matching.insert(candidates.begin(), candidates.end());
        return;
    }

    /// Per candidate: keep the positions of term k that continue a phrase started k terms back,
    /// advancing with a two-pointer over each next term's sorted positions. A candidate matches
    /// at the first position that survives to the last term. The first term is read in place;
    /// longer phrases alternate buffers so the one being read is never the one being written.
    std::vector<UInt32> buffers[2];
    for (size_t i = 0; i < candidates.size(); ++i)
    {
        bool matched = false;
        std::span<const UInt32> chain;
        size_t buffer_index = 0;
        for (size_t k = 1; k < term_to_unique.size(); ++k)
        {
            bool has_next = true;
            PositionsCursor next_term = open(k, i);
            const bool last = k + 1 == term_to_unique.size();
            auto & continuing = buffers[buffer_index];
            continuing.clear();

            /// Returns false once no later position of the chain can continue the phrase.
            const auto step = [&](UInt32 position)
            {
                /// Avoid `position + 1`, which wraps at the maximum position; past the advance the difference is exact.
                while (has_next && next_term.value() <= position)
                    has_next = next_term.advance();

                if (!has_next)
                    return false;

                if (next_term.value() - position == 1)
                {
                    if (last)
                    {
                        matched = true;
                        return false;
                    }
                    continuing.push_back(position + 1);
                }
                return true;
            };

            if (k == 1)
            {
                PositionsCursor first_term = open(0, i);
                while (step(first_term.value()) && first_term.advance())
                {
                }
            }
            else
            {
                for (UInt32 position : chain)
                {
                    if (!step(position))
                        break;
                }
            }

            if (last || continuing.empty())
                break;

            chain = continuing;
            buffer_index ^= 1;
        }

        if (matched)
            matching.push_back(candidates[i]);
    }
}

}
