#include <algorithm>
#include <limits>
#include <Compression/CodecSelectorSampled.h>
#include <base/defines.h>
#include <Common/HashTable/Hash.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
extern const Event AdaptiveCodecBlocksPriced;
extern const Event AdaptiveCodecBlocksRepriced;
extern const Event AdaptiveCodecBlocksReused;
extern const Event AdaptiveCodecBytesDefault;
extern const Event AdaptiveCodecBytesWritten;
}

namespace DB::AdaptiveCodec
{

CodecSelectorSampled::CodecSelectorSampled(Codecs candidates_, UInt64 selection_period_)
    : candidates(std::move(candidates_))
    , selection_period(selection_period_)
    , sizes(candidates.size())
{
    chassert(!candidates.empty());
    to_try.reserve(candidates.size());
}

void CodecSelectorSampled::beginBlock(UInt32 source_size_)
{
    std::ranges::fill(sizes, 0u);
    source_size = source_size_;

    const UInt64 block = block_index++;
    /// intHash64(block / selection_period) s.t. we get same hash for each block in the window.
    in_sample = block % selection_period == intHash64(block / selection_period) % selection_period;
}

std::span<const CompressionCodecPtr> CodecSelectorSampled::codecsToTry()
{
    const auto pending = static_cast<size_t>(std::ranges::count(sizes, 0u));

    /// Nothing ran on the block yet.
    if (pending == sizes.size())
    {
        /// No winner → either this is stream's first block or last block did full pricing and chose NONE.
        if (!winner || in_sample)
        {
            /// A stream with a single candidate is not adaptive.
            if (candidates.size() > 1)
                ProfileEvents::increment(ProfileEvents::AdaptiveCodecBlocksPriced);
            return candidates;
        }
        return std::span(candidates).subspan(*winner, 1);
    }

    /// Every candidate ran.
    if (pending == 0)
    {
        const Ranking ranking = rank();
        winner = ranking.best;
        winner_runner_up_size = ranking.runner_up_size;
        winner_source_size = source_size;

        if (in_sample && candidates.size() > 1)
        {
            ProfileEvents::increment(ProfileEvents::AdaptiveCodecBytesDefault, sizes[0]);
            ProfileEvents::increment(ProfileEvents::AdaptiveCodecBytesWritten, ranking.best_size);
        }
        return {};
    }

    /// The winner alone ran, but it writes more per source byte than the runner-up did when priced. Thus, try the rest.
    if (static_cast<UInt64>(rank().best_size) * winner_source_size > static_cast<UInt64>(winner_runner_up_size) * source_size)
    {
        ProfileEvents::increment(ProfileEvents::AdaptiveCodecBlocksRepriced);
        to_try.clear();
        for (size_t i = 0; i < candidates.size(); ++i)
            if (sizes[i] == 0)
                to_try.push_back(candidates[i]);
        return to_try;
    }

    ProfileEvents::increment(ProfileEvents::AdaptiveCodecBlocksReused);
    return {};
}

void CodecSelectorSampled::record(const CompressionCodecPtr & codec, UInt32 size)
{
    const auto it = std::ranges::find(candidates, codec);
    chassert(it != candidates.end());
    /// A codec that ran already runs again only as its chain's base, so it writes the same size.
    sizes[static_cast<size_t>(it - candidates.begin())] = size;
}

CompressionCodecPtr CodecSelectorSampled::codecToUse() const
{
    const auto best = rank().best;
    return best ? candidates[*best] : nullptr;
}

CodecSelectorSampled::Ranking CodecSelectorSampled::rank() const
{
    Ranking ranking{.best = std::nullopt, .best_size = rawSize(), .runner_up_size = std::numeric_limits<UInt32>::max()};
    for (size_t i = 0; i < sizes.size(); ++i)
    {
        if (sizes[i] == 0)
            continue;

        if (sizes[i] < ranking.best_size)
        {
            ranking.runner_up_size = ranking.best_size;
            ranking.best_size = sizes[i];
            ranking.best = i;
        }
        else
            ranking.runner_up_size = std::min(ranking.runner_up_size, sizes[i]);
    }
    return ranking;
}

}
