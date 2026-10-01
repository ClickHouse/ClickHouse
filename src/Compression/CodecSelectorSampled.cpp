#include <Compression/CodecSelectorSampled.h>
#include <base/defines.h>
#include <Common/HashTable/Hash.h>
#include <Common/assert_cast.h>

namespace DB::AdaptiveCodec
{

CodecSelectorSampled::CodecSelectorSampled(UInt64 selection_period_)
    : selection_period(selection_period_)
{
}

CompressionStatePtr CodecSelectorSampled::createState() const { return std::make_unique<State>(); }

Candidates CodecSelectorSampled::select(CompressionState * state, const Outcome & outcome) const
{
    chassert(state != nullptr);
    auto & stream = assert_cast<State &>(*state);

    /// Fresh block.
    if (outcome.ran.none())
    {
        const UInt64 block = stream.block_index++;
        /// intHash64(block / selection_period) s.t. we get same hash for each block in the window.
        const bool sampled = block % selection_period == intHash64(block / selection_period) % selection_period;
        if (!stream.winner || sampled)
            return outcome.all;
        return Candidates{}.set(*stream.winner);
    }

    /// We ran all candidates on this block, remember the pricing.
    if (outcome.ran == outcome.all)
    {
        stream.winner = outcome.best; /// Empty when nothing shrank the block, so no winner is remembered.
        stream.runner_up_size = outcome.runner_up_size;
        stream.source_size = outcome.source_size;
        return {};
    }

    /// The winner alone ran, but it writes more per source byte than the runner-up did when priced. Thus, try the rest.
    if (static_cast<UInt64>(outcome.best_size) * stream.source_size > static_cast<UInt64>(stream.runner_up_size) * outcome.source_size)
        return outcome.all & ~outcome.ran;
    return {};
}

}
