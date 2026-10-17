#include <Compression/CodecSelectorSampled.h>
#include <base/defines.h>
#include <Common/HashTable/Hash.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>

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
        stream.in_sample = block % selection_period == intHash64(block / selection_period) % selection_period;
        /// No winner → either this is stream's first block or last block did full pricing and chose NONE.
        if (!stream.winner || stream.in_sample)
        {
            /// A stream with a single candidate is not adaptive.
            if (outcome.all.count() > 1)
                ProfileEvents::increment(ProfileEvents::AdaptiveCodecBlocksPriced);
            return outcome.all;
        }
        return Candidates{}.set(*stream.winner);
    }

    if (outcome.ran == outcome.all)
    {
        stream.winner = outcome.best;
        stream.runner_up_size = outcome.runner_up_size;
        stream.source_size = outcome.source_size;

        if (stream.in_sample && outcome.all.count() > 1)
        {
            ProfileEvents::increment(ProfileEvents::AdaptiveCodecBytesDefault, outcome.default_size);
            ProfileEvents::increment(ProfileEvents::AdaptiveCodecBytesWritten, outcome.best_size);
        }
        return {};
    }

    /// The winner alone ran, but it writes more per source byte than the runner-up did when priced. Thus, try the rest.
    if (static_cast<UInt64>(outcome.best_size) * stream.source_size > static_cast<UInt64>(stream.runner_up_size) * outcome.source_size)
    {
        ProfileEvents::increment(ProfileEvents::AdaptiveCodecBlocksRepriced);
        return outcome.all & ~outcome.ran;
    }

    ProfileEvents::increment(ProfileEvents::AdaptiveCodecBlocksReused);
    return {};
}

}
