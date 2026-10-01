#pragma once

#include <bitset>
#include <limits>
#include <optional>
#include <Compression/ICompressionCodec.h>

namespace DB::AdaptiveCodec
{

/**
  * How `CompressionCodecAdaptive` picks a codec per block, as a selector sees it.
  *
  * Entry `e` of the pool owns positions `2 * e` (its codec) and `2 * e + 1` (its chain, the codec followed by the deployment default).
  * A selector sees positions and sizes, never codecs.
  *
  * A block is compressed in runs. Before the first run and after each one, `select` names the positions to run next, empty ends the block.
  * Planning a chain runs its codec too, as the chain applies its second codec to the first codec's output.
  *
  * A selector holds no per-stream state.
  * What a stream remembers travels in the `CompressionState` from `createState`, so one selector serves any number of streams.
  */

constexpr size_t MAX_POSITIONS = 64;
using Candidates = std::bitset<MAX_POSITIONS>;

/// What a block offers and what its runs produced so far. `all` is every candidate the pool has.
/// Starts at the raw block with empty `best` and `best_size` being raw size until a candidate beats it.
struct Outcome
{
    Outcome(Candidates all_, UInt32 source_size_)
        : all(all_)
        , best_size(ICompressionCodec::getHeaderSize() + source_size_)
        , source_size(source_size_)
    {
    }

    const Candidates all;
    Candidates ran;
    std::optional<size_t> best;
    UInt32 best_size;
    UInt32 runner_up_size = std::numeric_limits<UInt32>::max();
    UInt32 source_size;
};

class ICodecSelector
{
public:
    virtual ~ICodecSelector() = default;

    /// What a stream remembers, nullptr for nothing.
    virtual CompressionStatePtr createState() const = 0;

    /// The candidates to run next on the block, given what ran on it so far. Empty = the block is done.
    virtual Candidates select(CompressionState * state, const Outcome & outcome) const = 0;
};

}
