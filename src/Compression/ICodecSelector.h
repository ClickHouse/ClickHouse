#pragma once

#include <bitset>
#include <limits>
#include <optional>
#include <Compression/ICompressionCodec.h>

namespace DB::AdaptiveCodec
{

/// A pool entry takes two positions, its codec's and its chain's (e.g. `T64` and `T64 + Default`).
constexpr size_t MAX_POSITIONS = 64;

/// Candidates by position: those to run on a block, or those that ran. Selecting a chain runs its codec too, so `ran` holds both.
using Candidates = std::bitset<MAX_POSITIONS>;

/// What a block's runs produced so far. Starts at the raw block: `best` is empty and `best_size` its size until a candidate beats it.
struct Outcome
{
    explicit Outcome(UInt32 source_size_)
        : best_size(ICompressionCodec::getHeaderSize() + source_size_)
        , source_size(source_size_)
    {
    }

    Candidates ran;
    std::optional<size_t> best;
    UInt32 best_size;
    UInt32 runner_up_size = std::numeric_limits<UInt32>::max();
    UInt32 source_size;
};

/// For each block, picks the pool candidates `CompressionCodecAdaptive` runs. The codec compresses with them and keeps the smallest output.
/// Holds no state. Stream stores memory in `CompressionState`, so one selector serves any number of streams.
class ICodecSelector
{
public:
    virtual ~ICodecSelector() = default;

    /// What a stream remembers, nullptr for nothing.
    virtual CompressionStatePtr createState() const = 0;

    /// The candidates to run next on the block, given what ran on it so far (nothing at the first call).
    /// Those that ran already are skipped. Empty = the block is done.
    virtual Candidates select(CompressionState * state, const Outcome & outcome) const = 0;
};

}
