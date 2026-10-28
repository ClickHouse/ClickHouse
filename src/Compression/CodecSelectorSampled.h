#pragma once

#include <bitset>
#include <optional>
#include <Compression/ICodecSelector.h>

namespace DB::AdaptiveCodec
{

///   period 4, blocks   0 1 2 3   4 5 6 7   8 9 10 11
///                      P w w w   w w P w   w P w  w      P  every candidate runs, at the window's hashed offset (block 0, no winner yet)
///                                                        w  the remembered winner alone, unless it lost ground, then the rest run too

/// Prices one block per window of `selection_period` blocks, every candidate runs on it.
/// In between only the remembered winner runs, unless it writes more per source byte than the runner-up did when priced, then the rest run.
/// Per source byte rather than per block, as block sizes vary.
/// When nothing shrank the priced block there is no winner, so the next block is priced too.
/// The priced block sits at an offset hashed from the `block_index`.
/// A priced block is never larger than the default codec makes it, a block in between may be, as only the remembered winner runs on it.
class CodecSelectorSampled final : public ICodecSelector
{
public:
    static constexpr size_t MAX_CANDIDATES = 64;

    CodecSelectorSampled(Codecs candidates_, UInt64 selection_period_);

    void beginBlock(UInt32 source_size_) override;
    std::span<const CompressionCodecPtr> codecsToTry() override;
    void record(const CompressionCodecPtr & codec, UInt32 size) override;
    CompressionCodecPtr codecToUse() const override;

private:
    /// `candidates` in pool order, [0] the default codec. The smallest output wins, among equals the raw block, then the earlier candidate.
    const Codecs candidates;
    const UInt64 selection_period;

    /// The block in hand
    Codecs to_try;
    std::bitset<MAX_CANDIDATES> ran;
    std::optional<size_t> best;
    UInt32 best_size = 0;
    UInt32 runner_up_size = 0;
    UInt32 default_size = 0;
    UInt32 source_size = 0;
    bool in_sample = false;

    /// What the stream remembers
    UInt64 block_index = 0;
    std::optional<size_t> winner;
    UInt32 winner_runner_up_size = 0;
    UInt32 winner_source_size = 0;
};

}
