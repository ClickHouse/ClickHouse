#pragma once

#include <span>
#include <Compression/ICompressionCodec.h>

namespace DB::AdaptiveCodec
{

/// Picks the codecs for `CompressionCodecAdaptive` to run and remembers what it needs between the blocks. It's the stream's `CompressionState`.
class ICodecSelector : public CompressionState
{
public:
    /// A block of `source_size` bytes starts.
    virtual void beginBlock(UInt32 source_size) = 0;

    /// The codecs to try next on the block, empty when the block is done.
    virtual std::span<const CompressionCodecPtr> codecsToTry() = 0;

    /// A run of `codec` wrote `size` bytes, header included.
    virtual void record(const CompressionCodecPtr & codec, UInt32 size) = 0;

    /// The codec with the smallest output so far, nullptr while the raw block is smaller.
    virtual CompressionCodecPtr codecToUse() const = 0;
};

}
