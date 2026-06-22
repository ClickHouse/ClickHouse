#pragma once

#include <Storages/MergeTree/IPostingListCodec.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>

namespace DB
{

/// Per-block payload codec for the segmented posting-list framework (see SegmentedPostingListCodecImpl).
///
/// Encodes / decodes ONE block (1..BLOCK_SIZE delta values) including any codec-specific framing. The
/// surrounding segment / Index Section layout is identical across codecs; only the per-block payload differs:
///   - Bitpacking: [1 byte bits][bitpacked payload]
///   - FastPFOR:   [self-delimited FastPFOR payload]
///
/// This header is dependency-free (no SIMD / FastPFOR headers), so it stays include-safe in builds without
/// FastPFOR. FastPFOR availability is enforced inside FastPFORBlockCodec, which createPostingListBlockCodec
/// constructs — on a build without FastPFOR that construction throws SUPPORT_IS_DISABLED.
class IPostingListBlockCodec
{
public:
    virtual ~IPostingListBlockCodec() = default;

    /// Append one encoded block of `deltas` (1..BLOCK_SIZE values) to `out`. Returns the number of bytes appended.
    virtual size_t encodeBlock(std::span<uint32_t> deltas, std::string & out) = 0;

    /// Decode one block of `count` (1..BLOCK_SIZE) delta values from `in` into `out` (which must hold at least
    /// `count` slots), advancing `in` past the consumed bytes. Returns the number of bytes consumed.
    virtual size_t decodeBlock(std::span<const std::byte> & in, size_t count, std::span<uint32_t> out) = 0;

    /// Upper bound on the encoded size of one block (1..BLOCK_SIZE delta values), in bytes. The lazy cursor
    /// queries it through this interface to bound untrusted segment payload sizes before allocating, so it
    /// never has to name a concrete codec.
    virtual size_t maxBlockBytes() const = 0;

    /// The codec type recorded in each segment header.
    virtual IPostingListCodec::Type type() const = 0;
};

/// Creates the per-block payload codec for `type`. Throws for `None` (it has no blocks). For `FastPFOR` on a
/// build without FastPFOR, the underlying FastPFORBlockCodec constructor throws SUPPORT_IS_DISABLED.
std::unique_ptr<IPostingListBlockCodec> createPostingListBlockCodec(IPostingListCodec::Type type);

}
