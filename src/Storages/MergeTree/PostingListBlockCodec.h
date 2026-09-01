#pragma once

#include <Storages/MergeTree/IPostingListCodec.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>

namespace DB
{

/// Per-block payload codec for the segmented posting-list framework (see SegmentedPostingListCodec).
///
/// Encodes / decodes ONE block (1..BLOCK_SIZE values) including any codec-specific framing.
/// The surrounding segment / Index Section layout is identical across codecs; only the per-block payload differs:
///   - Bitpacking: [1 byte bits][bitpacked payload]
///
/// A segment carries two interleaved block streams, both encoded by the codec its header names: the row-id
/// deltas, and (when scoring is on) the per-row `(tf - 1)` written right after each block's deltas.
class IPostingListBlockCodec
{
public:
    virtual ~IPostingListBlockCodec() = default;

    /// Append one encoded block of `values` (1..BLOCK_SIZE) to `out`. Returns the number of bytes appended.
    virtual size_t encodeBlock(std::span<const uint32_t> values, std::string & out) = 0;

    /// Decode one block of `count` (1..BLOCK_SIZE) values from `in` into `out` (which must hold at least
    /// `count` slots), advancing `in` past the consumed bytes. Returns the number of bytes consumed.
    virtual size_t decodeBlock(std::span<const std::byte> & in, size_t count, std::span<uint32_t> out) = 0;

    /// Append one block of `count` (1..BLOCK_SIZE) zero values to `out`.
    /// Returns the number of bytes appended.
    /// Must produce exactly what `encodeBlock` over `count` zeros would.
    virtual size_t encodeZeros(size_t count, std::string & out) = 0;

    /// Advance `in` past one block of `count` (1..BLOCK_SIZE) values without decoding it.
    /// Returns the number of bytes skipped.
    virtual size_t skipBlock(std::span<const std::byte> & in, size_t count) = 0;

    /// Upper bound on the encoded size of one block (1..BLOCK_SIZE delta values), in bytes.
    virtual size_t maxBlockBytes() const = 0;

    /// The codec type recorded in each segment header.
    virtual IPostingListCodec::Type type() const = 0;
};

/// Creates the per-block payload codec for `type`. Throws for `None` (it has no blocks).
std::unique_ptr<IPostingListBlockCodec> createPostingListBlockCodec(IPostingListCodec::Type type);

}
