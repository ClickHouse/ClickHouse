#pragma once

#include <Storages/MergeTree/IPostingListCodec.h>
#include <Common/PODArray_fwd.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>

namespace DB
{

/// Per-block payload codec for the segmented posting-list framework (see SegmentedPostingListCodec).
///
/// Encodes / decodes ONE block (1..BLOCK_SIZE values) including any codec-specific framing.
/// The surrounding segment / Index Section layout is identical across codecs; only the per-block payload differs:
///   - Bitpacking: [1 byte bits][bitpacked payload]
///   - PFor:  [PFor block]
///
/// A segment carries two interleaved block streams: the row-id deltas, encoded by the codec its header names,
/// and (when scoring is on) the per-row weights, encoded by an `IPostingListWeightsCodec` right after each block's deltas.
class IPostingListBlockCodec
{
public:
    /// Number of values in a full block.
    static constexpr size_t BLOCK_SIZE = 128;

    virtual ~IPostingListBlockCodec() = default;

    /// Append one encoded block of `values` (1..BLOCK_SIZE) to `out`. Returns the number of bytes appended.
    virtual size_t encodeBlock(std::span<const uint32_t> values, PODArray<char> & out) = 0;

    /// Decode one block of `count` (1..BLOCK_SIZE) values from `in` into `out` (which must hold at least
    /// `count` slots), advancing `in` past the consumed bytes. Returns the number of bytes consumed.
    virtual size_t decodeBlock(std::span<const std::byte> & in, size_t count, std::span<uint32_t> out) = 0;

    /// Upper bound on the encoded size of one block (1..BLOCK_SIZE values), in bytes.
    virtual size_t maxBlockBytes() const = 0;

    /// The codec type recorded in each segment header.
    virtual IPostingListCodec::Type type() const = 0;
};

/// Creates the per-block payload codec for `type`. Throws for `None` (it has no blocks).
std::unique_ptr<IPostingListBlockCodec> createPostingListBlockCodec(IPostingListCodec::Type type);

/// Per-block codec of the per-row weights of a token, stored after the block of the row ids of the same rows
/// in the posting lists of a text index with scoring. The weights are positive integers, e.g. term frequencies.
/// The scoring kind of the index determines the codec (see `createWeightsCodec`).
class IPostingListWeightsCodec
{
public:
    virtual ~IPostingListWeightsCodec() = default;

    /// Append one encoded block of `weights` (1..BLOCK_SIZE) to `out`. Returns the number of bytes appended.
    virtual size_t encodeBlock(std::span<const uint32_t> weights, PODArray<char> & out) = 0;

    /// Append one encoded block of `count` (1..BLOCK_SIZE) weights equal to 1 to `out`. Returns the number of bytes appended.
    virtual size_t encodeOnes(size_t count, PODArray<char> & out) = 0;

    /// Decode one block of `count` (1..BLOCK_SIZE) weights from `in` into `out` (which must hold at least
    /// `count` slots), advancing `in` past the consumed bytes. Returns the number of bytes consumed.
    virtual size_t decodeBlock(std::span<const std::byte> & in, size_t count, std::span<uint32_t> out) = 0;

    /// Advance `in` past one block of `count` (1..BLOCK_SIZE) weights without decoding them. Returns the number of bytes skipped.
    virtual size_t skipBlock(std::span<const std::byte> & in, size_t count) = 0;

    /// Upper bound on the encoded size of one block (1..BLOCK_SIZE weights), in bytes.
    virtual size_t maxBlockBytes() const = 0;
};

/// Creates the weights codec that stores `(weight - 1)` with the per-block codec for `type`. Throws for `None`.
std::unique_ptr<IPostingListWeightsCodec> createWeightsCodecFromBlockCodec(IPostingListCodec::Type type);

}
