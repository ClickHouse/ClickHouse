#pragma once

#include <Common/PODArray_fwd.h>
#include <Common/assert_cast.h>
#include <IO/WriteBufferFromString.h>
#include <base/types.h>
#include <boost/noncopyable.hpp>

#include <memory>
#include <span>

#include <roaring/roaring.hh>

namespace DB
{

struct TokenPostingsInfo;
struct PostingListBuildContext;
enum class TextIndexScoringKind : UInt8;
class ReadBuffer;
class WriteBuffer;
using PostingList = roaring::Roaring;

/// Shared immutable array of UInt32 values of one posting-list block, e.g. row ids or weights.
using PaddedPODArrayPtr = std::shared_ptr<const PaddedPODArray<UInt32>>;

/// Incrementally encodes the posting list of a single token during the text index build.
/// Sorted row ids arrive in batches via `append`, are split into fixed-size segments and
/// encoded right away; `finalize` writes the buffered encoded segments to the output.
class IPostingListEncoder
{
public:
    /// Every `append`, except the final one before `finalize`,
    /// must contain a multiple of this many row ids.
    static constexpr size_t append_granularity = 128;

    virtual ~IPostingListEncoder() = default;

    /// Encodes a batch of sorted unique row ids (increasing across calls), appending to the open segment.
    /// Each time the open segment reaches the segment size, it is sealed and a new one is started.
    /// A non-empty `weights` (parallel to `row_ids`) carries the per-row weights on the scoring path.
    virtual void append(std::span<const UInt32> row_ids, std::span<const UInt32> weights, const PostingListBuildContext & context) = 0;

    /// Seals the last segment and writes all accumulated segments to `out`.
    /// Fills per-segment metadata (offsets, ranges) and header flags in `info`.
    virtual void finalize(WriteBuffer & out, TokenPostingsInfo & info) = 0;

    /// Total number of row ids accumulated so far.
    virtual size_t cardinality() const = 0;
};

/// IPostingListCodec is an interface for serializing/deserializing text index posting lists.
class IPostingListCodec
{
public:
    enum class Type
    {
        None,
        Bitpacking,
        PFor,
    };

    IPostingListCodec() = default;
    explicit IPostingListCodec(Type type_) : type(type_) {}

    IPostingListCodec(const IPostingListCodec &) = default;
    IPostingListCodec & operator=(const IPostingListCodec &) = default;

    virtual ~IPostingListCodec() = default;

    Type getType() const { return type; }

    /// Creates an accumulator that encodes segments of `segment_size` row ids into this codec's format.
    /// Codecs may round the requested size, e.g. up to a multiple of their physical block size.
    /// `scoring` is the scoring kind of the index, which determines the weights stored with the row ids.
    virtual std::unique_ptr<IPostingListEncoder> createEncoder(size_t segment_size, TextIndexScoringKind scoring) const = 0;

    /// Reads a single encoded segment of a posting list and decodes it into `postings`, which must be empty.
    /// `max_cardinality` is the max number of row ids the segment may hold according to the token metadata.
    /// The sizes claimed by the segment are checked against it before any buffer grows to them.
    /// `scoring` is the scoring kind of the weights stored in the posting list (`None` if it stores no weights),
    /// which determines their codec (see `createWeightsCodec`). The weights are skipped.
    /// `buffer` is a caller-owned scratch buffer, reused across calls.
    virtual void decode(ReadBuffer & in, UInt64 max_cardinality, PostingList & postings, TextIndexScoringKind scoring, PaddedPODArray<char> & buffer) const = 0;

    /// The same, but appends the decoded row ids to a plain array.
    virtual void decode(ReadBuffer & in, UInt64 max_cardinality, PaddedPODArray<UInt32> & row_ids, TextIndexScoringKind scoring, PaddedPODArray<char> & buffer) const = 0;

    /// The same, but also appends the exact per-row weights to `weights`, parallel to `row_ids`.
    /// Only valid for posting lists written with weights (`scoring` is not `None`).
    virtual void decodeWithWeights(ReadBuffer & in, UInt64 max_cardinality, PaddedPODArray<UInt32> & row_ids, PaddedPODArray<UInt32> & weights, TextIndexScoringKind scoring, PaddedPODArray<char> & buffer) const = 0;

private:
    Type type{};
};

inline constexpr bool isValidPostingListCodecType(UInt64 value)
{
    return value <= static_cast<UInt64>(IPostingListCodec::Type::PFor);
}

/// `None` is excluded: an uncompressed posting list has no segments.
inline constexpr bool isValidPostingListBlockCodecType(UInt64 value)
{
    return value == static_cast<UInt64>(IPostingListCodec::Type::Bitpacking)
        || value == static_cast<UInt64>(IPostingListCodec::Type::PFor);
}

class PostingListCodecFactory : public boost::noncopyable
{
public:
    static std::unique_ptr<IPostingListCodec> createPostingListCodec(IPostingListCodec::Type type);
    static std::unique_ptr<IPostingListCodec> createPostingListCodec(std::string_view codec_name, const String & caller_name);
};

}
