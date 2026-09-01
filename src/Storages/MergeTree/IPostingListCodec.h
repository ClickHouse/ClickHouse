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
class ReadBuffer;
class WriteBuffer;
using PostingList = roaring::Roaring;

/// Shared immutable array of UInt32 values of one posting-list block, e.g. row ids or term frequencies.
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
    /// Each time the open segment reaches `context.segment_size` row ids, it is sealed and a new one is started.
    /// A non-empty `tf_minus_one` (parallel to `row_ids`) carries the per-row term frequencies
    /// on the BM25 scoring path.
    virtual void append(
        std::span<const UInt32> row_ids,
        std::span<const UInt32> tf_minus_one,
        const PostingListBuildContext & context) = 0;

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
    };

    IPostingListCodec() = default;
    explicit IPostingListCodec(Type type_) : type(type_) {}

    IPostingListCodec(const IPostingListCodec &) = default;
    IPostingListCodec & operator=(const IPostingListCodec &) = default;

    virtual ~IPostingListCodec() = default;

    Type getType() const { return type; }

    /// Returns the effective segment size for the requested `posting_list_block_size`.
    /// Codecs may round the requested size.
    virtual size_t getSegmentSize(size_t posting_list_block_size) const { return posting_list_block_size; }

    /// Creates an accumulator that encodes segments of row ids into this codec's format.
    virtual std::unique_ptr<IPostingListEncoder> createEncoder() const = 0;

    /// Reads a single encoded segment of a posting list, decodes it, and appends it to `postings`.
    /// Term frequencies, if present, are skipped. `buffer` is a caller-owned scratch buffer, reused across calls.
    virtual void decode(ReadBuffer & in, PostingList & postings, bool has_term_frequencies, PaddedPODArray<char> & buffer) const = 0;

    /// The same, but appends the decoded row ids to a plain array.
    virtual void decode(ReadBuffer & in, PaddedPODArray<UInt32> & row_ids, bool has_term_frequencies, PaddedPODArray<char> & buffer) const = 0;

    /// The same, but also appends the exact per-row term frequencies to `tfs`, parallel to `row_ids`.
    /// Only valid for posting lists written with term frequencies.
    virtual void decodeWithTermFrequencies(ReadBuffer & in, PaddedPODArray<UInt32> & row_ids, PaddedPODArray<UInt32> & tfs, PaddedPODArray<char> & buffer) const = 0;


private:
    Type type{};
};

class PostingListCodecFactory : public boost::noncopyable
{
public:
    static std::unique_ptr<IPostingListCodec> createPostingListCodec(IPostingListCodec::Type type);
    static std::unique_ptr<IPostingListCodec> createPostingListCodec(std::string_view codec_name, const String & caller_name);
};

}
