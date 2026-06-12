#pragma once

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
class ReadBuffer;
class WriteBuffer;
using PostingList = roaring::Roaring;

/// IPostingListAccumulator accumulates a posting list encoded into the codec's
/// in-memory form and serializes it at the end.
///
/// During the text index build, row ids of each token are collected as raw values into
/// a vector, and the vector is flushed here every time it reaches `append_granularity`.
/// The accumulator splits the appended row ids into segments of the segment size
/// (`posting_list_block_size` row ids); therefore all segments, except possibly the
/// last one, contain exactly the segment size of row ids.
class IPostingListAccumulator
{
public:
    /// Every `append`, except the final one before `finalize`, must contain a multiple
    /// of this many row ids. It guarantees that codecs encoding fixed-size blocks
    /// (bitpacking) never produce a partial block in the middle of a segment.
    static constexpr size_t append_granularity = 128;

    virtual ~IPostingListAccumulator() = default;

    /// Encodes a batch of sorted unique row ids (increasing across calls) into the
    /// codec's in-memory form, appending to the open segment. Each time the open
    /// segment reaches `segment_size` row ids, it is sealed and a new one is started.
    virtual void append(std::span<const UInt32> row_ids, size_t segment_size) = 0;

    /// Seals the last segment and writes all accumulated segments to `out`, filling
    /// per-segment metadata (offsets, ranges) and header flags in `info`.
    /// Must be called exactly once, after all calls to `append`.
    virtual void finalize(WriteBuffer & out, TokenPostingsInfo & info) = 0;

    /// Total number of row ids accumulated so far.
    virtual size_t cardinality() const = 0;

    /// Heap memory held by the accumulator (for memory accounting during the build).
    virtual size_t memoryUsageBytes() const = 0;
};

/// IPostingListCodec is an interface for compressing text index posting lists.
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
    /// Codecs may round the requested size (e.g. `bitpacking` aligns it to the size of
    /// the physical block expected by the SIMD bit-packing implementation).
    virtual size_t getSegmentSize(size_t posting_list_block_size) const { return posting_list_block_size; }

    /// Creates an accumulator that encodes segments of row ids into this codec's format.
    virtual std::unique_ptr<IPostingListAccumulator> createAccumulator() const = 0;

    /// Reads a single encoded segment of a posting list, decodes it, and appends it to `postings`.
    virtual void decode(ReadBuffer & in, PostingList & postings) const = 0;
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
