#pragma once

#include <Common/assert_cast.h>
#include <IO/WriteBufferFromString.h>
#include <base/types.h>
#include <boost/noncopyable.hpp>

#include <memory>

#include <roaring/roaring.hh>

namespace DB
{

struct TokenPostingsInfo;
class ReadBuffer;
class WriteBuffer;
using PostingList = roaring::Roaring;

/// IPostingListAccumulator builds a posting list incrementally and serializes it via a codec.
///
/// Row ids are streamed in (strictly increasing) during text index construction. The
/// accumulator encodes them on the fly into its codec-specific in-memory form (Roaring
/// bitmaps for the None codec, bit-packed bytes for the Bitpacking codec) and splits them
/// into segments of `posting_list_block_size` row ids. After the index is built, `finalize`
/// flushes the accumulated data to the postings stream and fills `TokenPostingsInfo` with the
/// per-segment offsets, row ranges, and header flags.
class IPostingListAccumulator
{
public:
    virtual ~IPostingListAccumulator() = default;

    /// Row ids are added (strictly increasing) via each concrete accumulator's `insert`, which is
    /// called through its concrete (final) type so the per-row hot path is devirtualized — see
    /// `dispatchByPostingCodec` and `PostingListBuilder::add`. It is intentionally not part of this
    /// interface to keep that call out of the vtable.

    /// Flushes all accumulated postings to `out` and fills per-segment metadata and
    /// header flags into `info`. Must be called exactly once after all `insert` calls.
    virtual void finalize(WriteBuffer & out, TokenPostingsInfo & info) = 0;

    /// Number of row ids accumulated so far.
    virtual UInt32 cardinality() const = 0;

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

    /// Creates a streaming accumulator that encodes row ids directly into this codec's format.
    /// The segment size (`posting_list_block_size`) is passed per `insert` call, not stored.
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
