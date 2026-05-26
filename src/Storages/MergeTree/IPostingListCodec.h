#pragma once

#include <Common/assert_cast.h>
#include <IO/WriteBufferFromString.h>
#include <base/types.h>
#include <boost/noncopyable.hpp>

#include <span>

#include <roaring/roaring.hh>

namespace DB
{

struct TokenPostingsInfo;
class ReadBuffer;
class WriteBuffer;
using PostingList = roaring::Roaring;

/// IPostingListCodec is an interface for compressing text index posting list.
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

    /// Encodes posting list from a pre-sorted array of row_ids.
    /// Splits into posting_list_block_size-large blocks and encodes each block separately.
    /// Also collects per-segment metadata into info and returns it to the caller (TokenPostingsInfo).
    /// Compressed codecs (Bitpacking) append a per-block Index Section after each segment for lazy cursor support.
    virtual void encode(std::span<const UInt32> row_ids, size_t posting_list_block_size, TokenPostingsInfo & info, WriteBuffer & out) const = 0;

    /// Reads an encoded posting list, decodes it, and returns a posting list.
    virtual void decode(ReadBuffer & in, PostingList & postings) const = 0;
private:
    Type type;
};

class PostingListCodecFactory : public boost::noncopyable
{
public:
    static std::unique_ptr<IPostingListCodec> createPostingListCodec(IPostingListCodec::Type type);
    static std::unique_ptr<IPostingListCodec> createPostingListCodec(std::string_view codec_name, const String & caller_name);
};

}
