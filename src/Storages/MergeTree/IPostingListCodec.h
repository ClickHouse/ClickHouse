#pragma once

#include <Common/assert_cast.h>
#include <Core/Field.h>
#include <IO/WriteBufferFromString.h>
#include <base/types.h>
#include <boost/noncopyable.hpp>

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

    /// Splits the posting list into posting_list_block_size-large blocks and encodes each block separately.
    /// Also collects per-segment metadata into info and returns it to the caller (TokenPostingsInfo).
    virtual void encode(const PostingList & postings, size_t posting_list_block_size, TokenPostingsInfo & info, WriteBuffer & out) const = 0;

    /// Reads an encoded posting list, decodes it, and returns a posting list.
    virtual void decode(ReadBuffer & in, PostingList & postings) const = 0;
private:
    Type type;
};

class PostingListCodecFactory : public boost::noncopyable
{
public:
    static std::unique_ptr<IPostingListCodec> createPostingListCodec(std::string_view codec_name, const String & caller_name);
};

}
