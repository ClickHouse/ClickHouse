#pragma once

#include <base/types.h>
#include <Common/assert_cast.h>
#include <roaring/roaring.hh>
#include <boost/noncopyable.hpp>
#include <IO/WriteBufferFromString.h>
#include <Core/Field.h>


namespace DB
{

struct PostingListBuilder;
struct TokenPostingsInfo;
class ReadBuffer;
class WriteBuffer;
using PostingList = roaring::Roaring;

/// IPostingListCodec defines the Text Index posting list codec interface.
/// The `encode` writes an encoded posting list to a WriteBuffer,
/// and `decode` reads from a ReadBuffer, decodes the data, and produces a posting list instance.
struct IPostingListCodec
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

    /// The `encode splits the posting list into blocks according to posting_list_block_size and encodes each block separately.
    /// It also collects per-segment metadata into info and returns it to the caller.
    virtual void encode(const PostingListBuilder & builder, size_t posting_list_block_size, TokenPostingsInfo & info, WriteBuffer & wb) const = 0;

    /// Reads from a ReadBuffer, decodes the data, and fills the `posting_list.
    virtual void decode(ReadBuffer & rb, PostingList & posting_list) const = 0;
private:
    Type type;
};

class PostingListCodecFactory : public boost::noncopyable
{
public:
    static void isAllowedCodec(std::string_view codec_name, const std::vector<String> & allowed_codecs, std::string_view caller_name);

    static std::unique_ptr<IPostingListCodec> createPostingListCodec(
        std::string_view codec_name,
        const std::vector<String> & allowed_codecs,
        const String & caller_name,
        bool only_validate = false);
};

}
