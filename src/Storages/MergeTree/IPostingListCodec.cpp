#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

std::unique_ptr<IPostingListCodec> PostingListCodecFactory::createPostingListCodec(std::string_view codec_name, const String & caller_name)
{
    if (codec_name == "none")
        return std::make_unique<PostingListCodecNone>();

    if (codec_name == PostingListCodecBitpacking::getName())
        return std::make_unique<PostingListCodecBitpacking>();

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown posting list codec: '{}' for index '{}'", codec_name, caller_name);
}

}

