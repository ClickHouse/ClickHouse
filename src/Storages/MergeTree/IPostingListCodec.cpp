#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void PostingListCodecFactory::isAllowedCodec(std::string_view codec, const std::vector<String> & allowed_codecs, std::string_view caller_name)
{
    chassert(!allowed_codecs.empty());

    if (std::ranges::find(allowed_codecs, codec) == allowed_codecs.end())
    {
        WriteBufferFromOwnString buf;
        for (size_t i = 0; i < allowed_codecs.size(); ++i)
        {
            if (i < allowed_codecs.size() - 1)
                buf << "'" << allowed_codecs[i] << "', ";
            else
                buf << "and '" << allowed_codecs[i] << "'";
        }

        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Index '{}' supports only codec {}, received '{}'", caller_name, buf.str(), codec);
    }
}

std::unique_ptr<IPostingListCodec> PostingListCodecFactory::createPostingListCodec(
    std::string_view codec_name, const std::vector<String> & allowed_codecs, const String & caller_name, bool only_validate)
{
    isAllowedCodec(codec_name, allowed_codecs, caller_name);

    if (only_validate)
        return {};

    if (codec_name == "none")
        return std::make_unique<PostingListCodecNone>();

    if (codec_name == PostingListCodecBitpacking::getName())
        return std::make_unique<PostingListCodecBitpacking>();

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown posting list codec: '{}' for index '{}'", codec_name, caller_name);
}

}

