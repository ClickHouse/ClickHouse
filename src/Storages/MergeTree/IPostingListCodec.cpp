#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <IO/Operators.h>

#include <config.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

std::unique_ptr<IPostingListCodec> PostingListCodecFactory::createPostingListCodec(IPostingListCodec::Type type)
{
    switch (type)
    {
        case IPostingListCodec::Type::None:
            return std::make_unique<PostingListCodecNone>();
        case IPostingListCodec::Type::Bitpacking:
            return std::make_unique<PostingListCodecBitpacking>();
        case IPostingListCodec::Type::FastPFOR:
#if USE_FASTPFOR
            return std::make_unique<PostingListCodecFastPFOR>();
#else
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "FastPFOR posting list codec is not available: ClickHouse was built without FastPFOR");
#endif
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown posting list codec type: {}", static_cast<int>(type));
}

std::unique_ptr<IPostingListCodec> PostingListCodecFactory::createPostingListCodec(std::string_view codec_name, const String & caller_name)
{
    if (codec_name == "none")
        return createPostingListCodec(IPostingListCodec::Type::None);

    if (codec_name == PostingListCodecBitpacking::getName())
        return createPostingListCodec(IPostingListCodec::Type::Bitpacking);

#if USE_FASTPFOR
    if (codec_name == PostingListCodecFastPFOR::getName())
        return createPostingListCodec(IPostingListCodec::Type::FastPFOR);
#else
    if (codec_name == PostingListCodecFastPFOR::getName())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "FastPFOR posting list codec is not available: ClickHouse was built without FastPFOR, codec name {}, for index {}", codec_name, caller_name);
#endif

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown posting list codec: '{}' for index '{}'", codec_name, caller_name);
}

}
