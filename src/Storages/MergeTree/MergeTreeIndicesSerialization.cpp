#include <memory>
#include <Compression/CompressionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/MergeTree/MergeTreeIndicesSerialization.h>

namespace DB
{

CompressionCodecPtr getIndexComressionCodec(IndexSubstream::Type type, CompressionCodecPtr default_codec)
{
    if (type == IndexSubstream::Type::TextIndexPostings)
    {
        auto default_codec_desc = default_codec->getCodecDesc();
        auto delta_codec_desc = std::make_shared<ASTIdentifier>("Delta");
        auto codec_desc = makeASTFunction("CODEC", delta_codec_desc, default_codec_desc);
        return CompressionCodecFactory::instance().get(codec_desc, nullptr);
    }

    return default_codec;
}

}
