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
        return CompressionCodecFactory::instance().get("NONE");
    }

    return default_codec;
}

}
