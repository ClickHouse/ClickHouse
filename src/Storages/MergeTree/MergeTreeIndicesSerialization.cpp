#include <Storages/MergeTree/MergeTreeIndicesSerialization.h>
#include <Compression/CompressionFactory.h>

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
