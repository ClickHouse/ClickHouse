#include "CompressionCodecFSST.h"

namespace DB
{

void registerCodecFSST(CompressionCodecFactory & factory)
{
    auto codec_builder = [&](const ASTPtr & arguments) -> CompressionCodecPtr
    {
        UNUSED(arguments);
        std::cerr << "Lol kek" << std::endl;
        return std::make_shared<CompressionCodecFSST>();
    };
    factory.registerCompressionCodec("FSST", {}, codec_builder);
}
}
