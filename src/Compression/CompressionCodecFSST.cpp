#include "CompressionCodecFSST.h"

namespace DB
{

void registerCodecFSST(CompressionCodecFactory & factory)
{
    auto codec_builder = [&](const ASTPtr & arguments) -> CompressionCodecPtr
    {
        UNUSED(arguments);
        std::cerr << "Register FSST codec" << std::endl;
        return std::make_shared<CompressionCodecFSST>();
    };
    factory.registerCompressionCodec("FSST", static_cast<UInt8>(CompressionMethodByte::FSST), codec_builder);
}
}
