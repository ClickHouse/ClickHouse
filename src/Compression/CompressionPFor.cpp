#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <common/unaligned.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <IO/WriteHelpers.h>
#include <cstdlib>

#include <bitset>
#include <limits>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_SIZE_PARAMETER;
}

CompressionCodecPFor::CompressionCodecPFor(UInt8 data_bytes_size_)
        : data_bytes_size(data_bytes_size_) {}

UInt8 CompressionCodecPFor::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::PFor);
}

String CompressionCodecPFor::getCodecDesc() const
{
    return "GroupPFor(" + toString(data_bytes_size) + ")";
}

namespace
{

}
void registerCodecPFor(CompressionCodecFactory & factory)
{
    factory.registerSimpleCompressionCodec("PFor", static_cast<char>(CompressionMethodByte::NONE), [&] ()
    {
        return std::make_shared<CompressionCodecPFor>();
    });
}
}
