#include <Common/Exception.h>
#include <Common/escapeForFileName.h>

#include <Core/Defines.h>

#include <IO/WriteHelpers.h>

#include <Compression/ICompressionCodec.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int MULTIPLE_STREAMS_REQUIRED;
    extern const int LOGICAL_ERROR;
}


size_t ICompressionCodec::parseHeader(const char* header)
{
    return 0;
}

size_t ICompressionCodec::writeHeader(char* header)
{
    *header = reinterpret_cast<char>(bytecode);
    return sizeof(char);
}

}
