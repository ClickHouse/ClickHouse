#include <Common/JSONParsers/SimdJSONParser.h>

#if USE_SIMDJSON

#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_ALLOCATE_MEMORY;
extern const int UNKNOWN_EXCEPTION;
}

void SimdJSONParser::reserve(size_t max_size)
{
    auto res = parser.allocate(max_size);
    if (res != simdjson::error_code::SUCCESS)
    {
        if (res == simdjson::error_code::MEMALLOC)
            throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Couldn't allocate {} bytes when parsing JSON", max_size);
        else
            throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "simdjson failed with: {}", res);
    }
}

}

#endif
