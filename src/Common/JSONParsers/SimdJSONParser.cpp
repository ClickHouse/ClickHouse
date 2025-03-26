#include <Common/Exception.h>
#include <Common/JSONParsers/SimdJSONParser.h>

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_ALLOCATE_MEMORY;
}

void SimdJSONParser::reserve(size_t max_size)
{
    if (parser.allocate(max_size) != simdjson::error_code::SUCCESS)
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Couldn't allocate {} bytes when parsing JSON", max_size);
}

}
