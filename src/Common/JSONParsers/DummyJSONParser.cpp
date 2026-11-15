#include <Common/Exception.h>
#include <Common/JSONParsers/DummyJSONParser.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

bool DummyJSONParser::parse(std::string_view, Element &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Functions JSON* are not supported");
}

}
