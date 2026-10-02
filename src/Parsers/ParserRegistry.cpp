#include <Parsers/ParserRegistry.h>

#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ParserRegistry & ParserRegistry::instance()
{
    static ParserRegistry registry;
    return registry;
}

void ParserRegistry::registerParser(Creator creator)
{
    if (!creators.insert(creator).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The parser is registered more than once");
}

}
