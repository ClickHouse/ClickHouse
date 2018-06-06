#include <Common/parseAddress.h>
#include <Common/Exception.h>
#include <IO/ReadHelpers.h>
#include <common/find_first_symbols.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

std::pair<std::string, UInt16> parseAddress(const std::string & str, UInt16 default_port)
{
    if (str.empty())
        throw Exception("Empty address passed to function parseAddress", ErrorCodes::BAD_ARGUMENTS);

    const char * begin = str.data();
    const char * end = begin + str.size();
    const char * port = end;

    if (begin[0] == '[')
    {
        const char * closing_square_bracket = find_first_symbols<']'>(begin + 1, end);
        if (closing_square_bracket >= end)
            throw Exception("Illegal address passed to function parseAddress: "
                "the address begins with opening square bracket, but no closing square bracket found", ErrorCodes::BAD_ARGUMENTS);

        port = find_first_symbols<':'>(closing_square_bracket + 1, end);
    }
    else
        port = find_first_symbols<':'>(begin, end);

    if (port != end)
    {
        UInt16 port_number = parse<UInt16>(port + 1);
        return { std::string(begin, port), port_number };
    }
    else if (default_port)
    {
        return { str, default_port };
    }
    else
        throw Exception("The address passed to function parseAddress doesn't contain port number "
            "and no 'default_port' was passed", ErrorCodes::BAD_ARGUMENTS);
}

}
