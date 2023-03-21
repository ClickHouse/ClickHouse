#include "EscapedCharacterReader.h"

#include <charconv>

#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>

namespace DB
{

EscapedCharacterReader::ReadResult EscapedCharacterReader::read(std::string_view element, std::size_t offset)
{
    return read({element.begin() + offset, element.end()});
}

EscapedCharacterReader::ReadResult EscapedCharacterReader::read(std::string_view str)
{
    std::string escaped_sequence;

    auto rb = ReadBufferFromString(str);

    if (parseComplexEscapeSequence(escaped_sequence, rb))
    {
        return {
            rb.position(),
            {escaped_sequence.begin(), escaped_sequence.end()}
        };
    }

    return {
        rb.position(),
        {}
    };
}

}
