#include <Backups/findCharacterNotPreservedByXML.h>

#include <Common/UTF8Helpers.h>

namespace DB
{

namespace
{
    /// The `Char` production of XML 1.0.
    bool isValidXMLCharacter(UInt32 code_point)
    {
        return code_point == 0x9 || code_point == 0xA || code_point == 0xD
            || (code_point >= 0x20 && code_point <= 0xD7FF)
            || (code_point >= 0xE000 && code_point <= 0xFFFD)
            || (code_point >= 0x10000 && code_point <= 0x10FFFF);
    }
}

std::optional<size_t> findCharacterNotPreservedByXML(std::string_view s)
{
    const char * const begin = s.data();
    const char * const end = begin + s.size();

    for (const char * pos = begin; pos < end;)
    {
        const size_t length = UTF8::seqLength(static_cast<UInt8>(*pos));

        std::optional<UInt32> code_point;
        if (length <= static_cast<size_t>(end - pos))
            code_point = UTF8::convertUTF8ToCodePoint(pos, length);

        /// A carriage return passes the `Char` production but not the round trip, so it is rejected
        /// separately rather than by bending what that production means.
        if (!code_point || !isValidXMLCharacter(*code_point) || *code_point == '\r')
            return pos - begin;

        pos += length;
    }

    return {};
}

}
