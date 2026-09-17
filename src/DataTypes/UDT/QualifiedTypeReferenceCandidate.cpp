#include <DataTypes/UDT/QualifiedTypeReferenceCandidate.h>

#include <Core/Types.h>
#include <Common/StringUtils.h>

namespace DB::UDT
{
namespace
{

struct QuotedTokenEnd
{
    size_t position = 0;
    bool closed = false;
};

struct TriviaEnd
{
    size_t position = 0;
    bool malformed = false;
};

constexpr bool matchesUTF8(std::string_view text, size_t position, UInt8 first, UInt8 second, UInt8 third) noexcept
{
    return position + 2 < text.size() && static_cast<UInt8>(text[position]) == first && static_cast<UInt8>(text[position + 1]) == second
        && static_cast<UInt8>(text[position + 2]) == third;
}

QuotedTokenEnd skipUnicodeQuotedToken(std::string_view text, size_t position, UInt8 expected_end) noexcept
{
    position += 3;
    while (position < text.size())
    {
        if (matchesUTF8(text, position, 0xE2, 0x80, expected_end))
            return {.position = position + 3, .closed = true};
        ++position;
    }
    return {.position = text.size(), .closed = false};
}

QuotedTokenEnd skipQuotedToken(std::string_view text, size_t position, char quote) noexcept
{
    ++position;
    while (position < text.size())
    {
        if (text[position] == '\\' && position + 1 < text.size())
        {
            position += 2;
            continue;
        }
        if (text[position] != quote)
        {
            ++position;
            continue;
        }
        if (position + 1 < text.size() && text[position + 1] == quote)
        {
            position += 2;
            continue;
        }
        return {.position = position + 1, .closed = true};
    }
    return {.position = text.size(), .closed = false};
}

TriviaEnd skipTrivia(std::string_view text, size_t position) noexcept
{
    while (position < text.size())
    {
        const char * begin = text.data() + position;
        const char * skipped = skipWhitespacesUTF8(begin, text.data() + text.size());
        if (skipped != begin)
        {
            position = static_cast<size_t>(skipped - text.data());
            continue;
        }
        if (text[position] == '#')
        {
            while (position < text.size() && text[position] != '\n')
                ++position;
            continue;
        }
        if (position + 1 < text.size() && text[position] == '-' && text[position + 1] == '-')
        {
            position += 2;
            while (position < text.size() && text[position] != '\n')
                ++position;
            continue;
        }
        if (position + 1 < text.size() && text[position] == '/' && text[position + 1] == '*')
        {
            position += 2;
            while (position + 1 < text.size() && (text[position] != '*' || text[position + 1] != '/'))
                ++position;
            if (position + 1 >= text.size())
                return {.position = text.size(), .malformed = true};
            position += 2;
            continue;
        }
        break;
    }
    return {.position = position};
}

bool consumeIdentifier(std::string_view text, size_t & position) noexcept
{
    if (position >= text.size())
        return false;
    if (matchesUTF8(text, position, 0xE2, 0x80, 0x9C))
    {
        const auto end = skipUnicodeQuotedToken(text, position, 0x9D);
        if (!end.closed || end.position == position + 6)
            return false;
        position = end.position;
        return true;
    }
    if (text[position] == '`' || text[position] == '"')
    {
        const auto end = skipQuotedToken(text, position, text[position]);
        if (!end.closed)
            return false;
        position = end.position;
        return true;
    }
    if (!isValidIdentifierBegin(text[position]) && text[position] != '$')
        return false;
    ++position;
    while (position < text.size() && (isWordCharASCII(text[position]) || text[position] == '$'))
        ++position;
    return true;
}

}

bool hasQualifiedTypeReferenceCandidate(std::string_view type_spelling) noexcept
{
    size_t position = 0;
    while (position < type_spelling.size())
    {
        const auto trivia = skipTrivia(type_spelling, position);
        if (trivia.malformed)
            return true;
        position = trivia.position;
        if (position >= type_spelling.size())
            return false;
        if (matchesUTF8(type_spelling, position, 0xE2, 0x80, 0x98))
        {
            const auto end = skipUnicodeQuotedToken(type_spelling, position, 0x99);
            if (!end.closed)
                return true;
            position = end.position;
            continue;
        }
        if (type_spelling[position] == '\'')
        {
            const auto end = skipQuotedToken(type_spelling, position, '\'');
            if (!end.closed)
                return true;
            position = end.position;
            continue;
        }

        /// The SQL lexer accepts only ASCII bare words, Unicode whitespace and
        /// the two English-style Unicode quote pairs handled above. Any other
        /// non-ASCII token is not evidence that the spelling contains no UDT.
        /// Keep the screening boundary conservative so a future lexer extension
        /// cannot silently create a false negative here.
        const bool unicode_quoted_identifier = matchesUTF8(type_spelling, position, 0xE2, 0x80, 0x9C);
        if (static_cast<UInt8>(type_spelling[position]) >= 0x80 && !unicode_quoted_identifier)
            return true;

        size_t identifier_end = position;
        if (!consumeIdentifier(type_spelling, identifier_end))
        {
            if (type_spelling[position] == '`' || type_spelling[position] == '"' || matchesUTF8(type_spelling, position, 0xE2, 0x80, 0x9C))
                return true;
            ++position;
            continue;
        }
        const auto separator_trivia = skipTrivia(type_spelling, identifier_end);
        if (separator_trivia.malformed)
            return true;
        size_t separator = separator_trivia.position;
        if (separator < type_spelling.size() && type_spelling[separator] == '.')
        {
            const auto local_name_trivia = skipTrivia(type_spelling, separator + 1);
            if (local_name_trivia.malformed)
                return true;
            size_t local_name = local_name_trivia.position;
            if (consumeIdentifier(type_spelling, local_name))
                return true;
        }
        position = identifier_end;
    }
    return false;
}

}
