#include <algorithm>
#include <array>
#include <cctype>
#include <Parsers/CommonParsers.h>
#include <base/find_symbols.h>
namespace DB
{

namespace
{

/// These invariants used to be checked while the keyword table was built at program startup.
/// They are compile-time now, so they cost nothing and cannot be violated by a build that skips them.

constexpr bool containsUnderscore(std::string_view value)
{
    for (char c : value)
        if (c == '_')
            return true;
    return false;
}

constexpr char toLowerASCII(char c)
{
    return c >= 'A' && c <= 'Z' ? static_cast<char>(c - 'A' + 'a') : c;
}

/// A keyword identifier must spell out its value, with underscores standing for spaces.
constexpr bool identifierMatchesValue(std::string_view identifier, std::string_view value)
{
    if (value == "TRUE" || value == "FALSE" || value == "NULL")
        return true;

    if (identifier.size() != value.size())
        return false;

    for (size_t i = 0; i < identifier.size(); ++i)
    {
        if (identifier[i] == '_' && value[i] == ' ')
            continue;
        if (toLowerASCII(identifier[i]) != toLowerASCII(value[i]))
            return false;
    }

    return true;
}

#define CHECK_KEYWORD_HAS_NO_UNDERSCORE(identifier, value) \
    static_assert(!containsUnderscore(value), \
        "The keyword " value " has an underscore. If this is intentional, declare it in APPLY_FOR_PARSER_KEYWORDS_WITH_UNDERSCORES.");
APPLY_FOR_PARSER_KEYWORDS(CHECK_KEYWORD_HAS_NO_UNDERSCORE)
#undef CHECK_KEYWORD_HAS_NO_UNDERSCORE

#define CHECK_KEYWORD_MATCHES_IDENTIFIER(identifier, value) \
    static_assert(identifierMatchesValue(#identifier, value), \
        "The keyword identifier " #identifier " differs from its value " value ".");
APPLY_FOR_PARSER_KEYWORDS(CHECK_KEYWORD_MATCHES_IDENTIFIER)
APPLY_FOR_PARSER_KEYWORDS_WITH_UNDERSCORES(CHECK_KEYWORD_MATCHES_IDENTIFIER)
#undef CHECK_KEYWORD_MATCHES_IDENTIFIER

/// Indexed by `Keyword`: the enumerators are declared from the same two lists, in the same order.
#define KEYWORD_TO_STRING_VIEW(identifier, value) std::string_view{value},
constexpr std::array keyword_strings
{
    APPLY_FOR_PARSER_KEYWORDS(KEYWORD_TO_STRING_VIEW)
    APPLY_FOR_PARSER_KEYWORDS_WITH_UNDERSCORES(KEYWORD_TO_STRING_VIEW)
};
#undef KEYWORD_TO_STRING_VIEW

}


std::string_view toStringView(Keyword type)
{
    return keyword_strings[static_cast<size_t>(type)];
}

/// Only used to populate system tables and to obfuscate queries, so it is built on demand
/// rather than at startup.
const std::vector<String> & getAllKeyWords()
{
    static const std::vector<String> res(keyword_strings.begin(), keyword_strings.end());
    return res;
}

const std::unordered_set<std::string> & getKeyWordSet()
{
    static const std::unordered_set<std::string> res(keyword_strings.begin(), keyword_strings.end());
    return res;
}

ParserKeyword::ParserKeyword(Keyword keyword)
    : s(toStringView(keyword))
{}

bool ParserKeyword::parseImpl(Pos & pos, [[maybe_unused]] ASTPtr & node, Expected & expected)
{
    if (pos->type != TokenType::BareWord)
        return false;

    const char * current_word = s.data();

    while (true)
    {
        expected.add(pos, current_word);

        if (pos->type != TokenType::BareWord)
            return false;

        const char * const next_whitespace = find_first_symbols<' ', '\0'>(current_word, s.data() + s.size());
        const size_t word_length = next_whitespace - current_word;

        if (word_length != pos->size())
            return false;

        if (0 != strncasecmp(pos->begin, current_word, word_length))
            return false;

        ++pos;

        if (!*next_whitespace)
            break;

        current_word = next_whitespace + 1;
    }

    return true;
}


}
