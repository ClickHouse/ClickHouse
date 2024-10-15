#include <algorithm>
#include <cctype>
#include <Parsers/CommonParsers.h>
#include <base/find_symbols.h>
#include <Common/ErrorCodes.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
using Strings = std::vector<String>;

class KeyWordToStringConverter
{
public:
    static const KeyWordToStringConverter & instance()
    {
        static const KeyWordToStringConverter res;
        return res;
    }

    std::string_view convert(Keyword type) const
    {
        return mapping[static_cast<size_t>(type)];
    }

    const std::vector<String> & getMapping() const
    {
        return mapping;
    }

    const std::unordered_set<std::string> & getSet() const { return set; }

private:
    KeyWordToStringConverter()
    {
#define KEYWORD_TYPE_TO_STRING_CONVERTER_ADD_TO_MAPPING(identifier, value) \
        checkUnderscore(value); \
        addToMapping(Keyword::identifier, value);
        APPLY_FOR_PARSER_KEYWORDS(KEYWORD_TYPE_TO_STRING_CONVERTER_ADD_TO_MAPPING)
#undef KEYWORD_TYPE_TO_STRING_CONVERTER_ADD_TO_MAPPING

#define KEYWORD_TYPE_TO_STRING_CONVERTER_ADD_TO_MAPPING(identifier, value) \
        addToMapping(Keyword::identifier, value);
        APPLY_FOR_PARSER_KEYWORDS_WITH_UNDERSCORES(KEYWORD_TYPE_TO_STRING_CONVERTER_ADD_TO_MAPPING)
#undef KEYWORD_TYPE_TO_STRING_CONVERTER_ADD_TO_MAPPING

#ifndef NDEBUG
#define KEYWORD_TYPE_TO_STRING_CONVERTER_ADD_TO_MAPPING(identifier, value) \
        check(#identifier, value);
        APPLY_FOR_PARSER_KEYWORDS(KEYWORD_TYPE_TO_STRING_CONVERTER_ADD_TO_MAPPING)
        APPLY_FOR_PARSER_KEYWORDS_WITH_UNDERSCORES(KEYWORD_TYPE_TO_STRING_CONVERTER_ADD_TO_MAPPING)
#undef KEYWORD_TYPE_TO_STRING_CONVERTER_ADD_TO_MAPPING
#endif
    }

    void addToMapping(Keyword identifier, std::string_view value)
    {
        size_t index = static_cast<size_t>(identifier);
        mapping.resize(std::max(index + 1, mapping.size()));
        mapping[index] = value;
        set.emplace(value);
    }

    void checkUnderscore(std::string_view value)
    {
        if (value.contains('_'))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "The keyword {} has underscore. If this is intentional, please declare it in another list.", value);
    }

    [[ maybe_unused ]] void check(std::string_view identifier, std::string_view value)
    {
        if (value == "TRUE" || value == "FALSE" || value == "NULL")
            return;

        if (identifier.size() != value.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The length of the keyword identifier and the length of its value are different.");

        for (size_t i = 0; i < identifier.size(); ++i)
        {
            if (std::tolower(identifier[i]) == '_' && std::tolower(value[i]) == ' ')
                continue;
            if (std::tolower(identifier[i]) == std::tolower(value[i]))
                continue;
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Keyword identifier {} differs from its value {} in {} position: {} and {}",
                identifier, value, i, identifier[i], value[i]);
        }

    }

    Strings mapping;
    std::unordered_set<std::string> set;
};
}


std::string_view toStringView(Keyword type)
{
    return KeyWordToStringConverter::instance().convert(type);
}

const std::vector<String> & getAllKeyWords()
{
    return KeyWordToStringConverter::instance().getMapping();
}

const std::unordered_set<std::string> & getKeyWordSet()
{
    return KeyWordToStringConverter::instance().getSet();
}

ParserKeyword::ParserKeyword(Keyword keyword)
    : s(toStringView(keyword))
{}

bool ParserKeyword::parseImpl(Pos & pos, [[maybe_unused]] ASTPtr & node, Expected & expected)
{
    if (pos->type != TokenType::BareWord)
        return false;

    const char * current_word = s.begin();

    while (true)
    {
        expected.add(pos, current_word);

        if (pos->type != TokenType::BareWord)
            return false;

        const char * const next_whitespace = find_first_symbols<' ', '\0'>(current_word, s.end());
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
