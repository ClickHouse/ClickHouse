#include <base/find_symbols.h>
#include <Parsers/Kusto/IKQLParserBase.h>

namespace DB
{

bool IKQLParserBase::parse(KQLPos & pos, ASTPtr & node, KQLExpected & expected)
{
    expected.add(pos, getName());

    return wrapParseImpl(pos, IncreaseDepthTag{}, [&]
    {
        bool res = parseImpl(pos, node, expected);
        if (!res)
            node = nullptr;
        return res;
    });
}


ParserKQLKeyword::ParserKQLKeyword(Keyword keyword)
    : s(toStringView(keyword))
{}

bool ParserKQLKeyword::parseImpl(KQLPos & pos, [[maybe_unused]] ASTPtr & node, KQLExpected & expected)
{
    if (pos->type != KQLTokenType::BareWord)
        return false;

    const char * current_word = s.begin();

    while (true)
    {
        expected.add(pos, current_word);

        if (pos->type != KQLTokenType::BareWord)
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
