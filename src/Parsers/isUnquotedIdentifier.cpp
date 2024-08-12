#include <Parsers/isUnquotedIdentifier.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/Lexer.h>

namespace DB
{

bool isUnquotedIdentifier(const String & name)
{
    auto is_keyword = [&name](Keyword keyword)
    {
        auto s = toStringView(keyword);
        if (name.size() != s.size())
            return false;
        return strncasecmp(s.data(), name.data(), s.size()) == 0;
    };

    /// Special keywords are parsed as literals instead of identifiers.
    if (is_keyword(Keyword::NULL_KEYWORD) || is_keyword(Keyword::TRUE_KEYWORD) || is_keyword(Keyword::FALSE_KEYWORD))
        return false;

    Lexer lexer(name.data(), name.data() + name.size());

    auto maybe_ident = lexer.nextToken();

    if (maybe_ident.type != TokenType::BareWord)
        return false;

    return lexer.nextToken().isEnd();
}

}
