#include <Parsers/isUnquotedIdentifier.h>

#include <Parsers/Lexer.h>

namespace DB
{

bool isUnquotedIdentifier(const String & name)
{
    Lexer lexer(name.data(), name.data() + name.size());

    auto maybe_ident = lexer.nextToken();

    if (maybe_ident.type != TokenType::BareWord)
        return false;

    return lexer.nextToken().isEnd();
}

}
