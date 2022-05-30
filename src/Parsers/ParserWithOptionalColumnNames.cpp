#include <Parsers/IAST_fwd.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserWithOptionalColumnNames.h>


namespace DB
{
bool ParserWithOptionalColumnNames::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto old_pos = pos;
    ASTPtr column_names;
    ParserToken open_bracket(TokenType::OpeningRoundBracket);
    ParserToken close_bracket(TokenType::ClosingRoundBracket);
    ParserList s_identifiers(std::make_unique<ParserIdentifier>(), std::make_unique<ParserToken>(TokenType::Comma), false);

    if (open_bracket.ignore(pos, expected) && s_identifiers.parse(pos, column_names, expected) 
        && close_bracket.ignore(pos, expected))
    {
        node = column_names;
    }
    else
    {
        pos = old_pos;
    }

    return true;
}


}
