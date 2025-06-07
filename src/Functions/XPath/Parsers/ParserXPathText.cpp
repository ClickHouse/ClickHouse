#include "ParserXPathText.h"

#include <memory>

#include <Functions/XPath/ASTs/ASTXPathText.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/CommonParsers.h>

namespace DB
{

bool ParserXPathText::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (pos->type != TokenType::Slash)
        return false;
    ++pos;

    if (!ParserKeyword{Keyword::TEXT}.ignore(pos, expected))
        return false;

    if (pos->type != TokenType::OpeningRoundBracket)
    {
        return false;
    }
    ++pos;

    if (pos->type != TokenType::ClosingRoundBracket)
    {
        return false;
    }
    ++pos;

    node = std::make_shared<ASTXPathText>();

    return true;
}

}
