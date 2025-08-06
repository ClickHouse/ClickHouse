#include "ParserXPathMemberAccess.h"

#include <memory>

#include <Functions/XPath/ASTs/ASTXPathMemberAccess.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{

bool ParserXPathMemberAccess::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (pos->type != TokenType::Slash)
        return false;
    ++pos;

    bool skip_ancestors = false;
    if (pos->type == TokenType::Slash)
    {
        skip_ancestors = true;
        ++pos;
    }

    ASTPtr member_name;
    if (pos->type != TokenType::BareWord)
        return false;

    ParserIdentifier name_p;
    if (!name_p.parse(pos, member_name, expected))
        return false;

    auto member_access = std::make_shared<ASTXPathMemberAccess>();
    node = member_access;

    member_access->skip_ancestors = skip_ancestors;
    member_access->member_name = getIdentifierName(member_name);

    return true;
}

}
