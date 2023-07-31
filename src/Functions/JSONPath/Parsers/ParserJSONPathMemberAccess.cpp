#include <Functions/JSONPath/ASTs/ASTJSONPathMemberAccess.h>
#include <Functions/JSONPath/Parsers/ParserJSONPathMemberAccess.h>

#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/Lexer.h>

namespace DB
{
/**
 *
 * @param pos token iterator
 * @param node node of ASTJSONPathMemberAccess
 * @param expected stuff for logging
 * @return was parse successful
 */
bool ParserJSONPathMemberAccess::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (pos->type != TokenType::Dot)
        return false;

    ++pos;

    if (pos->type != TokenType::BareWord && pos->type !=TokenType::QuotedIdentifier)
        return false;

    ParserIdentifier name_p;
    ASTPtr member_name;
    if (!name_p.parse(pos, member_name, expected))
        return false;

    auto member_access = std::make_shared<ASTJSONPathMemberAccess>();
    node = member_access;
    return tryGetIdentifierNameInto(member_name, member_access->member_name);
}

}
