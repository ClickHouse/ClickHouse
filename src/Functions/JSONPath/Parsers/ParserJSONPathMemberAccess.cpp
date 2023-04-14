#include <Functions/JSONPath/ASTs/ASTJSONPathMemberAccess.h>
#include <Functions/JSONPath/Parsers/ParserJSONPathMemberAccess.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/Lexer.h>
#include <Common/StringUtils/StringUtils.h>

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
    // There's a special case, that a path member can begin with number
    if (pos->type != TokenType::Dot && pos->type != TokenType::Number)
        return false;
    if (pos->type != TokenType::Number)
        ++pos;

    ASTPtr member_name;

    if (pos->type == TokenType::Number)[[unlikely]]
    {
        for (const auto * c = pos->begin; c != pos->end; ++c)
        {
            if (*c == '.' && c == pos->begin)
                continue;
            if (!isNumericASCII(*c))
            {
                return false;
            }
        }
        const auto * last_begin = *pos->begin == '.' ? pos->begin + 1 : pos->begin;
        const auto * last_end = pos->end;
        ++pos;

        if (pos.isValid() && pos->type == TokenType::BareWord && pos->begin == last_end)
        {
            member_name = std::make_shared<ASTIdentifier>(String(last_begin, pos->end));
            ++pos;
        }
        else
        {
            return false;
        }
    }
    else
    {
        if (pos->type != TokenType::BareWord && pos->type != TokenType::QuotedIdentifier)
            return false;

        ParserIdentifier name_p;
        if (!name_p.parse(pos, member_name, expected))
            return false;
    }

    auto member_access = std::make_shared<ASTJSONPathMemberAccess>();
    node = member_access;
    return tryGetIdentifierNameInto(member_name, member_access->member_name);
}

}
