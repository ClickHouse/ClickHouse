#include <Functions/JSONPath/Parsers/ParserJSONPathMemberSquareBracketAccess.h>
#include <memory>
#include <Functions/JSONPath/ASTs/ASTJSONPathMemberAccess.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{
bool ParserJSONPathMemberSquareBracketAccess::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (pos->type != TokenType::OpeningSquareBracket)
        return false;
    ++pos;
    ASTPtr member_name;
    if (pos->type == TokenType::BareWord || pos->type == TokenType::QuotedIdentifier)
    {
        ParserIdentifier name_p;
        if (!name_p.parse(pos, member_name, expected))
            return false;
    }
    else if (pos->type == TokenType::StringLiteral)
    {
        ReadBufferFromMemory in(pos->begin, pos->size());
        String name;
        readQuotedStringWithSQLStyle(name, in);
        member_name = std::make_shared<ASTIdentifier>(name);
        ++pos;
    }
    else
    {
        return false;
    }
    if (pos->type != TokenType::ClosingSquareBracket)
    {
        return false;
    }
    ++pos;
    auto member_access = std::make_shared<ASTJSONPathMemberAccess>();
    node = member_access;
    return tryGetIdentifierNameInto(member_name, member_access->member_name);
}
}
