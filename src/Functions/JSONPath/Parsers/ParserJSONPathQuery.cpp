#include <Functions/JSONPath/Parsers/ParserJSONPathQuery.h>

#include <Functions/JSONPath/Parsers/ParserJSONPathMemberAccess.h>

#include <Functions/JSONPath/ASTs/ASTJSONPathQuery.h>

namespace DB

{
/**
 *
 * @param pos token iterator
 * @param query node of ASTJSONPathQuery
 * @param expected stuff for logging
 * @return was parse successful
 */
bool ParserJSONPathQuery::parseImpl(Pos & pos, ASTPtr & query, Expected & expected)
{
    query = std::make_shared<ASTJSONPathQuery>();
    ParserJSONPathMemberAccess parser_jsonpath_member_access;

    if (pos->type != TokenType::DollarSign) {
        return false;
    }
    ++pos;

    bool res = false;
    ASTPtr member_access;
    while (parser_jsonpath_member_access.parse(pos, member_access, expected))
    {
        query->children.push_back(member_access);
        member_access = nullptr;
        res = true;
    }
    /// true in case of at least one success
    return res;
}

}
