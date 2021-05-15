#include <Functions/JSONPath/Parsers/ParserJSONPathQuery.h>
#include <Functions/JSONPath/Parsers/ParserJSONPathMemberAccess.h>
#include <Functions/JSONPath/Parsers/ParserJSONPathRange.h>
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
    ParserJSONPathRange parser_jsonpath_range;

    if (pos->type != TokenType::DollarSign) {
        return false;
    }
    ++pos;

    bool res = false;
    ASTPtr subquery;
    while (parser_jsonpath_member_access.parse(pos, subquery, expected) ||
        parser_jsonpath_range.parse(pos, subquery, expected))
    {
        if (subquery)
        {
            query->children.push_back(subquery);
            subquery = nullptr;
        }
        res = true;
    }
    /// if we had at least one success and no fails
    return res && pos->type == TokenType::EndOfStream;
}

}
