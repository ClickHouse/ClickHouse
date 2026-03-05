#include <Functions/JSONPath/ASTs/ASTJSONPathQuery.h>
#include <Functions/JSONPath/Parsers/ParserJSONPathQuery.h>
#include <Functions/JSONPath/Parsers/ParserJSONPathRoot.h>
#include <Functions/JSONPath/Parsers/ParserJSONPathMemberAccess.h>
#include <Functions/JSONPath/Parsers/ParserJSONPathMemberSquareBracketAccess.h>
#include <Functions/JSONPath/Parsers/ParserJSONPathRange.h>
#include <Functions/JSONPath/Parsers/ParserJSONPathStar.h>

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
    ParserJSONPathMemberSquareBracketAccess parser_jsonpath_member_square_bracket_access;
    ParserJSONPathRange parser_jsonpath_range;
    ParserJSONPathStar parser_jsonpath_star;
    ParserJSONPathRoot parser_jsonpath_root;

    ASTPtr path_root;
    if (!parser_jsonpath_root.parse(pos, path_root, expected))
    {
        return false;
    }
    query->children.push_back(path_root);

    ASTPtr accessor;
    while (parser_jsonpath_member_access.parse(pos, accessor, expected)
           || parser_jsonpath_member_square_bracket_access.parse(pos, accessor, expected)
           || parser_jsonpath_range.parse(pos, accessor, expected)
           || parser_jsonpath_star.parse(pos, accessor, expected))
    {
        if (accessor)
        {
            query->children.push_back(accessor);
            accessor = nullptr;
        }
    }
    /// parsing was successful if we reached the end of query by this point
    return pos->type == TokenType::EndOfStream;
}

}
